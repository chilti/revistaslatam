"""
api/routers/exports.py - Asynchronous Background Export Manager for Revistas LATAM
"""
import os
import sys
import json
import csv
import time
import uuid
import threading
from pathlib import Path
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse

from api.db import DATA_DIR
from pipeline_revistaslatam.export_articles_openalex import (
    get_work_ids_from_db,
    fetch_single_work,
    map_work_to_openalex_csv_row,
    OPENALEX_CSV_COLUMNS,
    DEFAULT_OPENALEX_API,
    normalize_id
)

# Integración con TlachIA Metrics Engine y dependencias
def _get_metrics_engine():
    """Importa e inicializa dinámicamente TlachIAMetricsEngine asegurando los paths de dependencias."""
    REVISTASLATAM_PACKAGES = Path("/home/ambientesPy/revistaslatam/lib/python3.12/site-packages").resolve()
    if str(REVISTASLATAM_PACKAGES) not in sys.path and REVISTASLATAM_PACKAGES.exists():
        sys.path.insert(0, str(REVISTASLATAM_PACKAGES))

    TLACHIA_PATH = Path("/mnt/expansion/desplegados/TlachIA-Metrics").resolve()
    if str(TLACHIA_PATH) not in sys.path and TLACHIA_PATH.exists():
        sys.path.insert(0, str(TLACHIA_PATH))

    try:
        from openalex_indicators_engine.engine import TlachIAMetricsEngine
        return TlachIAMetricsEngine()
    except Exception as e:
        raise RuntimeError(f"No se pudo inicializar TlachIAMetricsEngine: {e}")

router = APIRouter(prefix="/api/exports", tags=["Exportaciones Asíncronas"])

EXPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "exports"
EXPORTS_DIR.mkdir(exist_ok=True)

JOBS_REGISTRY_FILE = EXPORTS_DIR / "jobs_registry.json"
JOBS_LOCK = threading.Lock()


def _load_all_jobs() -> Dict[str, Dict[str, Any]]:
    """Carga todos los jobs desde el registro en disco de forma segura."""
    if not JOBS_REGISTRY_FILE.exists():
        return {}
    try:
        with open(JOBS_REGISTRY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_job_record(job_info: Dict[str, Any]):
    """Guarda o actualiza un job en el registro en disco de forma atómica."""
    with JOBS_LOCK:
        jobs = _load_all_jobs()
        jobs[job_info["id"]] = job_info
        temp_file = JOBS_REGISTRY_FILE.with_suffix(".tmp")
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(jobs, f, ensure_ascii=False, indent=2)
            temp_file.replace(JOBS_REGISTRY_FILE)
        except Exception:
            pass


def _get_job_record(job_id: str) -> Optional[Dict[str, Any]]:
    """Obtiene un job específico por ID."""
    jobs = _load_all_jobs()
    return jobs.get(job_id)


def _delete_job_record(job_id: str) -> bool:
    """Elimina un job del registro en disco."""
    with JOBS_LOCK:
        jobs = _load_all_jobs()
        if job_id in jobs:
            del jobs[job_id]
            temp_file = JOBS_REGISTRY_FILE.with_suffix(".tmp")
            try:
                with open(temp_file, "w", encoding="utf-8") as f:
                    json.dump(jobs, f, ensure_ascii=False, indent=2)
                temp_file.replace(JOBS_REGISTRY_FILE)
                return True
            except Exception:
                pass
    return False


class ExportRequest(BaseModel):
    journal_id: Optional[str] = None
    country_code: Optional[str] = None
    year_min: Optional[int] = None
    year_max: Optional[int] = None
    format: str = "json"  # "json", "csv", "jsonl", "metrics"
    limit: Optional[int] = None
    title: Optional[str] = None


def run_export_job(job_id: str, req_data: Dict[str, Any]):
    """Background worker function executing the full export pipeline."""
    job_info = _get_job_record(job_id)
    if not job_info:
        return

    job_info["status"] = "processing"
    _save_job_record(job_info)

    try:
        journal_id = req_data.get("journal_id")
        country_code = req_data.get("country_code")
        year_min = req_data.get("year_min")
        year_max = req_data.get("year_max")
        fmt = req_data.get("format", "json").lower()
        limit = req_data.get("limit")
        title_hint = req_data.get("title") or "articulos"

        # 1. Obtain Work IDs from database
        work_ids = get_work_ids_from_db(
            journal_id=journal_id,
            country_code=country_code,
            year_min=year_min,
            year_max=year_max,
            limit=limit
        )

        total_works = len(work_ids)
        if total_works == 0:
            job_info["status"] = "failed"
            job_info["error"] = "No se encontraron artículos para exportar con los filtros indicados."
            _save_job_record(job_info)
            return

        job_info["total"] = total_works
        job_info["progress"] = 0
        job_info["pct"] = 0.0
        _save_job_record(job_info)

        safe_title = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in title_hint)[:40]
        year_tag = f"_{year_min}_{year_max}" if (year_min or year_max) else "_todos"

        # ── Caso Especial: Cálculo de Métricas Cienciométricas TlachIA (ZIP) ───
        if fmt in ("metrics", "zip", "tlachia"):
            engine = _get_metrics_engine()
            job_info["pct"] = 5.0
            _save_job_record(job_info)

            # Carga del corpus directamente desde los IDs normalizados
            df_corpus = engine.load_corpus(work_ids)
            
            def progress_cb(pct, msg):
                rec = _get_job_record(job_id)
                if rec:
                    rec["pct"] = round(float(pct), 1)
                    rec["progress"] = int((float(pct) / 100.0) * total_works)
                    _save_job_record(rec)

            pkg_name = f"metricas_{safe_title}{year_tag}_{job_id[:8]}"
            out_pkg_dir = EXPORTS_DIR / pkg_name
            
            res_pkg = engine.process_and_export_package(
                df=df_corpus,
                package_name=pkg_name,
                output_dir=out_pkg_dir,
                export_parquet=True,
                export_json=True,
                progress_callback=progress_cb
            )

            zip_path = Path(res_pkg["zip_path"])
            file_size = zip_path.stat().st_size
            file_size_mb = round(file_size / (1024 * 1024), 2)

            job_info["status"] = "completed"
            job_info["progress"] = total_works
            job_info["pct"] = 100.0
            job_info["filename"] = zip_path.name
            job_info["filepath"] = str(zip_path)
            job_info["filesize_bytes"] = file_size
            job_info["filesize_mb"] = file_size_mb
            job_info["completed_at"] = time.time()
            _save_job_record(job_info)
            return

        # 2. Fetch full OpenAlex records concurrently para JSON / CSV / JSONL
        works = []
        max_workers = 16
        completed_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_wid = {
                executor.submit(fetch_single_work, wid, DEFAULT_OPENALEX_API): wid
                for wid in work_ids
            }
            for future in as_completed(future_to_wid):
                res = future.result()
                if res:
                    works.append(res)
                completed_count += 1
                if completed_count % 10 == 0 or completed_count == total_works:
                    rec = _get_job_record(job_id)
                    if rec:
                        rec["progress"] = completed_count
                        rec["pct"] = round((completed_count / total_works) * 100, 1)
                        _save_job_record(rec)

        if not works:
            job_info["status"] = "failed"
            job_info["error"] = "No se pudieron descargar los registros desde OpenAlex local."
            _save_job_record(job_info)
            return

        # 3. Write file to disk (Compressed JSON/JSONL with gzip / CSV)
        import gzip
        if fmt == "csv":
            filename = f"openalex_{safe_title}{year_tag}_{job_id[:8]}.csv"
            filepath = EXPORTS_DIR / filename
            with open(filepath, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=OPENALEX_CSV_COLUMNS)
                writer.writeheader()
                for w in works:
                    writer.writerow(map_work_to_openalex_csv_row(w))
        elif fmt == "jsonl":
            filename = f"openalex_{safe_title}{year_tag}_{job_id[:8]}.jsonl.gz"
            filepath = EXPORTS_DIR / filename
            with gzip.open(filepath, "wt", encoding="utf-8") as f:
                for w in works:
                    f.write(json.dumps(w, ensure_ascii=False) + "\n")
        else:  # json array compressed with gzip
            filename = f"openalex_{safe_title}{year_tag}_{job_id[:8]}.json.gz"
            filepath = EXPORTS_DIR / filename
            with gzip.open(filepath, "wt", encoding="utf-8") as f:
                json.dump(works, f, ensure_ascii=False, indent=2)

        file_size = filepath.stat().st_size
        file_size_mb = round(file_size / (1024 * 1024), 2)

        job_info["status"] = "completed"
        job_info["progress"] = total_works
        job_info["pct"] = 100.0
        job_info["filename"] = filename
        job_info["filepath"] = str(filepath)
        job_info["filesize_bytes"] = file_size
        job_info["filesize_mb"] = file_size_mb
        job_info["completed_at"] = time.time()
        _save_job_record(job_info)

    except Exception as e:
        rec = _get_job_record(job_id) or job_info
        if rec:
            rec["status"] = "failed"
            rec["error"] = str(e)
            _save_job_record(rec)


@router.post("/start")
def start_export_job(req: ExportRequest, background_tasks: BackgroundTasks):
    """Starts an asynchronous export job and returns its tracking Job ID immediately."""
    job_id = str(uuid.uuid4())
    display_title = req.title or (f"Revista {req.journal_id}" if req.journal_id else (f"País {req.country_code}" if req.country_code else "Artículos"))

    job_info = {
        "id": job_id,
        "title": display_title,
        "format": req.format.lower(),
        "status": "pending",
        "progress": 0,
        "total": 0,
        "pct": 0.0,
        "filename": "",
        "filepath": "",
        "filesize_bytes": 0,
        "filesize_mb": 0.0,
        "created_at": time.time(),
        "completed_at": None,
        "error": None
    }

    _save_job_record(job_info)

    # Launch background thread
    threading.Thread(
        target=run_export_job,
        args=(job_id, req.dict()),
        daemon=True
    ).start()

    return {
        "job_id": job_id,
        "status": "pending",
        "message": f"Exportación iniciada para '{display_title}'.",
        "job": job_info
    }


@router.get("/status/{job_id}")
def get_export_status(job_id: str):
    """Polls the real-time progress and completion state of an export job."""
    job = _get_job_record(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Trabajo de exportación no encontrado")
    return job


@router.get("/list")
def list_exports():
    """Returns the list of recent export jobs sorted by most recent first."""
    all_jobs = _load_all_jobs()
    jobs_list = sorted(list(all_jobs.values()), key=lambda x: x.get("created_at", 0), reverse=True)
    return jobs_list[:50]


@router.get("/download/{job_id}")
def download_export_file(job_id: str):
    """Downloads the generated file when the export job is completed."""
    job = _get_job_record(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Trabajo de exportación no encontrado")

    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"El archivo aún no está listo. Estado actual: {job['status']}")

    filepath = Path(job["filepath"])
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="El archivo generado ya no existe en el servidor.")

    if job["filename"].endswith(".zip") or job["format"] in ("metrics", "zip", "tlachia"):
        media_type = "application/zip"
    elif job["filename"].endswith(".gz"):
        media_type = "application/gzip"
    elif job["format"] == "csv":
        media_type = "text/csv; charset=utf-8"
    else:
        media_type = "application/json; charset=utf-8"

    return FileResponse(
        path=str(filepath),
        filename=job["filename"],
        media_type=media_type
    )


@router.delete("/{job_id}")
def delete_export_job(job_id: str):
    """Removes an export job and deletes its file if exists."""
    job = _get_job_record(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Trabajo no encontrado")

    _delete_job_record(job_id)

    if job.get("filepath"):
        fp = Path(job["filepath"])
        if fp.exists():
            try:
                fp.unlink()
            except Exception:
                pass

    return {"deleted": True, "job_id": job_id}


