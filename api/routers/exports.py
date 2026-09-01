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

router = APIRouter(prefix="/api/exports", tags=["Exportaciones Asíncronas"])

EXPORTS_DIR = Path(__file__).resolve().parent.parent.parent / "exports"
EXPORTS_DIR.mkdir(exist_ok=True)

# In-memory registry of export jobs
# Structure:
# {
#   job_id: {
#     "id": str,
#     "title": str,
#     "format": "json" | "csv" | "jsonl",
#     "status": "pending" | "processing" | "completed" | "failed",
#     "progress": int,
#     "total": int,
#     "pct": float,
#     "filename": str,
#     "filepath": str,
#     "filesize_bytes": int,
#     "filesize_mb": float,
#     "created_at": float,
#     "completed_at": Optional[float],
#     "error": Optional[str]
#   }
# }
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()


class ExportRequest(BaseModel):
    journal_id: Optional[str] = None
    country_code: Optional[str] = None
    year_min: Optional[int] = None
    year_max: Optional[int] = None
    format: str = "json"  # "json", "csv", "jsonl"
    limit: Optional[int] = None
    title: Optional[str] = None


def run_export_job(job_id: str, req_data: Dict[str, Any]):
    """Background worker function executing the full export pipeline."""
    with JOBS_LOCK:
        if job_id not in JOBS:
            return
        JOBS[job_id]["status"] = "processing"

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
            with JOBS_LOCK:
                JOBS[job_id]["status"] = "failed"
                JOBS[job_id]["error"] = "No se encontraron artículos para exportar con los filtros indicados."
            return

        with JOBS_LOCK:
            JOBS[job_id]["total"] = total_works
            JOBS[job_id]["progress"] = 0
            JOBS[job_id]["pct"] = 0.0

        # 2. Fetch full OpenAlex records concurrently
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
                    with JOBS_LOCK:
                        JOBS[job_id]["progress"] = completed_count
                        JOBS[job_id]["pct"] = round((completed_count / total_works) * 100, 1)

        if not works:
            with JOBS_LOCK:
                JOBS[job_id]["status"] = "failed"
                JOBS[job_id]["error"] = "No se pudieron descargar los registros desde OpenAlex local."
            return

        # 3. Write file to disk (Compressed JSON/JSONL with gzip)
        safe_title = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in title_hint)[:40]
        year_tag = f"_{year_min}_{year_max}" if (year_min or year_max) else "_todos"

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

        with JOBS_LOCK:
            JOBS[job_id]["status"] = "completed"
            JOBS[job_id]["progress"] = total_works
            JOBS[job_id]["pct"] = 100.0
            JOBS[job_id]["filename"] = filename
            JOBS[job_id]["filepath"] = str(filepath)
            JOBS[job_id]["filesize_bytes"] = file_size
            JOBS[job_id]["filesize_mb"] = file_size_mb
            JOBS[job_id]["completed_at"] = time.time()

    except Exception as e:
        with JOBS_LOCK:
            JOBS[job_id]["status"] = "failed"
            JOBS[job_id]["error"] = str(e)


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

    with JOBS_LOCK:
        JOBS[job_id] = job_info

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
    with JOBS_LOCK:
        if job_id not in JOBS:
            raise HTTPException(status_code=404, detail="Trabajo de exportación no encontrado")
        return JOBS[job_id]


@router.get("/list")
def list_exports():
    """Returns the list of recent export jobs sorted by most recent first."""
    with JOBS_LOCK:
        jobs_list = sorted(list(JOBS.values()), key=lambda x: x.get("created_at", 0), reverse=True)
        return jobs_list[:50]


@router.get("/download/{job_id}")
def download_export_file(job_id: str):
    """Downloads the generated file when the export job is completed."""
    with JOBS_LOCK:
        if job_id not in JOBS:
            raise HTTPException(status_code=404, detail="Trabajo de exportación no encontrado")
        job = JOBS[job_id]

    if job["status"] != "completed":
        raise HTTPException(status_code=400, detail=f"El archivo aún no está listo. Estado actual: {job['status']}")

    filepath = Path(job["filepath"])
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="El archivo generado ya no existe en el servidor.")

    if job["filename"].endswith(".gz"):
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
    with JOBS_LOCK:
        if job_id not in JOBS:
            raise HTTPException(status_code=404, detail="Trabajo no encontrado")
        job = JOBS.pop(job_id)

    if job.get("filepath"):
        fp = Path(job["filepath"])
        if fp.exists():
            try:
                fp.unlink()
            except Exception:
                pass

    return {"deleted": True, "job_id": job_id}
