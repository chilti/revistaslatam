"""
api/main.py - Revistas LATAM FastAPI Server Entrypoint
"""
import os
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from api.routers import regional, countries, journals, maps, networks, reports
from api.db import get_con

app = FastAPI(
    title="Revistas LATAM - Scientific Intelligence API",
    description="High-performance analytical engine for 7,494 Latin American journals and 3.63M works (DuckDB + UMAP + WebGL).",
    version="2.0.0"
)

# CORS middleware for React / Vite
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Gzip compression for large payload transfers (UMAP points, Sunburst trees)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Include Routers
app.include_router(regional.router)
app.include_router(countries.router)
app.include_router(journals.router)
app.include_router(maps.router)
app.include_router(networks.router)
app.include_router(reports.router)

@app.on_event("startup")
def startup_event():
    """Initializes DuckDB connection pool on server startup."""
    con = get_con()
    print("[Revistas LATAM API] DuckDB engine loaded and ready.")

@app.get("/api/health")
def health_check():
    return {"status": "healthy"}

@app.get("/api/info")
def api_info():
    return {
        "status": "online",
        "app": "Revistas LATAM - Scientific Intelligence API",
        "version": "2.0.0",
        "docs": "/docs"
    }

# Mount React production build if available
FRONTEND_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
if FRONTEND_DIST.exists() and (FRONTEND_DIST / "index.html").exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST / "assets")), name="static")

    @app.get("/{full_path:path}")
    def serve_frontend(full_path: str):
        file_path = FRONTEND_DIST / full_path
        if full_path and file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(FRONTEND_DIST / "index.html")
else:
    @app.get("/")
    def root():
        return {
            "status": "online",
            "app": "Revistas LATAM - Scientific Intelligence API",
            "version": "2.0.0",
            "docs": "/docs"
        }
