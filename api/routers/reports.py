"""
api/routers/reports.py - Study Dossier & Report Exporter Endpoints
"""
import os
import json
from datetime import datetime
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from fastapi import APIRouter

router = APIRouter(prefix="/api/reports", tags=["Exportador de Reportes"])

class ReportItem(BaseModel):
    key: str
    title: str
    context: Optional[str] = ""
    category: Optional[str] = "General"
    data: Any

class ReportRequest(BaseModel):
    title: str = "Dossier de Estudio Cienciométrico - Revistas LATAM"
    items: List[ReportItem]
    format: str = "markdown"

@router.post("/generate")
def generate_report(req: ReportRequest):
    """Generates structured Markdown report or JSON bundle from dossier items."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if req.format == "json":
        return {
            "title": req.title,
            "generated_at": timestamp,
            "items": [item.dict() for item in req.items]
        }
        
    # Generate Markdown
    md_lines = [
        f"# {req.title}",
        f"*Generado el {timestamp} por la Plataforma Revistas LATAM (OpenAlex + DuckDB OLAP)*",
        "\n---\n"
    ]
    
    # Group by category
    categories = {}
    for item in req.items:
        categories.setdefault(item.category, []).append(item)
        
    for cat_name, cat_items in categories.items():
        md_lines.append(f"## 📁 {cat_name}\n")
        for item in cat_items:
            md_lines.append(f"### {item.title}")
            if item.context:
                md_lines.append(f"> {item.context}\n")
                
            if isinstance(item.data, dict):
                md_lines.append("| Indicador | Valor |")
                md_lines.append("| :--- | :--- |")
                for k, v in item.data.items():
                    md_lines.append(f"| **{k}** | {v} |")
                md_lines.append("")
            elif isinstance(item.data, list) and len(item.data) > 0 and isinstance(item.data[0], dict):
                headers = list(item.data[0].keys())
                md_lines.append("| " + " | ".join(headers) + " |")
                md_lines.append("| " + " | ".join([":---"] * len(headers)) + " |")
                for row in item.data[:25]:
                    md_lines.append("| " + " | ".join([str(row.get(h, '')) for h in headers]) + " |")
                md_lines.append("")
            else:
                md_lines.append(f"```json\n{json.dumps(item.data, indent=2, ensure_ascii=False)}\n```\n")
                
    markdown_content = "\n".join(md_lines)
    return {
        "title": req.title,
        "format": "markdown",
        "content": markdown_content
    }
