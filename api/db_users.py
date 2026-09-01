"""
api/db_users.py - SQLite Persistence for Registered ORCID Users in Revistas LATAM
"""
import os
import sqlite3
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_DIR = Path(__file__).resolve().parent.parent / "data"
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "users.db"


def get_admin_orcids() -> set:
    """Lee y normaliza la lista de ORCID de administradores desde .env."""
    raw_admins = os.getenv("admins") or os.getenv("ADMINS") or os.getenv("ADMIN_ORCIDS") or ""
    # Limpiar comillas y espacios
    clean_str = raw_admins.replace('"', '').replace("'", "").strip()
    if not clean_str:
        return set()
    return {item.strip() for item in clean_str.split(",") if item.strip()}


def init_users_db():
    """Inicializa la tabla de usuarios registrados si no existe."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS registered_users (
        orcid TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        institution TEXT,
        country TEXT,
        role TEXT DEFAULT 'user',
        first_login DATETIME,
        last_login DATETIME,
        login_count INTEGER DEFAULT 1,
        raw_metadata TEXT
    )
    """)
    conn.commit()
    conn.close()


def upsert_user(orcid: str, name: str, institution: str = "", country: str = "", role: str = "user", raw_metadata: str = "") -> dict:
    """Inserta o actualiza el registro de un investigador al iniciar sesión."""
    init_users_db()
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Verificar si ya existe
    cursor.execute("SELECT * FROM registered_users WHERE orcid = ?", (orcid,))
    existing = cursor.fetchone()

    admin_set = get_admin_orcids()
    final_role = "admin" if orcid in admin_set else role

    if existing:
        new_count = existing["login_count"] + 1
        # Preservar o actualizar datos institucionales si se obtienen nuevos
        final_inst = institution or existing["institution"] or ""
        final_country = country or existing["country"] or ""
        final_name = name or existing["name"] or ""

        cursor.execute("""
            UPDATE registered_users
            SET name = ?, institution = ?, country = ?, role = ?, last_login = ?, login_count = ?
            WHERE orcid = ?
        """, (final_name, final_inst, final_country, final_role, now_iso, new_count, orcid))
        conn.commit()
        user_record = {
            "orcid": orcid,
            "name": final_name,
            "institution": final_inst,
            "country": final_country,
            "role": final_role,
            "first_login": existing["first_login"],
            "last_login": now_iso,
            "login_count": new_count,
            "is_admin": (final_role == "admin")
        }
    else:
        cursor.execute("""
            INSERT INTO registered_users (orcid, name, institution, country, role, first_login, last_login, login_count, raw_metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
        """, (orcid, name, institution, country, final_role, now_iso, now_iso, raw_metadata))
        conn.commit()
        user_record = {
            "orcid": orcid,
            "name": name,
            "institution": institution,
            "country": country,
            "role": final_role,
            "first_login": now_iso,
            "last_login": now_iso,
            "login_count": 1,
            "is_admin": (final_role == "admin")
        }

    conn.close()
    return user_record


def get_all_users() -> list:
    """Obtiene el listado completo de usuarios registrados ordenados por último acceso."""
    init_users_db()
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    admin_set = get_admin_orcids()

    cursor.execute("SELECT * FROM registered_users ORDER BY last_login DESC")
    rows = cursor.fetchall()
    users = []
    for r in rows:
        orcid_val = r["orcid"]
        is_admin_val = (orcid_val in admin_set) or (r["role"] == "admin")
        users.append({
            "orcid": orcid_val,
            "name": r["name"],
            "institution": r["institution"] or "No especificada",
            "country": r["country"] or "No especificado",
            "role": "admin" if is_admin_val else "user",
            "is_admin": is_admin_val,
            "first_login": r["first_login"],
            "last_login": r["last_login"],
            "login_count": r["login_count"]
        })
    conn.close()
    return users


# Inicializar DB al importar
init_users_db()
