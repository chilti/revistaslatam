"""
api/routers/auth.py - ORCID OAuth 2.0 Authentication & User Management Router for Revistas LATAM
"""
import os
import urllib.parse
import json
import requests
from typing import Optional
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException, Query, Header
from dotenv import load_dotenv

from api.db_users import upsert_user, get_all_users, get_admin_orcids

load_dotenv()

router = APIRouter(prefix="/api/auth", tags=["Autenticación ORCID"])

ORCID_CLIENT_ID = os.getenv("ORCID_CLIENT_ID", "")
ORCID_CLIENT_SECRET = os.getenv("ORCID_CLIENT_SECRET", "")
ORCID_REDIRECT_URI = os.getenv("ORCID_REDIRECT_URI", "https://dinamica1.fciencias.unam.mx/infotlachia/")

ORCID_AUTH_URL = "https://orcid.org/oauth/authorize"
ORCID_TOKEN_URL = "https://orcid.org/oauth/token"
ORCID_API_BASE = "https://pub.orcid.org/v3.0"


class TokenExchangeRequest(BaseModel):
    code: str
    redirect_uri: Optional[str] = None


def fetch_orcid_affiliation_metadata(orcid_id: str, access_token: Optional[str] = None) -> dict:
    """Consulta la API pública de ORCID para extraer la afiliación institucional y país más reciente."""
    headers = {"Accept": "application/json"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"

    institution = ""
    country = ""

    try:
        # 1. Consultar resumen de empleos / afiliaciones
        emp_url = f"{ORCID_API_BASE}/{orcid_id}/employments"
        resp = requests.get(emp_url, headers=headers, timeout=5.0)
        if resp.status_code == 200:
            data = resp.json()
            groups = data.get("affiliation-group") or []
            if groups:
                summaries = groups[0].get("summaries") or []
                if summaries:
                    emp_summary = summaries[0].get("employment-summary") or {}
                    org = emp_summary.get("organization") or {}
                    institution = org.get("name") or ""
                    address = org.get("address") or {}
                    country = address.get("country") or ""

        # 2. Si no hay país en afiliación, consultar sección person
        if not country:
            person_url = f"{ORCID_API_BASE}/{orcid_id}/person"
            p_resp = requests.get(person_url, headers=headers, timeout=5.0)
            if p_resp.status_code == 200:
                p_data = p_resp.json()
                addresses = (p_data.get("addresses") or {}).get("address") or []
                if addresses:
                    country = (addresses[0].get("country") or {}).get("value") or ""
    except Exception as e:
        print(f"[WARN] Error consultando metadatos de ORCID para {orcid_id}: {e}")

    return {
        "institution": institution,
        "country": country
    }


@router.get("/orcid/url")
def get_orcid_auth_url(redirect_uri: Optional[str] = None, state: Optional[str] = None):
    """Genera la URL oficial de redirección OAuth 2.0 de ORCID con state para puente entre módulos."""
    if not ORCID_CLIENT_ID:
        raise HTTPException(status_code=500, detail="ORCID_CLIENT_ID no configurado en el servidor.")

    final_redirect = redirect_uri if isinstance(redirect_uri, str) and redirect_uri.strip() else ORCID_REDIRECT_URI
    state_val = state if isinstance(state, str) and state.strip() else "revistaslatam"
    params = {
        "client_id": ORCID_CLIENT_ID,
        "response_type": "code",
        "scope": "/authenticate",
        "redirect_uri": final_redirect,
        "state": state_val
    }
    auth_url = f"{ORCID_AUTH_URL}?{urllib.parse.urlencode(params)}"
    return {
        "auth_url": auth_url,
        "client_id": ORCID_CLIENT_ID,
        "redirect_uri": final_redirect,
        "state": state_val
    }


@router.post("/orcid/token")
def exchange_orcid_token(req: TokenExchangeRequest):
    """Intercambia el código de autorización temporal, extrae metadatos institucionales y registra al usuario."""
    if not req.code:
        raise HTTPException(status_code=400, detail="Código de autorización faltante.")

    final_redirect = req.redirect_uri or ORCID_REDIRECT_URI
    payload = {
        "client_id": ORCID_CLIENT_ID,
        "client_secret": ORCID_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": req.code,
        "redirect_uri": final_redirect
    }
    headers = {"Accept": "application/json"}

    try:
        resp = requests.post(ORCID_TOKEN_URL, data=payload, headers=headers, timeout=10.0)
        if resp.status_code != 200:
            error_detail = resp.text
            try:
                error_detail = resp.json().get("error_description") or resp.json().get("error") or resp.text
            except Exception:
                pass
            raise HTTPException(status_code=resp.status_code, detail=f"Error al autenticar con ORCID: {error_detail}")

        token_data = resp.json()
        orcid_val = token_data.get("orcid", "")
        name_val = token_data.get("name") or f"Investigador ({orcid_val})"
        token_val = token_data.get("access_token")

        # 1. Extraer país e institución automáticamente vía API de ORCID
        profile_meta = fetch_orcid_affiliation_metadata(orcid_val, token_val)
        inst_val = profile_meta.get("institution", "")
        country_val = profile_meta.get("country", "")

        # 2. Chequeo de privilegios de administrador
        admin_set = get_admin_orcids()
        is_admin_val = orcid_val in admin_set
        role_val = "admin" if is_admin_val else "user"

        # 3. Persistir en la base de datos de usuarios
        user_record = upsert_user(
            orcid=orcid_val,
            name=name_val,
            institution=inst_val,
            country=country_val,
            role=role_val,
            raw_metadata=json.dumps(token_data)
        )

        return {
            "authenticated": True,
            "orcid": orcid_val,
            "name": name_val,
            "institution": inst_val,
            "country": country_val,
            "role": role_val,
            "is_admin": is_admin_val,
            "login_count": user_record.get("login_count", 1),
            "access_token": token_val,
            "token_type": token_data.get("token_type", "bearer"),
            "scope": token_data.get("scope", "/authenticate")
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en comunicación con ORCID: {str(e)}")


@router.get("/users")
def list_registered_users():
    """Retorna el listado consolidado de usuarios e investigadores registrados para el panel de administración."""
    users = get_all_users()
    admin_set = get_admin_orcids()

    total_admins = sum(1 for u in users if u.get("is_admin"))
    total_logins = sum(u.get("login_count", 1) for u in users)
    distinct_countries = len({u.get("country") for u in users if u.get("country") and u.get("country") != "No especificado"})

    return {
        "total_users": len(users),
        "total_admins": total_admins,
        "total_logins": total_logins,
        "distinct_countries": distinct_countries,
        "admin_orcids_configured": list(admin_set),
        "users": users
    }


@router.get("/me")
def get_current_user_profile(authorization: Optional[str] = Header(None)):
    """Verifica si la petición cuenta con credenciales activas."""
    if not authorization or not authorization.startswith("Bearer "):
        return {"authenticated": False, "user": None}
    
    token = authorization.replace("Bearer ", "").strip()
    if not token:
        return {"authenticated": False, "user": None}
        
    return {
        "authenticated": True,
        "token_preview": f"{token[:6]}...{token[-4:]}" if len(token) > 10 else "***"
    }
