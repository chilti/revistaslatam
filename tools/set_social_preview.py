import os
import shutil
import pathlib
import sys

# Get site-packages via import streamlit
try:
    import streamlit
except ImportError:
    print("❌ Streamlit no está instalado en este entorno.")
    sys.exit(1)

# Configuración de Metadatos
TITLE = "Dashboard Bibliométrico LATAM"
DESCRIPTION = "Análisis interactivo de la producción científica, revistas y trayectorias de desempeño en Latinoamérica."
URL = "http://dinamica1.fciencias.unam.mx/revistaslatam/"

def patch_streamlit_index():
    # 1. Localizar la instalación de Streamlit
    streamlit_path = pathlib.Path(streamlit.__file__).parent
    static_path = streamlit_path / "static" / "index.html"
    
    if not static_path.exists():
        print(f"❌ No se encontró el archivo index.html en: {static_path}")
        return

    print(f"📍 Archivo localizado: {static_path}")

    # 2. Hacer backup
    backup_path = static_path.with_suffix(".html.bak")
    if not backup_path.exists():
        shutil.copy2(static_path, backup_path)
        print("✅ Backup creado.")
    else:
        print("ℹ️ Backup ya existente. Restaurando antes de aplicar cambios...")
        shutil.copy2(backup_path, static_path)

    # 3. Leer contenido
    try:
        with open(static_path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        print(f"❌ Error leyendo archivo: {e}")
        return

    # 4. Definir los nuevos tags
    meta_tags = f"""
    <title>{TITLE}</title>
    <meta name="description" content="{DESCRIPTION}">
    <meta property="og:title" content="{TITLE}">
    <meta property="og:description" content="{DESCRIPTION}">
    <meta property="og:url" content="{URL}">
    <meta property="og:type" content="website">
    <meta property="og:image" content="{URL}/static/og-image.png"> <!-- Opcional -->
    """

    # 5. Reemplazar/Insertar
    updated = False
    
    # Reemplazar título existente
    if "<title>Streamlit</title>" in content:
        content = content.replace("<title>Streamlit</title>", meta_tags)
        updated = True
        print("✅ Título por defecto reemplazado.")
    elif f"<title>{TITLE}</title>" in content:
        print("ℹ️ El título ya parece estar actualizado.")
    else:
        # Si tiene otro título, intentamos inyectar meta tags en <head>
        if "<head>" in content:
            content = content.replace("<head>", f"<head>\n{meta_tags}")
            updated = True
            print("✅ Meta tags inyectados en <head>.")
        else:
            print("❌ No se pudo encontrar <head> ni <title> para reemplazar.")
            return

    # 6. Guardar cambios
    if updated:
        try:
            with open(static_path, "w", encoding="utf-8") as f:
                f.write(content)
            print("\n✨ ¡Éxito! El archivo index.html ha sido actualizado.")
            print("💡 Nota: Debes reiniciar tu aplicación Streamlit para ver los cambios.")
        except Exception as e:
            print(f"❌ Error escribiendo el archivo: {e}")
            print("   Asegúrate de tener permisos de escritura o ejecutar como administrador.")

if __name__ == "__main__":
    patch_streamlit_index()
