import os
import zipfile
import requests
from datetime import datetime
from utils import log

DOWNLOAD_URL = os.getenv("SPLITTER_API_DOWNLOAD")
LOCAL_RECEIVED = os.path.join(os.getenv("BASE_DIR", ""), "recebidos")

def baixar_output(nsa_hint="000"):
    os.makedirs(LOCAL_RECEIVED, exist_ok=True)
    log("⬇️ Iniciando download do ZIP consolidado...")

    try:
        res = requests.get(DOWNLOAD_URL, timeout=180)
        if res.status_code == 200 and "application/zip" in res.headers.get("Content-Type", ""):
            now = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_name = f"output_NSA_{nsa_hint}_{now}.zip"
            zip_path = os.path.join(LOCAL_RECEIVED, zip_name)

            with open(zip_path, "wb") as f:
                f.write(res.content)

            log(f"📦 Download concluído → {zip_path}")
            extract_dir = os.path.join(LOCAL_RECEIVED, f"NSA_{nsa_hint}_{now}")
            os.makedirs(extract_dir, exist_ok=True)

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)

            log(f"📂 Arquivos extraídos em: {extract_dir}")
        else:
            log(f"⚠️ Falha no download ({res.status_code}) → {res.text[:120]}")
    except Exception as e:
        log(f"❌ Erro durante o download: {e}")
