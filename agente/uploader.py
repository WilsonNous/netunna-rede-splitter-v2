import os
import time
import requests
import shutil
from utils import log

UPLOAD_URL = os.getenv("SPLITTER_API_UPLOAD")

LOCAL_SENT = os.path.join(os.getenv("BASE_DIR", ""), "enviados")

def upload_file(file_path):
    filename = os.path.basename(file_path)
    log(f"📤 Enviando {filename} para {UPLOAD_URL}...")

    for tentativa in range(1, 4):
        try:
            with open(file_path, "rb") as f:
                files = {"file": (filename, f)}
                response = requests.post(UPLOAD_URL, files=files, timeout=90)

            if response.status_code == 200:
                log(f"✅ [{tentativa}/3] {filename} enviado com sucesso.")
                os.makedirs(LOCAL_SENT, exist_ok=True)
                shutil.move(file_path, os.path.join(LOCAL_SENT, filename))
                return True
            else:
                log(f"⚠️ [{tentativa}/3] Falha ({response.status_code}): {response.text[:100]}")
        except Exception as e:
            log(f"⏱ [{tentativa}/3] Erro ao enviar {filename}: {e}")
        time.sleep(5)

    log(f"❌ Falha final: não foi possível enviar {filename}.")
    return False
