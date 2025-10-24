import os
import time
import requests
import shutil
from agente.utils import log

# ==============================================================
# 📦 Configurações de Upload
# ==============================================================

# 🔹 URL principal do backend Splitter
UPLOAD_URL = (
    os.getenv("SPLITTER_API_UPLOAD")
    or "https://nn-rede-splitter-v3-gzbmdjduhjgketh3.brazilsouth-01.azurewebsites.net/api/upload"
)

# 🔹 Diretório local de arquivos enviados
BASE_DIR = os.getenv("BASE_DIR", os.getcwd())
LOCAL_SENT = os.path.join(BASE_DIR, "enviados")


# ==============================================================
# 🚀 Função principal de upload
# ==============================================================
def upload_file(file_path: str) -> bool:
    """
    Envia o arquivo local para o endpoint /api/upload do Splitter.
    Se SPLITTER_API_UPLOAD não estiver definido, usa o fallback padrão
    configurado acima. Faz até 3 tentativas automáticas com delay.
    """
    filename = os.path.basename(file_path)

    if not UPLOAD_URL or not UPLOAD_URL.startswith(("http://", "https://")):
        log(f"❌ URL inválida ou não configurada: {UPLOAD_URL}")
        return False

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
                log(f"⚠️ [{tentativa}/3] Falha ({response.status_code}): {response.text[:150]}")

        except Exception as e:
            log(f"⏱ [{tentativa}/3] Erro ao enviar {filename}: {e}")

        time.sleep(5)

    log(f"❌ Falha final: não foi possível enviar {filename}.")
    return False
