import os
import time
import requests
from agente.utils import log

SPLITTER_BASE_URL = (os.getenv("SPLITTER_BASE_URL") or "https://nn-rede-splitter-v3-gzbmdjduhjgketh3.brazilsouth-01.azurewebsites.net").rstrip("/")
UPLOAD_URL = os.getenv("SPLITTER_API_UPLOAD") or f"{SPLITTER_BASE_URL}/api/v1/upload"
SPLITTER_API_KEY = (os.getenv("SPLITTER_API_KEY") or "").strip()
REQUEST_TIMEOUT = int(os.getenv("SPLITTER_UPLOAD_TIMEOUT", "300"))


def _headers():
    if not SPLITTER_API_KEY:
        raise RuntimeError("SPLITTER_API_KEY não configurada no agente.")
    return {"Authorization": f"Bearer {SPLITTER_API_KEY}"}


def upload_file(file_path: str, cliente: str = "default") -> dict:
    """Envia um arquivo à API v1 e retorna o JSON do batch criado.

    O arquivo local NÃO é movido aqui. Ele só deve ser arquivado depois que
    todos os filhos forem baixados e gravados com sucesso na pasta do SMEDI.
    """
    filename = os.path.basename(file_path)
    if not os.path.isfile(file_path):
        raise FileNotFoundError(file_path)

    log(f"📤 [{cliente}] Enviando {filename} para {UPLOAD_URL}...")

    last_error = None
    for tentativa in range(1, 4):
        try:
            with open(file_path, "rb") as f:
                response = requests.post(
                    UPLOAD_URL,
                    headers=_headers(),
                    files={"file": (filename, f)},
                    data={"cliente": cliente},
                    timeout=REQUEST_TIMEOUT,
                )

            try:
                payload = response.json()
            except Exception:
                payload = {"erro": response.text[:1000]}

            if response.status_code == 200 and payload.get("ok"):
                log(
                    f"✅ [{cliente}] Upload concluído: {filename} | "
                    f"batch={payload.get('batch_id')} | NSA={payload.get('nsa')} | "
                    f"filhos={payload.get('quantidade_gerados')}"
                )
                return payload

            last_error = f"HTTP {response.status_code}: {payload}"
            log(f"⚠️ [{cliente}] [{tentativa}/3] {last_error}")
        except Exception as e:
            last_error = str(e)
            log(f"⏱ [{cliente}] [{tentativa}/3] Erro no upload de {filename}: {e}")

        time.sleep(5)

    raise RuntimeError(f"Falha final no upload de {filename}: {last_error}")
