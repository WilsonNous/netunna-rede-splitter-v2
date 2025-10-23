"""
Agente Netunna Splitter - v4
Execução manual: Upload → Processamento → Download
"""

import os
import time
from dotenv import load_dotenv
from agente.uploader import upload_file
from agente.downloader import baixar_output
from agente.utils import log, tempo, extrair_nsa, ensure_dirs

load_dotenv()
ensure_dirs()

LOCAL_INPUT = os.getenv("AGENTE_INPUT_DIR")
LOCAL_SENT = os.path.join(os.getenv("BASE_DIR", ""), "enviados")

def main():
    log("🚀 Iniciando Netunna Splitter Agent v4")

    arquivos = [f for f in os.listdir(LOCAL_INPUT)
                if os.path.isfile(os.path.join(LOCAL_INPUT, f))]

    if not arquivos:
        log("📂 Nenhum arquivo encontrado em Input.")
        return

    ultimo_nsa = "000"

    for nome in arquivos:
        caminho = os.path.join(LOCAL_INPUT, nome)
        nsa = extrair_nsa(nome)
        ultimo_nsa = nsa or ultimo_nsa
        upload_file(caminho)

    log("⏳ Aguardando processamento remoto...")
    time.sleep(30)

    baixar_output(nsa_hint=ultimo_nsa)
    log("🏁 Ciclo concluído com sucesso.")

if __name__ == "__main__":
    main()
