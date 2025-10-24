"""
Monitoramento contínuo do diretório input.
Quando novos arquivos são detectados → upload automático.
"""
import time
import os
from agente.uploader import upload_file
from agente.utils import log, extrair_nsa

def run_watcher():
    input_dir = os.getenv("AGENTE_INPUT_DIR")
    log(f"👀 Iniciando monitoramento de {input_dir}...")
    arquivos_enviados = set()

    while True:
        arquivos = [f for f in os.listdir(input_dir) if os.path.isfile(os.path.join(input_dir, f))]
        novos = [f for f in arquivos if f not in arquivos_enviados]
        for nome in novos:
            caminho = os.path.join(input_dir, nome)
            nsa = extrair_nsa(nome)
            log(f"🆕 Novo arquivo detectado: {nome} (NSA {nsa})")
            upload_file(caminho)
            arquivos_enviados.add(nome)
        time.sleep(int(os.getenv("AGENTE_POLL_INTERVAL", 10)))

if __name__ == "__main__":
    run_watcher()
