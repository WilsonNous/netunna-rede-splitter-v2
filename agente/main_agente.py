"""
=========================================================
Agente Netunna Splitter - v4.1
---------------------------------------------------------
Fluxo Automático:
  1️⃣ Upload → 2️⃣ Processamento remoto → 3️⃣ Download
---------------------------------------------------------
Compatível com painel principal do Splitter (Azure)
Autor: Nous / Netunna Software © 2025
=========================================================
"""

import os
import time
from dotenv import load_dotenv
from agente.uploader import upload_file
from agente.downloader import baixar_output
from agente.utils import log, tempo, extrair_nsa

# =========================================================
# 🔧 Inicialização e Configuração
# =========================================================
load_dotenv()

# Diretório base (padrão para Azure)
BASE_DIR = os.getenv("BASE_DIR", "/home/site/wwwroot")

# Diretórios operacionais — sincronizados com o painel principal
LOCAL_INPUT  = os.getenv("AGENTE_INPUT_DIR", os.path.join(BASE_DIR, "input"))
LOCAL_OUTPUT = os.getenv("AGENTE_OUTPUT_DIR", os.path.join(BASE_DIR, "output"))
LOCAL_ERROR  = os.getenv("AGENTE_ERROR_DIR", os.path.join(BASE_DIR, "erro"))
LOCAL_LOGS   = os.getenv("AGENTE_LOG_DIR", os.path.join(BASE_DIR, "logs"))
LOCAL_SENT   = os.path.join(BASE_DIR, "enviados")

# =========================================================
# 🧩 Criação automática de diretórios
# =========================================================
def ensure_dirs():
    """Garante a existência das pastas essenciais do agente."""
    dirs = [LOCAL_INPUT, LOCAL_OUTPUT, LOCAL_ERROR, LOCAL_LOGS, LOCAL_SENT]
    for d in dirs:
        try:
            os.makedirs(d, exist_ok=True)
        except Exception as e:
            print(f"⚠️ Falha ao criar diretório {d}: {e}")
    log("📁 Estrutura de diretórios verificada/ajustada.")

# =========================================================
# 🚀 Execução Principal
# =========================================================
def main():
    ensure_dirs()
    log("🚀 Iniciando Netunna Splitter Agent v4.1")

    # Lista arquivos disponíveis no diretório de input
    try:
        arquivos = [f for f in os.listdir(LOCAL_INPUT)
                    if os.path.isfile(os.path.join(LOCAL_INPUT, f))]
    except Exception as e:
        log(f"❌ Erro ao listar arquivos em {LOCAL_INPUT}: {e}")
        return

    if not arquivos:
        log("📂 Nenhum arquivo encontrado no diretório INPUT.")
        return

    ultimo_nsa = "000"

    # =========================================================
    # 1️⃣ Upload de arquivos
    # =========================================================
    for nome in arquivos:
        caminho = os.path.join(LOCAL_INPUT, nome)
        nsa = extrair_nsa(nome)
        ultimo_nsa = nsa or ultimo_nsa
        try:
            log(f"📤 Enviando arquivo {nome}...")
            upload_file(caminho)
        except Exception as e:
            log(f"❌ Falha ao enviar {nome}: {e}")

    # =========================================================
    # 2️⃣ Aguardar processamento remoto
    # =========================================================
    log("⏳ Aguardando processamento remoto (30s)...")
    time.sleep(30)

    # =========================================================
    # 3️⃣ Baixar arquivos processados
    # =========================================================
    try:
        log("⬇️ Iniciando download do output remoto...")
        baixar_output(nsa_hint=ultimo_nsa)
        log("✅ Download concluído com sucesso.")
    except Exception as e:
        log(f"⚠️ Erro ao baixar arquivos: {e}")

    log("🏁 Ciclo de execução finalizado com sucesso.")
    log(f"🕒 Duração total: {tempo()}")

# =========================================================
# 🔘 Execução Direta
# =========================================================
if __name__ == "__main__":
    main()
