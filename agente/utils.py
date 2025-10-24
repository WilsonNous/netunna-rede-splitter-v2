import os
import re
from datetime import datetime

# =========================================================
# 🔧 Diretórios e configuração
# =========================================================
BASE_DIR = os.getenv("BASE_DIR", "/home/site/wwwroot")
LOG_DIR = os.getenv("AGENTE_LOG_DIR", os.path.join(BASE_DIR, "logs"))
LOG_FILE = os.path.join(LOG_DIR, "agent.log")

# =========================================================
# 🕒 Utilitários de tempo e log
# =========================================================
def tempo():
    """Retorna timestamp atual formatado (dd/mm/aaaa HH:MM:SS)."""
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def log(msg: str):
    """Registra mensagem no log e imprime no console."""
    os.makedirs(LOG_DIR, exist_ok=True)
    linha = f"[{tempo()}] {msg}"
    print(linha)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(linha + "\n")
    except Exception as e:
        print(f"⚠️ Falha ao gravar log: {e}")

# =========================================================
# 🧩 Funções auxiliares
# =========================================================
def extrair_nsa(nome_arquivo: str) -> str:
    """
    Extrai o número NSA do nome do arquivo.
    Aceita formatos como:
      - VENTUNO_05102025_041.TXT
      - VENTUNO_041.TXT
      - NSA_041
    """
    m = re.search(r"(?:NSA_)?(\d{3})(?:\.|_|$)", nome_arquivo)
    return m.group(1) if m else "000"

def ensure_dirs():
    """Garante que as pastas essenciais existam no ambiente do agente."""
    estrutura = [
        os.path.join(BASE_DIR, "input"),
        os.path.join(BASE_DIR, "output"),
        os.path.join(BASE_DIR, "erro"),
        os.path.join(BASE_DIR, "logs"),
        os.path.join(BASE_DIR, "enviados"),
    ]
    for d in estrutura:
        try:
            os.makedirs(d, exist_ok=True)
        except Exception as e:
            print(f"⚠️ Falha ao criar diretório {d}: {e}")
    log("📁 Estrutura de diretórios verificada/ajustada.")

def ler_ultimos_logs(linhas: int = 40):
    """Lê as últimas linhas do log do agente."""
    if not os.path.exists(LOG_FILE):
        return []
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            data = f.readlines()[-linhas:]
        return [l.strip() for l in data]
    except Exception as e:
        print(f"⚠️ Falha ao ler logs: {e}")
        return []
