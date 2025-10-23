import os
import re
from datetime import datetime

LOG_DIR = os.getenv("AGENTE_LOG_DIR", "./logs")
LOG_FILE = os.path.join(LOG_DIR, "agent.log")

def tempo():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")

def log(msg):
    print(msg)
    os.makedirs(LOG_DIR, exist_ok=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{tempo()}] {msg}\n")

def extrair_nsa(nome_arquivo):
    m = re.search(r"(\d{3})\D*\.[0-9]+$", nome_arquivo)
    return m.group(1) if m else "000"

def ensure_dirs():
    for d in ["input", "enviados", "recebidos", "logs"]:
        os.makedirs(os.path.join(os.getenv("BASE_DIR", ""), d), exist_ok=True)

def ler_ultimos_logs(linhas=40):
    if not os.path.exists(LOG_FILE):
        return []
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        data = f.readlines()[-linhas:]
    return [l.strip() for l in data]
