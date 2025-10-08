import os
import csv
from datetime import datetime

LOG_PATH = os.path.join("logs", "operacoes.csv")

def log_operation(arquivo, tipo, total_trailer, total_processado, status, detalhe=""):
    """Registra cada operação de processamento no CSV de logs"""
    os.makedirs("logs", exist_ok=True)
    header = ["data_hora", "arquivo", "tipo", "total_trailer", "total_processado", "status", "detalhe"]

    # Cria o arquivo com cabeçalho se não existir
    novo = not os.path.exists(LOG_PATH)
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if novo:
            writer.writeheader()
        writer.writerow({
            "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "arquivo": arquivo,
            "tipo": tipo,
            "total_trailer": total_trailer,
            "total_processado": total_processado,
            "status": status,
            "detalhe": detalhe
        })
