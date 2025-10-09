import os
import csv
from datetime import datetime

LOG_PATH = os.path.join("logs", "operacoes.csv")

def log_result(arquivo, tipo, total_trailer, total_processado, status, detalhe):
    """Registra resultado em CSV de logs."""
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    nova_linha = {
        "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "arquivo": arquivo,
        "tipo": tipo,
        "total_trailer": total_trailer,
        "total_processado": total_processado,
        "status": status,
        "detalhe": detalhe
    }

    novo = not os.path.exists(LOG_PATH)
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=nova_linha.keys())
        if novo:
            writer.writeheader()
        writer.writerow(nova_linha)
