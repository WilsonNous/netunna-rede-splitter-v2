import os
import csv
from datetime import datetime
from modules.eevd_processor import process_eevd
from modules.eevc_processor import process_eevc
from modules.eefi_processor import process_eefi
from utils.log_utils import log_result

LOG_PATH = os.path.join("logs", "operacoes.csv")

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def process_file(input_path, output_dir, error_dir):
    """Detecta o tipo do arquivo e chama o módulo específico."""
    ensure_dir(output_dir)
    ensure_dir(error_dir)

    filename = os.path.basename(input_path).upper()
    print(f"📥 Iniciando processamento de: {filename}")

    tipo = None
    resultado = None

    try:
        if "EEVD" in filename or "_VD_" in filename:
            tipo = "EEVD"
            resultado = process_eevd(input_path, output_dir)

        elif "EEVC" in filename or "_VC_" in filename:
            tipo = "EEVC"
            resultado = process_eevc(input_path, output_dir)

        elif "EEFI" in filename or "_FI_" in filename:
            tipo = "EEFI"
            resultado = process_eefi(input_path, output_dir)

        else:
            raise ValueError("Tipo de arquivo não reconhecido.")

        total_trailer = resultado.get("total_trailer", 0)
        total_processado = resultado.get("total_processado", 0)
        status = "OK" if abs(total_trailer - total_processado) < 1 else "ERRO"
        detalhe = resultado.get("detalhe", "")

        log_result(filename, tipo, total_trailer, total_processado, status, detalhe)
        print(f"✅ {filename}: {status} — {detalhe}")
        return {"status": status, "detalhe": detalhe}

    except Exception as e:
        log_result(filename, tipo or "DESCONHECIDO", 0, 0, "ERRO", str(e))
        print(f"❌ Falha ao processar {filename}: {e}")
        return {"erro": str(e)}
