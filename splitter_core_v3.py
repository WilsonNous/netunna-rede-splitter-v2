import os
import csv
from datetime import datetime
from modules.eevd_processor import process_eevd
from modules.eevc_processor import process_eevc
from modules.eefi_processor import process_eefi
from utils.log_utils import log_result

LOG_PATH = os.path.join("logs", "operacoes.csv")

# ======================================================
#  ⚙️  FUNÇÕES AUXILIARES
# ======================================================
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def limpar_output(output_dir):
    """Remove todos os arquivos do diretório de saída antes de novo processamento."""
    ensure_dir(output_dir)
    for fname in os.listdir(output_dir):
        fpath = os.path.join(output_dir, fname)
        if os.path.isfile(fpath):
            os.remove(fpath)
    print(f"🧹 Limpeza realizada em {output_dir}")


# ======================================================
#  🚀  PROCESSAMENTO PRINCIPAL
# ======================================================
def process_file(input_path, output_dir, error_dir):
    """
    Detecta o tipo de arquivo e chama o módulo correspondente (EEVD / EEVC / EEFI).
    Retorna um dicionário com status, detalhe, tipo e — quando aplicável — arquivos gerados.
    """
    ensure_dir(output_dir)
    ensure_dir(error_dir)

    filename = os.path.basename(input_path).upper()
    print(f"\n📥 Iniciando processamento de: {filename}")

    tipo = None
    resultado = None

    try:
        # ==================================================
        # 🔎 DETECÇÃO DO TIPO DE ARQUIVO
        # ==================================================
        if "EEVD" in filename or "_VD_" in filename:
            tipo = "EEVD"
            limpar_output(output_dir)
            resultado = process_eevd(input_path, output_dir)

        elif "EEVC" in filename or "_VC_" in filename:
            tipo = "EEVC"
            limpar_output(output_dir)
            resultado = process_eevc(input_path, output_dir)

        elif "EEFI" in filename or "_FI_" in filename:
            tipo = "EEFI"
            limpar_output(output_dir)
            resultado = process_eefi(input_path, output_dir)

        else:
            raise ValueError("Tipo de arquivo não reconhecido (esperado EEVD, EEVC ou EEFI).")

        # ==================================================
        # 📊 VALIDAÇÃO E REGISTRO
        # ==================================================
        total_trailer = int(resultado.get("total_trailer", 0))
        total_processado = int(resultado.get("total_processado", 0))
        status = resultado.get("status", "OK" if total_trailer == total_processado else "ERRO")
        detalhe = resultado.get("detalhe", "")

        # 🕒 REGISTRA NO CSV DE LOG
        log_result(filename, tipo, total_trailer, total_processado, status, detalhe)

        # ==================================================
        # 📦 RETORNO PARA O FRONT (com suporte ao EEVC)
        # ==================================================
        gerados = resultado.get("gerados", []) if isinstance(resultado, dict) else []
        data_ref = resultado.get("data_ref", "")
        nsa = resultado.get("nsa", "")
        lotes_count = len(gerados)

        print("------------------------------------------------")
        print(f"✅ {filename} ({tipo})")
        print(f"   ▸ Status..........: {status}")
        print(f"   ▸ Total trailer...: {total_trailer}")
        print(f"   ▸ Total processado: {total_processado}")
        print(f"   ▸ Arquivos gerados: {lotes_count}")
        print("------------------------------------------------")

        return {
            "arquivo": filename,
            "tipo": tipo,
            "status": status,
            "detalhe": detalhe,
            "total_trailer": total_trailer,
            "total_processado": total_processado,
            "data_ref": data_ref,
            "nsa": nsa,
            "gerados": gerados,
            "lotes_count": lotes_count
        }

    except Exception as e:
        log_result(filename, tipo or "DESCONHECIDO", 0, 0, "ERRO", str(e))
        print(f"❌ Falha ao processar {filename}: {e}")
        return {
            "erro": str(e),
            "arquivo": filename,
            "tipo": tipo or "DESCONHECIDO",
            "status": "ERRO",
            "detalhe": str(e),
            "gerados": []
        }
