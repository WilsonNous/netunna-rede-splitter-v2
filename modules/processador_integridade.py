# =============================================================
# processador_integridade.py
# Processador de Integridade – Netunna Splitter Framework
# v1.1 | Unifica validações EEVC, EEVD e EEFI + Registro de Log
# =============================================================

import os
import csv
import logging
from modules.validator_core import validar_generico
from utils.log_utils import log_result

logger = logging.getLogger("processador_integridade")
logger.setLevel(logging.INFO)


# -------------------------------------------------------------
def processar_integridade(tipo: str, arquivo_mae: str, pasta_filhos: str):
    """
    Executa o processo de validação de integridade entre o arquivo mãe
    e os arquivos filhos, de acordo com o tipo informado.

    tipo: 'EEVC' | 'EEVD' | 'EEFI'
    arquivo_mae: caminho completo do arquivo consolidado (mãe)
    pasta_filhos: diretório onde estão os arquivos splitados por PV
    """

    tipo = tipo.upper().strip()

    if tipo == "EEVC":
        tipos_validos = ("002", "004", "006", "010", "016", "022")
        relatorio_nome = "report_integridade_EEVC.csv"

    elif tipo == "EEVD":
        tipos_validos = ("002", "004", "006", "010", "016", "022")
        relatorio_nome = "report_integridade_EEVD.csv"

    elif tipo == "EEFI":
        tipos_validos = ("034", "035", "036", "038", "040", "043", "045")
        relatorio_nome = "report_integridade_EEFI.csv"

    else:
        raise ValueError(f"Tipo de arquivo inválido para validação: {tipo}")

    # -----------------------------------------------------
    # 🔹 Executa a validação genérica (módulo validator_core)
    # -----------------------------------------------------
    resultados = validar_generico(tipo, arquivo_mae, pasta_filhos, tipos_validos, relatorio_nome)

    # -----------------------------------------------------
    # 🔍 Avalia integridade geral
    # -----------------------------------------------------
    ok_count = sum(1 for r in resultados if r[-1] == "OK")
    faltando_count = sum(1 for r in resultados if r[-1] == "Faltando")
    extra_count = sum(1 for r in resultados if r[-1] == "Extra")

    if faltando_count or extra_count:
        status_geral = "FALHA"
        motivo = f"Divergências detectadas — Faltando: {faltando_count}, Extras: {extra_count}"
    else:
        status_geral = "OK"
        motivo = "Integridade confirmada"

    # -----------------------------------------------------
    # 🪵 Registra resultado no log Netunna padrão
    # -----------------------------------------------------
    try:
        log_result(
            os.path.basename(arquivo_mae),
            tipo,
            len(resultados),
            ok_count,
            status_geral,
            motivo
        )
        logger.info(f"🧾 Log de integridade registrado para {arquivo_mae}")
    except Exception as e:
        logger.warning(f"⚠️ Falha ao registrar log de integridade: {e}")

    # -----------------------------------------------------
    # 🔙 Retorno JSON simplificado para API e painel
    # -----------------------------------------------------
    return {
        "ok": status_geral == "OK",
        "mensagem": motivo,
        "status": status_geral,
        "detalhe": motivo,
        "arquivo": os.path.basename(arquivo_mae),
        "tipo": tipo
    }
