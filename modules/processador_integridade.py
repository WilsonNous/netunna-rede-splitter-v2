# =============================================================
# processador_integridade.py
# Processador de Integridade – Netunna Splitter Framework
# v1.0 | Unifica validações EEVC, EEVD e EEFI
# =============================================================

from modules.validator_core import validar_generico

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

    return validar_generico(tipo, arquivo_mae, pasta_filhos, tipos_validos, relatorio_nome)
