# =============================================================
# validator_core.py
# Núcleo de Validação – Netunna Splitter Framework
# v1.0 | Base genérica para EEVC, EEVD e EEFI
# =============================================================

from pathlib import Path
import csv
import logging
import re
from collections import defaultdict

# -------------------------------------------------------------
# CONFIGURAÇÃO DE LOG
# -------------------------------------------------------------
logger = logging.getLogger("splitter_validator")
logger.setLevel(logging.INFO)

# -------------------------------------------------------------
# FUNÇÕES AUXILIARES
# -------------------------------------------------------------
def extract_tipo(line: str) -> str:
    """Retorna o tipo do registro (3 primeiros caracteres)."""
    return line[:3]

def extract_pv(line: str) -> str | None:
    """Extrai o número do PV (preferencialmente na faixa 3–12 ou padrão 9 dígitos)."""
    pv = line[3:12].strip()
    if pv.isdigit() and len(pv) == 9:
        return pv
    m = re.search(r"\d{9}", line[:80])
    return m.group(0) if m else None

# -------------------------------------------------------------
def indexar_arquivo(arquivo: Path | str, tipos_validos: tuple[str, ...]) -> dict[str, list[str]]:
    """
    Lê um arquivo e indexa os tipos de registro por PV.
    Retorna um dicionário { pv: [tipos...] }.
    """
    registros = defaultdict(list)
    with open(arquivo, encoding="utf-8", errors="ignore") as f:
        for ln in f:
            tipo = extract_tipo(ln)
            if tipo not in tipos_validos:
                continue
            pv = extract_pv(ln)
            if not pv:
                continue
            registros[pv].append(tipo)
    return dict(registros)

# -------------------------------------------------------------
def comparar(mae_dict: dict[str, list[str]], filhos_dict: dict[str, list[str]]) -> list[list]:
    """
    Compara os dicionários de registros mãe e filhos.
    Retorna uma lista de resultados [PV, Tipo, Qtd_Mãe, Qtd_Filho, Status].
    """
    resultados = []
    todos_pvs = sorted(set(mae_dict.keys()) | set(filhos_dict.keys()))

    for pv in todos_pvs:
        tipos_mae = mae_dict.get(pv, [])
        tipos_filho = filhos_dict.get(pv, [])

        cont_mae = {t: tipos_mae.count(t) for t in set(tipos_mae)}
        cont_filho = {t: tipos_filho.count(t) for t in set(tipos_filho)}
        todos_tipos = sorted(set(cont_mae.keys()) | set(cont_filho.keys()))

        for tipo in todos_tipos:
            mae_qtd = cont_mae.get(tipo, 0)
            filho_qtd = cont_filho.get(tipo, 0)
            if mae_qtd == filho_qtd:
                status = "OK"
            elif filho_qtd < mae_qtd:
                status = "Faltando"
            else:
                status = "Extra"
            resultados.append([pv, tipo, mae_qtd, filho_qtd, status])

    return resultados

# -------------------------------------------------------------
def gerar_csv(resultados: list[list], arquivo_csv: Path | str) -> Path:
    """Gera o relatório CSV consolidado."""
    with open(arquivo_csv, "w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp, delimiter=";")
        writer.writerow(["PV", "Tipo", "Qtd_Mãe", "Qtd_Filho", "Status"])
        writer.writerows(resultados)
    return Path(arquivo_csv)

# -------------------------------------------------------------
def validar_generico(
    tipo: str,
    arquivo_mae: str,
    pasta_filhos: str,
    tipos_validos: tuple[str, ...],
    relatorio_nome: str
) -> dict:
    """
    Função genérica de validação.
    Lê o arquivo mãe, varre os filhos e gera o relatório consolidado.
    Retorna um dicionário JSON com status, mensagem e caminho do relatório.
    """
    logger.info(f"🔍 Iniciando validação {tipo}")
    logger.info(f"Arquivo mãe: {arquivo_mae}")
    logger.info(f"Pasta filhos: {pasta_filhos}")

    arquivo_mae = Path(arquivo_mae)
    pasta_filhos = Path(pasta_filhos)
    relatorio_csv = pasta_filhos / relatorio_nome

    if not arquivo_mae.exists():
        logger.error(f"❌ Arquivo mãe não encontrado: {arquivo_mae}")
        return {"ok": False, "mensagem": f"Arquivo mãe não encontrado: {arquivo_mae}"}

    if not pasta_filhos.exists() or not any(pasta_filhos.glob("*.txt")):
        logger.error(f"❌ Nenhum arquivo filho encontrado em: {pasta_filhos}")
        return {"ok": False, "mensagem": f"Nenhum arquivo filho encontrado em: {pasta_filhos}"}

    # Indexação
    mae_dict = indexar_arquivo(arquivo_mae, tipos_validos)
    filhos_dict = {}

    for child in pasta_filhos.glob("*.txt"):
        pv = child.stem.split("_")[0]
        filhos_dict[pv] = []
        filhos_dict[pv].extend(sum(indexar_arquivo(child, tipos_validos).values(), []))

    # Comparação
    resultados = comparar(mae_dict, filhos_dict)

    # CSV
    gerar_csv(resultados, relatorio_csv)

    # Resumo
    total_ok = sum(1 for r in resultados if r[-1] == "OK")
    total_faltando = sum(1 for r in resultados if r[-1] == "Faltando")
    total_extra = sum(1 for r in resultados if r[-1] == "Extra")

    logger.info(f"✅ Validação {tipo} concluída: OK={total_ok}, Faltando={total_faltando}, Extra={total_extra}")
    logger.info(f"Relatório salvo em: {relatorio_csv}")

    return {
        "ok": True,
        "mensagem": (
            f"Validação {tipo} concluída — {len(mae_dict)} PVs "
            f"({total_ok} OK, {total_faltando} faltando, {total_extra} extra)"
        ),
        "relatorio": str(relatorio_csv)
    }
