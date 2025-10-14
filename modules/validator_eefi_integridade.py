# =============================================================
# validator_eefi_integridade.py
# Validador de integridade entre EEFI Mãe e Filhos
# v1.0 | Netunna Splitter Framework
# =============================================================

from pathlib import Path
import csv
import logging

logger = logging.getLogger("eefi_validator")
logger.setLevel(logging.INFO)

def _slice(line, a, b): return line[a:b]
def _extract_tipo(line): return line[:3]
def _extract_pv(line):
    # tenta 3–12, se falhar tenta achar 9 dígitos
    pv = line[3:12].strip()
    if pv.isdigit() and len(pv) == 9:
        return pv
    import re
    m = re.search(r"\d{9}", line[:60])
    return m.group(0) if m else None

# -------------------------------------------------------------
def indexar_arquivo(arquivo):
    registros = {}
    with open(arquivo, encoding="utf-8", errors="ignore") as f:
        for ln in f:
            tipo = _extract_tipo(ln)
            if tipo not in ("034", "035", "036", "038", "040", "043", "045"):
                continue
            pv = _extract_pv(ln)
            if not pv:
                continue
            registros.setdefault(pv, []).append(tipo)
    return registros

# -------------------------------------------------------------
def validar_integridade(arquivo_mae, pasta_filhos, relatorio_csv="report_integridade_EEFI.csv"):
    logger.info(f"🔎 Validando integridade EEFI | mãe={arquivo_mae} | filhos={pasta_filhos}")

    mae = indexar_arquivo(arquivo_mae)
    filhos = {}
    for child in Path(pasta_filhos).glob("*.txt"):
        pv = child.stem.split("_")[0]
        filhos[pv] = indexar_arquivo(child)

    resultados = []
    for pv, tipos_mae in mae.items():
        tipos_filho = filhos.get(pv, {})
        tipos_mae_contagem = {}
        tipos_filho_contagem = {}
        for t in tipos_mae:
            tipos_mae_contagem[t] = tipos_mae_contagem.get(t, 0) + 1
        for t_list in tipos_filho.values() if isinstance(tipos_filho, dict) else tipos_filho:
            # se for dicionário, significa que o filho foi indexado como pv->list
            for t in t_list:
                tipos_filho_contagem[t] = tipos_filho_contagem.get(t, 0) + 1

        for tipo in sorted(set(list(tipos_mae_contagem.keys()) + list(tipos_filho_contagem.keys()))):
            mae_qtd = tipos_mae_contagem.get(tipo, 0)
            filho_qtd = tipos_filho_contagem.get(tipo, 0)
            status = "OK" if mae_qtd == filho_qtd else ("Faltando" if filho_qtd < mae_qtd else "Extra")
            resultados.append([pv, tipo, mae_qtd, filho_qtd, status])

    # grava csv
    with open(relatorio_csv, "w", newline="", encoding="utf-8") as fp:
        w = csv.writer(fp, delimiter=";")
        w.writerow(["PV", "Tipo", "Qtd_Mãe", "Qtd_Filho", "Status"])
        for r in resultados:
            w.writerow(r)

    logger.info(f"✅ Validação concluída. Relatório: {relatorio_csv}")
    return resultados
