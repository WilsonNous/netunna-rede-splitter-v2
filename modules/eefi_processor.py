# =============================================================
# modules/eefi_processor.py
# Extrato Eletrônico Financeiro – EEFI (REDE)
# v1.0 | Netunna Splitter Framework
# =============================================================

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple
import csv
import logging

logger = logging.getLogger("splitter")
logger.setLevel(logging.INFO)

# =============================================================
# 📋 Layout Posições (confirmado via Instruções Básicas - EEFI)
# =============================================================
# Indexação 0-based para slicing Python
LAYOUT_POS = {
    "030": {  # Header arquivo
        "tipo": (0, 3),
        "data_emissao": (3, 11),
        "sequencia": (75, 81),
        "pv_grupo": (81, 90),
    },
    "032": {  # Header da matriz/PV
        "tipo": (0, 3),
        "pv": (3, 12),
        "nome": (12, 34),
    },
    # Registros de movimento (financeiros)
    "034": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46), "ind_cd": (46, 47)},  # Crédito
    "035": {"tipo": (0, 3), "pv": (3, 12), "valor": (29, 44), "ind_cd": (44, 45)},  # Ajuste NET/Débito
    "036": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46), "ind_cd": (46, 47)},  # Antecipação
    "038": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46), "ind_cd": (46, 47)},  # Débito via banco
    "043": {"tipo": (0, 3), "pv": (3, 12), "valor": (48, 63), "ind_cd": (63, 64)},  # Ajuste a crédito
    "050": {  # Trailer matriz (totalizador PV)
        "tipo": (0, 3),
        "pv": (3, 12),
        "qtd_cred_norm": (12, 18),
        "valor_cred_norm": (18, 33),
        "qtd_ant": (33, 39),
        "valor_ant": (39, 54),
        "qtd_aj_cred": (54, 58),
        "valor_aj_cred": (58, 73),
        "qtd_aj_deb": (73, 79),
        "valor_aj_deb": (79, 94),
    },
    "052": {  # Trailer geral (totalizador do arquivo)
        "tipo": (0, 3),
        "valor_rv": (26, 41),
        "valor_ant": (47, 62),
        "valor_aj_cred": (66, 81),
        "valor_aj_deb": (85, 100),
    },
}

# =============================================================
# ⚙️ Funções utilitárias
# =============================================================
def _slice(line: str, rng: Tuple[int, int]) -> str:
    return line[rng[0]:rng[1]]

def _to_int_cents(num_txt: str) -> int:
    num = "".join(ch for ch in num_txt if ch.isdigit())
    return int(num or "0")

def _write_money(line: str, rng: Tuple[int, int], value_cents: int) -> str:
    s = str(value_cents).rjust(rng[1]-rng[0], "0")
    return line[:rng[0]] + s[:rng[1]-rng[0]] + line[rng[1]:]

def _write_number(line: str, rng: Tuple[int, int], value: int) -> str:
    s = str(value).rjust(rng[1]-rng[0], "0")
    return line[:rng[0]] + s[:rng[1]-rng[0]] + line[rng[1]:]

# =============================================================
# 📊 Estruturas de dados
# =============================================================
@dataclass
class PVTotals:
    pv: str
    qtd_cred_norm: int = 0
    qtd_ant: int = 0
    qtd_aj_cred: int = 0
    qtd_aj_deb: int = 0
    valor_cred_norm: int = 0
    valor_ant: int = 0
    valor_aj_cred: int = 0
    valor_aj_deb: int = 0
    lines: List[str] = field(default_factory=list)

    @property
    def valor_total(self) -> int:
        return self.valor_cred_norm + self.valor_ant + self.valor_aj_cred - self.valor_aj_deb

@dataclass
class ProcessResult:
    nsa: str
    output_dir: Path
    files_generated: List[Path]
    report_csv: Path
    sum_pvs: int
    total_052: int
    ok: bool
    message: str = ""

# =============================================================
# 🚀 Função principal de processamento
# =============================================================
def process(file_path: str, output_root: str = "output") -> ProcessResult:
    logger.info(f"🟢 Processando EEFI | arquivo={file_path}")
    src = Path(file_path)
    lines = src.read_text(encoding="utf-8", errors="ignore").splitlines()

    header_030 = lines[0]
    nsa = _slice(header_030, LAYOUT_POS["030"]["sequencia"])
    data_emissao = _slice(header_030, LAYOUT_POS["030"]["data_emissao"])

    trailer_052_line = next((ln for ln in reversed(lines) if ln.startswith("052")), None)
    if not trailer_052_line:
        raise ValueError("Trailer 052 não encontrado.")
    total_052 = (
        _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_rv"])) +
        _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_ant"])) +
        _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_aj_cred"])) -
        _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_aj_deb"]))
    )

    pv_map: Dict[str, PVTotals] = {}
    current_pv = None

    for ln in lines:
        tipo = ln[0:3]
        if tipo == "032":
            pv = _slice(ln, LAYOUT_POS["032"]["pv"]).strip()
            current_pv = pv_map.setdefault(pv, PVTotals(pv=pv))
            current_pv.lines.append(ln)
        elif tipo in ("034", "035", "036", "038", "043"):
            if not current_pv:
                raise ValueError("Registro financeiro sem PV (032) precedente.")
            valor = _to_int_cents(_slice(ln, LAYOUT_POS[tipo]["valor"]))
            ind = ln[LAYOUT_POS[tipo]["ind_cd"][0]:LAYOUT_POS[tipo]["ind_cd"][1]]

            if tipo == "034":  # Crédito normal
                current_pv.qtd_cred_norm += 1
                current_pv.valor_cred_norm += valor
            elif tipo == "036":  # Antecipação
                current_pv.qtd_ant += 1
                current_pv.valor_ant += valor
            elif tipo == "043":  # Ajuste a crédito
                current_pv.qtd_aj_cred += 1
                current_pv.valor_aj_cred += valor
            elif tipo in ("035", "038"):  # Débitos
                current_pv.qtd_aj_deb += 1
                current_pv.valor_aj_deb += valor

            current_pv.lines.append(ln)
        # Ignora 050/052 do original (serão recalculados)

    out_dir = Path(output_root) / f"NSA_{nsa}"
    out_dir.mkdir(parents=True, exist_ok=True)

    files_out: List[Path] = []
    sum_pvs = 0

    for pv, totals in pv_map.items():
        sum_pvs += totals.valor_total

        trailer_050 = " " * 400
        trailer_050 = _write_money(trailer_050, LAYOUT_POS["050"]["valor_cred_norm"], totals.valor_cred_norm)
        trailer_050 = _write_money(trailer_050, LAYOUT_POS["050"]["valor_ant"], totals.valor_ant)
        trailer_050 = _write_money(trailer_050, LAYOUT_POS["050"]["valor_aj_cred"], totals.valor_aj_cred)
        trailer_050 = _write_money(trailer_050, LAYOUT_POS["050"]["valor_aj_deb"], totals.valor_aj_deb)
        trailer_050 = _write_number(trailer_050, LAYOUT_POS["050"]["qtd_cred_norm"], totals.qtd_cred_norm)
        trailer_050 = _write_number(trailer_050, LAYOUT_POS["050"]["qtd_ant"], totals.qtd_ant)
        trailer_050 = _write_number(trailer_050, LAYOUT_POS["050"]["qtd_aj_cred"], totals.qtd_aj_cred)
        trailer_050 = _write_number(trailer_050, LAYOUT_POS["050"]["qtd_aj_deb"], totals.qtd_aj_deb)
        trailer_050 = _write_number(trailer_050, LAYOUT_POS["050"]["pv"], int(pv))

        child_lines = [header_030] + totals.lines + [trailer_050]
        child_name = f"{pv}_{data_emissao}_{nsa}_EEFI.txt"
        child_path = out_dir / child_name
        child_path.write_text("\n".join(child_lines) + "\n", encoding="utf-8")
        files_out.append(child_path)
        logger.info(f"🧾 Gerado: {child_name}")

    # CSV de validação
    report_csv = out_dir / f"report_EEFI_{nsa}.csv"
    with report_csv.open("w", newline="", encoding="utf-8") as fp:
        w = csv.writer(fp, delimiter=";")
        w.writerow(["PV", "Creditos", "Antecipacoes", "Aj_Cred", "Aj_Deb", "Valor_Total"])
        for pv, totals in pv_map.items():
            w.writerow([pv, totals.qtd_cred_norm, totals.qtd_ant, totals.qtd_aj_cred, totals.qtd_aj_deb, totals.valor_total])
        w.writerow([])
        w.writerow(["Soma_PVs", "", "", "", "", sum_pvs])
        w.writerow(["Total_052", "", "", "", "", total_052])
        w.writerow(["Validacao_OK", "", "", "", "", "SIM" if sum_pvs == total_052 else "NAO"])

    ok = sum_pvs == total_052
    if ok:
        logger.info(f"✅ Validação OK | Soma PVs={sum_pvs} == Trailer 052={total_052}")
    else:
        logger.error(f"❌ Validação FALHOU | Soma PVs={sum_pvs} != Trailer 052={total_052}")

    return ProcessResult(
        nsa=nsa,
        output_dir=out_dir,
        files_generated=files_out,
        report_csv=report_csv,
        sum_pvs=sum_pvs,
        total_052=total_052,
        ok=ok,
        message="Processamento EEFI concluído."
    )
