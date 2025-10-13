# =============================================================
# modules/eefi_processor.py
# Extrato Financeiro Completo – EEFI (REDE)
# v2.0 | Netunna Splitter Framework
# =============================================================

from pathlib import Path
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger("splitter")
logger.setLevel(logging.INFO)

# =============================================================
# 📋 Layout Posições (EEFI completo)
# =============================================================
LAYOUT_POS = {
    "030": {  # Header arquivo
        "tipo": (0, 3),
        "data_emissao": (3, 11),
        "sequencia": (75, 81),
    },
    "032": {  # Header da matriz/PV
        "tipo": (0, 3),
        "pv": (3, 12),
    },
    # Registros financeiros
    "034": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46)},  # Crédito
    "035": {"tipo": (0, 3), "pv": (3, 12), "valor": (29, 44)},  # Ajuste NET/Débito
    "036": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46)},  # Antecipação
    "038": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46)},  # Débito via banco
    "043": {"tipo": (0, 3), "pv": (3, 12), "valor": (48, 63)},  # Ajuste a crédito
    "050": {  # Totalizador PV
        "tipo": (0, 3),
        "pv": (3, 12),
        "valor_cred_norm": (18, 33),
        "valor_ant": (39, 54),
        "valor_aj_cred": (58, 73),
        "valor_aj_deb": (79, 94),
    },
    "052": {  # Trailer geral
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
    s = str(value_cents).rjust(rng[1] - rng[0], "0")
    return line[:rng[0]] + s + line[rng[1]:]

# =============================================================
# 🚀 Função principal
# =============================================================
def process_eefi(file_path: str, output_root: str = "output") -> dict:
    try:
        logger.info(f"🟢 Processando EEFI Completo | arquivo={file_path}")
        src = Path(file_path)
        lines = src.read_text(encoding="utf-8", errors="ignore").splitlines()

        # HEADER
        header_030 = lines[0]
        nsa = _slice(header_030, LAYOUT_POS["030"]["sequencia"])
        data_emissao = _slice(header_030, LAYOUT_POS["030"]["data_emissao"])

        # TRAILER 052
        trailer_052_line = next((ln for ln in reversed(lines) if ln.startswith("052")), None)
        if not trailer_052_line:
            raise ValueError("Trailer 052 não encontrado.")
        total_052 = (
            _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_rv"])) +
            _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_ant"])) +
            _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_aj_cred"])) -
            _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_aj_deb"]))
        )

        # -----------------------------------------------------
        # Agrupamento por PV
        # -----------------------------------------------------
        pv_map: Dict[str, List[str]] = {}
        current_pv = None

        for ln in lines:
            tipo = ln[0:3]
            if tipo == "032":
                current_pv = _slice(ln, LAYOUT_POS["032"]["pv"]).strip()
                pv_map.setdefault(current_pv, []).append(ln)
            elif tipo in ("034", "035", "036", "038", "043", "050"):
                if not current_pv:
                    logger.warning(f"⚠️ Registro {tipo} sem PV anterior — ignorado.")
                    continue
                pv_map[current_pv].append(ln)
            # ignora 052 (global)

        # -----------------------------------------------------
        # Geração dos arquivos filhos
        # -----------------------------------------------------
        out_dir = Path(output_root) / f"NSA_{nsa}"
        out_dir.mkdir(parents=True, exist_ok=True)

        arquivos_gerados: List[Path] = []
        sum_pvs = 0

        for pv, registros in pv_map.items():
            # soma todos os valores relevantes para conferência
            total_pv = 0
            for ln in registros:
                tipo = ln[0:3]
                if tipo in ("034", "036", "043"):  # créditos
                    total_pv += _to_int_cents(_slice(ln, LAYOUT_POS[tipo]["valor"]))
                elif tipo in ("035", "038"):       # débitos
                    total_pv -= _to_int_cents(_slice(ln, LAYOUT_POS[tipo]["valor"]))
                elif tipo == "050":               # já totalizado no trailer do PV
                    total_pv += _to_int_cents(_slice(ln, LAYOUT_POS["050"]["valor_cred_norm"]))

            sum_pvs += total_pv

            # gera arquivo filho
            child_name = f"{pv}_{data_emissao}_{nsa}_EEFI.txt"
            child_path = out_dir / child_name
            with child_path.open("w", encoding="utf-8") as f:
                f.write(header_030 + "\n")
                for ln in registros:
                    f.write(ln + "\n")
            arquivos_gerados.append(child_path)
            logger.info(f"🧾 Gerado: {child_name}")

        # -----------------------------------------------------
        # Validação final
        # -----------------------------------------------------
        ok = (sum_pvs == total_052)
        if ok:
            logger.info(f"✅ Validação OK | Soma PVs={sum_pvs} == Trailer 052={total_052}")
        else:
            logger.error(f"❌ Validação FALHOU | Soma PVs={sum_pvs} != Trailer 052={total_052}")

        return {
            "nsa": nsa,
            "output_dir": str(out_dir),
            "files_generated": [str(p) for p in arquivos_gerados],
            "report_csv": "",  # compatibilidade, mas não gera arquivo
            "sum_pvs": sum_pvs,
            "total_052": total_052,
            "ok": ok,
            "message": "Processamento EEFI completo concluído."
        }

    except Exception as e:
        logger.exception(f"❌ Erro ao processar EEFI completo: {e}")
        return {
            "nsa": "000000",
            "output_dir": "",
            "files_generated": [],
            "report_csv": "",
            "sum_pvs": 0,
            "total_052": 0,
            "ok": False,
            "message": f"Erro: {str(e)}"
        }
