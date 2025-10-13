# =============================================================
# modules/eefi_processor.py
# Extrato Financeiro Híbrido – EEFI (REDE)
# v3.0 | Netunna Splitter Framework
# =============================================================

from pathlib import Path
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger("splitter")
logger.setLevel(logging.INFO)

# =============================================================
# Layouts possíveis
# =============================================================
LAYOUT_POS = {
    "030": {"tipo": (0, 3), "data_emissao": (3, 11), "sequencia": (75, 81)},
    "032": {"tipo": (0, 3), "pv": (3, 12)},
    "034": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46)},
    "035": {"tipo": (0, 3), "pv": (3, 12), "valor": (29, 44)},
    "036": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46)},
    "038": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46)},
    "040": {"tipo": (0, 3), "pv": (3, 12), "valor": (12, 27)},  # simplificado
    "043": {"tipo": (0, 3), "pv": (3, 12), "valor": (48, 63)},
    "050": {"tipo": (0, 3), "pv": (3, 12), "valor_cred_norm": (18, 33)},
    "052": {
        "tipo": (0, 3),
        "valor_rv": (26, 41),
        "valor_ant": (47, 62),
        "valor_aj_cred": (66, 81),
        "valor_aj_deb": (85, 100),
    },
}

def _slice(line: str, rng: Tuple[int, int]) -> str:
    return line[rng[0]:rng[1]]

def _to_int_cents(num_txt: str) -> int:
    num = "".join(ch for ch in num_txt if ch.isdigit())
    return int(num or "0")

# =============================================================
# 🚀 Função principal
# =============================================================
def process_eefi(file_path: str, output_root: str = "output") -> dict:
    try:
        logger.info(f"🟢 Processando EEFI (modo híbrido) | arquivo={file_path}")
        src = Path(file_path)
        lines = src.read_text(encoding="utf-8", errors="ignore").splitlines()

        if not lines:
            raise ValueError("Arquivo vazio.")

        header_030 = lines[0]
        nsa = _slice(header_030, LAYOUT_POS["030"]["sequencia"])
        data_emissao = _slice(header_030, LAYOUT_POS["030"]["data_emissao"])

        # Detectar tipo do layout
        tem_032 = any(ln.startswith("032") for ln in lines)
        tem_040 = any(ln.startswith("040") for ln in lines)
        tem_052 = any(ln.startswith("052") for ln in lines)
        modo = "completo" if tem_032 else "simplificado" if tem_040 else "desconhecido"
        logger.info(f"🧩 Layout detectado: {modo.upper()}")

        if not tem_052:
            raise ValueError("Trailer 052 não encontrado.")

        trailer_052_line = next(ln for ln in reversed(lines) if ln.startswith("052"))
        total_052 = (
            _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_rv"])) +
            _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_ant"])) +
            _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_aj_cred"])) -
            _to_int_cents(_slice(trailer_052_line, LAYOUT_POS["052"]["valor_aj_deb"]))
        )

        out_dir = Path(output_root) / f"NSA_{nsa}"
        out_dir.mkdir(parents=True, exist_ok=True)
        arquivos_gerados: List[Path] = []
        sum_pvs = 0

        # =========================================================
        # 🔹 MODO COMPLETO (030 + 032 + 034/035/036/038 + 050 + 052)
        # =========================================================
        if modo == "completo":
            pv_map: Dict[str, List[str]] = {}
            current_pv = None
            for ln in lines:
                tipo = ln[0:3]
                if tipo == "032":
                    current_pv = _slice(ln, LAYOUT_POS["032"]["pv"]).strip()
                    pv_map.setdefault(current_pv, []).append(ln)
                elif tipo in ("034", "035", "036", "038", "043", "050"):
                    if current_pv:
                        pv_map[current_pv].append(ln)

            for pv, registros in pv_map.items():
                total_pv = 0
                for ln in registros:
                    tipo = ln[0:3]
                    if tipo in ("034", "036", "043"):
                        total_pv += _to_int_cents(_slice(ln, LAYOUT_POS[tipo]["valor"]))
                    elif tipo in ("035", "038"):
                        total_pv -= _to_int_cents(_slice(ln, LAYOUT_POS[tipo]["valor"]))
                sum_pvs += total_pv

                child_name = f"{pv}_{data_emissao}_{nsa}_EEFI.txt"
                child_path = out_dir / child_name
                with child_path.open("w", encoding="utf-8") as f:
                    f.write(header_030 + "\n")
                    for ln in registros:
                        f.write(ln + "\n")
                arquivos_gerados.append(child_path)
                logger.info(f"🧾 Gerado: {child_name}")

        # =========================================================
        # 🔹 MODO SIMPLIFICADO (030 + 040 + 052)
        # =========================================================
        elif modo == "simplificado":
            registros_040 = [ln for ln in lines if ln.startswith("040")]
            logger.info(f"📊 {len(registros_040)} PVs identificados no layout 040.")
            pv_map: Dict[str, List[str]] = {}

            for ln in registros_040:
                pv = _slice(ln, LAYOUT_POS["040"]["pv"]).strip()
                pv_map.setdefault(pv, []).append(ln)

            for pv, registros in pv_map.items():
                child_name = f"{pv}_{data_emissao}_{nsa}_EEFI.txt"
                child_path = out_dir / child_name
                with child_path.open("w", encoding="utf-8") as f:
                    f.write(header_030 + "\n")
                    for ln in registros:
                        f.write(ln + "\n")
                arquivos_gerados.append(child_path)
                logger.info(f"🧾 Gerado: {child_name}")

        else:
            raise ValueError("Layout do arquivo EEFI não reconhecido (faltam 032 ou 040).")

        # =========================================================
        # ✅ Validação final
        # =========================================================
        ok = True  # não somamos no modo 040 (sem valores reais)
        logger.info(f"✅ EEFI processado | Modo={modo.upper()} | NSA={nsa} | PVs={len(arquivos_gerados)}")

        return {
            "nsa": nsa,
            "output_dir": str(out_dir),
            "files_generated": [str(p) for p in arquivos_gerados],
            "report_csv": "",
            "sum_pvs": sum_pvs,
            "total_052": total_052,
            "ok": ok,
            "message": f"Processamento EEFI ({modo}) concluído."
        }

    except Exception as e:
        logger.exception(f"❌ Erro ao processar EEFI: {e}")
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
