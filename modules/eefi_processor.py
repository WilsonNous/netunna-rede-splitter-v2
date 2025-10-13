# =============================================================
# modules/eefi_processor.py
# Extrato Financeiro Híbrido – EEFI (REDE / NETUNNA)
# v5.0 | Netunna Splitter Framework
# =============================================================

from pathlib import Path
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger("splitter")
logger.setLevel(logging.INFO)

# =============================================================
# 📋 Layout Posições – conforme EEFI v4.00 (REDE) + modo simplificado (NETUNNA)
# =============================================================
LAYOUT_POS = {
    "030": {"tipo": (0, 3), "data_emissao": (3, 11), "sequencia": (75, 81)},
    "032": {"tipo": (0, 3), "pv": (3, 12)},
    "034": {"tipo": (0, 3), "valor": (31, 46)},
    "035": {"tipo": (0, 3), "valor": (29, 44)},
    "036": {"tipo": (0, 3), "valor": (31, 46)},
    "038": {"tipo": (0, 3), "valor": (31, 46)},
    "040": {"tipo": (0, 3), "pv": (3, 12), "valor": (12, 27)},  # simplificado
    "043": {"tipo": (0, 3), "valor": (48, 63)},
    "050": {
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
    "052": {
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

def _write_number(line: str, rng: Tuple[int, int], value: int) -> str:
    s = str(value).rjust(rng[1] - rng[0], "0")
    return line[:rng[0]] + s + line[rng[1]:]

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

        # Detectar tipo
        tem_032 = any(ln.startswith("032") for ln in lines)
        tem_040 = any(ln.startswith("040") for ln in lines)
        tem_052 = any(ln.startswith("052") for ln in lines)

        modo = "completo" if tem_032 else "simplificado" if tem_040 else "desconhecido"
        logger.info(f"🧩 Layout detectado: {modo.upper()}")

        # Trailer geral (se existir)
        total_052 = 0
        if tem_052:
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

        # =========================================================
        # 🔹 MODO COMPLETO (032/034–038/043)
        # =========================================================
        if modo == "completo":
            pv_map: Dict[str, List[str]] = {}
            current_pv = None
            for ln in lines:
                tipo = ln[0:3]
                if tipo == "032":
                    current_pv = _slice(ln, LAYOUT_POS["032"]["pv"]).strip()
                    pv_map.setdefault(current_pv, []).append(ln)
                elif tipo in ("034", "035", "036", "038", "043"):
                    if current_pv:
                        pv_map[current_pv].append(ln)

        # =========================================================
        # 🔹 MODO SIMPLIFICADO (030/040)
        # =========================================================
        elif modo == "simplificado":
            pv_map: Dict[str, List[str]] = {}
            for ln in lines:
                if ln.startswith("040"):
                    pv = _slice(ln, LAYOUT_POS["040"]["pv"]).strip()
                    pv_map.setdefault(pv, []).append(ln)
        else:
            raise ValueError("Layout EEFI não reconhecido (faltam 032 ou 040).")

        # =========================================================
        # 🔹 Geração dos arquivos filhos
        # =========================================================
        soma_total_arquivos = 0
        for pv, registros in pv_map.items():
            qtd_cred_norm = qtd_ant = qtd_aj_cred = qtd_aj_deb = 0
            valor_cred_norm = valor_ant = valor_aj_cred = valor_aj_deb = 0

            for ln in registros:
                tipo = ln[0:3]
                if tipo in ("034", "036", "043", "035", "038", "040"):
                    valor = _to_int_cents(_slice(ln, LAYOUT_POS[tipo]["valor"]))
                    if tipo == "034":  # crédito normal
                        qtd_cred_norm += 1
                        valor_cred_norm += valor
                    elif tipo == "036":  # antecipação
                        qtd_ant += 1
                        valor_ant += valor
                    elif tipo == "043":  # ajuste crédito
                        qtd_aj_cred += 1
                        valor_aj_cred += valor
                    elif tipo in ("035", "038"):  # débito
                        qtd_aj_deb += 1
                        valor_aj_deb += valor
                    elif tipo == "040":  # modo simplificado
                        qtd_cred_norm += 1  # trata 040 como crédito normal
                        valor_cred_norm += valor

            total_pv = valor_cred_norm + valor_ant + valor_aj_cred - valor_aj_deb
            soma_total_arquivos += total_pv

            # Trailer 050 recalculado
            trailer_050 = " " * 400
            trailer_050 = _write_number(trailer_050, LAYOUT_POS["050"]["pv"], int(pv))
            trailer_050 = _write_number(trailer_050, LAYOUT_POS["050"]["qtd_cred_norm"], qtd_cred_norm)
            trailer_050 = _write_money(trailer_050, LAYOUT_POS["050"]["valor_cred_norm"], valor_cred_norm)
            trailer_050 = _write_number(trailer_050, LAYOUT_POS["050"]["qtd_ant"], qtd_ant)
            trailer_050 = _write_money(trailer_050, LAYOUT_POS["050"]["valor_ant"], valor_ant)
            trailer_050 = _write_number(trailer_050, LAYOUT_POS["050"]["qtd_aj_cred"], qtd_aj_cred)
            trailer_050 = _write_money(trailer_050, LAYOUT_POS["050"]["valor_aj_cred"], valor_aj_cred)
            trailer_050 = _write_number(trailer_050, LAYOUT_POS["050"]["qtd_aj_deb"], qtd_aj_deb)
            trailer_050 = _write_money(trailer_050, LAYOUT_POS["050"]["valor_aj_deb"], valor_aj_deb)

            # Gera o arquivo filho
            child_name = f"{pv}_{data_emissao}_{nsa}_EEFI.txt"
            child_path = out_dir / child_name
            with child_path.open("w", encoding="utf-8") as f:
                f.write(header_030 + "\n")
                for ln in registros:
                    f.write(ln + "\n")
                f.write(trailer_050 + "\n")

            arquivos_gerados.append(child_path)
            logger.info(f"🧾 Gerado: {child_name} | Total={total_pv}")

        # =========================================================
        # 🔹 Validação final
        # =========================================================
        ok = True
        if tem_052:
            ok = soma_total_arquivos == total_052
            if ok:
                logger.info(f"✅ Validação OK | Soma filhos={soma_total_arquivos} == Trailer 052={total_052}")
            else:
                logger.error(f"❌ Validação FALHOU | Soma filhos={soma_total_arquivos} != Trailer 052={total_052}")
        else:
            logger.info(f"⚙️ Arquivo sem trailer 052 – validação global não aplicada.")

        return {
            "nsa": nsa,
            "output_dir": str(out_dir),
            "files_generated": [str(p) for p in arquivos_gerados],
            "report_csv": "",
            "sum_pvs": soma_total_arquivos,
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
