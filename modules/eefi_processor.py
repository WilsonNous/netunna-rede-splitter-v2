# =============================================================
# modules/eefi_processor.py
# Extrato Financeiro – EEFI (REDE / NETUNNA)
# v5.5 | Netunna Splitter Framework
# =============================================================

from pathlib import Path
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger("splitter")
logger.setLevel(logging.INFO)

# -------------------------------------------------------------
# Layout Posições (índices 0-based p/ slicing)
# -------------------------------------------------------------
LAYOUT_POS = {
    # 030 – Header de arquivo
    # 001-003 tipo, 004-011 data (DDMMAAAA), 076-081 sequência,
    # 082-090 PV grupo ou matriz (precisamos sobrescrever no filho)
    "030": {
        "tipo": (0, 3),
        "data_emissao": (3, 11),
        "sequencia": (75, 81),
        "pv_grupo_matriz": (81, 90),
    },

    # 032 – Header Matriz / PV
    "032": {"tipo": (0, 3), "pv": (3, 12)},

    # Registros financeiros (somam no trailer)
    "034": {"tipo": (0, 3), "valor": (31, 46)},  # Crédito normal (C)
    "036": {"tipo": (0, 3), "valor": (31, 46)},  # Antecipação (C)
    "043": {"tipo": (0, 3), "valor": (49, 64)},  # Ajuste a crédito (C) 49–63 => 0-based (48,63) mas vamos seg. manual 49–63 -> (48,63); aqui usamos (49,64) p/ robustez a 1-char var.
    "035": {"tipo": (0, 3), "valor": (30, 45)},  # Ajuste/Net/Desag. (D)
    "038": {"tipo": (0, 3), "valor": (31, 46)},  # Débito via banco (D)

    # 040 – Serasa (NÃO soma financeiramente)
    "040": {"tipo": (0, 3), "pv": (3, 12)},

    # 052 – Trailer (do arquivo mãe e dos filhos)
    # Campos e larguras EXATOS conforme manual
    "052": {
        "tipo": (0, 3),                # "052"
        "qtde_matrizes": (3, 7),       # 4
        "qtde_registros": (7, 13),     # 6
        "pv_solicitante": (13, 22),    # 9
        "qtd_cred_norm": (22, 26),     # 4
        "valor_rv": (26, 41),          # 15
        "qtd_ant": (41, 47),           # 6
        "valor_ant": (47, 62),         # 15
        "qtd_aj_cred": (62, 66),       # 4
        "valor_aj_cred": (66, 81),     # 15
        "qtd_aj_deb": (81, 85),        # 4
        "valor_aj_deb": (85, 100),     # 15
    },
}

# -------------------------------------------------------------
# Utils
# -------------------------------------------------------------
def _slice(line: str, rng: Tuple[int, int]) -> str:
    return line[rng[0]:rng[1]]

def _to_int_cents(num_txt: str) -> int:
    # só dígitos; vazio -> 0
    num = "".join(ch for ch in num_txt if ch.isdigit())
    return int(num or "0")

def _write_fixed(line: str, start: int, end: int, value: str) -> str:
    width = end - start
    s = value[:width].ljust(width)  # alfanum: left pad spaces
    return line[:start] + s + line[end:]

def _write_number(line: str, rng: Tuple[int, int], value: int) -> str:
    width = rng[1] - rng[0]
    s = f"{value:0>{width}}"
    return line[:rng[0]] + s + line[rng[1]:]

def _write_money(line: str, rng: Tuple[int, int], value_cents: int) -> str:
    width = rng[1] - rng[0]
    s = f"{value_cents:0>{width}}"
    return line[:rng[0]] + s + line[rng[1]:]

# -------------------------------------------------------------
# Principal
# -------------------------------------------------------------
def process_eefi(file_path: str, output_root: str = "output") -> dict:
    try:
        logger.info(f"🟢 EEFI | arquivo={file_path}")
        src = Path(file_path)
        lines = src.read_text(encoding="utf-8", errors="ignore").splitlines()

        if not lines:
            raise ValueError("Arquivo vazio.")

        header_030 = lines[0]
        nsa = _slice(header_030, LAYOUT_POS["030"]["sequencia"])
        data_emissao = _slice(header_030, LAYOUT_POS["030"]["data_emissao"])

        tem_032 = any(ln.startswith("032") for ln in lines)
        tem_040 = any(ln.startswith("040") for ln in lines)
        tem_052 = any(ln.startswith("052") for ln in lines)

        if not (tem_032 or tem_040):
            raise ValueError("Layout não reconhecido: não há 032 nem 040.")

        modo = "completo" if tem_032 else "simplificado"
        logger.info(f"🧩 Layout detectado: {modo.upper()}")

        # Trailer do MÃE (052) para validação global (se existir)
        total_mae_052 = 0
        if tem_052:
            trailer_mae_052 = next(ln for ln in reversed(lines) if ln.startswith("052"))
            total_mae_052 = (
                _to_int_cents(_slice(trailer_mae_052, LAYOUT_POS["052"]["valor_rv"])) +
                _to_int_cents(_slice(trailer_mae_052, LAYOUT_POS["052"]["valor_ant"])) +
                _to_int_cents(_slice(trailer_mae_052, LAYOUT_POS["052"]["valor_aj_cred"])) -
                _to_int_cents(_slice(trailer_mae_052, LAYOUT_POS["052"]["valor_aj_deb"]))
            )

        # Agrupamento por PV
        pv_map: Dict[str, List[str]] = {}
        if modo == "completo":
            current_pv = None
            for ln in lines:
                tipo = ln[:3]
                if tipo == "032":
                    current_pv = _slice(ln, LAYOUT_POS["032"]["pv"]).strip()
                    pv_map.setdefault(current_pv, []).append(ln)
                elif tipo in ("034", "035", "036", "038", "043"):
                    if current_pv:
                        pv_map[current_pv].append(ln)
        else:
            for ln in lines:
                if ln.startswith("040"):
                    pv = _slice(ln, LAYOUT_POS["040"]["pv"]).strip()
                    pv_map.setdefault(pv, []).append(ln)

        out_dir = Path(output_root) / f"NSA_{nsa}"
        out_dir.mkdir(parents=True, exist_ok=True)

        arquivos_gerados: List[Path] = []
        soma_filhos = 0
        pv_logs = []

        # Geração de filhos
        for pv, registros in pv_map.items():
            # === Totais por PV (somam somente 034/036/043/035/038) ===
            qtd_cred_norm = qtd_ant = qtd_aj_cred = qtd_aj_deb = 0
            valor_cred_norm = valor_ant = valor_aj_cred = valor_aj_deb = 0

            for ln in registros:
                tipo = ln[:3]
                if tipo in ("034", "036", "043", "035", "038"):
                    pos = LAYOUT_POS[tipo]["valor"]
                    valor = _to_int_cents(_slice(ln, pos))
                    if tipo == "034":
                        qtd_cred_norm += 1
                        valor_cred_norm += valor
                    elif tipo == "036":
                        qtd_ant += 1
                        valor_ant += valor
                    elif tipo == "043":
                        qtd_aj_cred += 1
                        valor_aj_cred += valor
                    elif tipo in ("035", "038"):
                        qtd_aj_deb += 1
                        valor_aj_deb += valor

            total_pv = valor_cred_norm + valor_ant + valor_aj_cred - valor_aj_deb
            soma_filhos += total_pv
            pv_logs.append(f"{pv}={total_pv}")

            # === Header 030 ajustado para o PV do filho (pos. 082–090) ===
            hdr_child = header_030
            hdr_child = _write_fixed(
                hdr_child,
                LAYOUT_POS["030"]["pv_grupo_matriz"][0],
                LAYOUT_POS["030"]["pv_grupo_matriz"][1],
                f"{int(pv):0>9}",
            )

            # === Trailer 052 do FILHO (larguras e posições exatas) ===
            qtd_registros = 2 + len(registros)  # 030 + blocos PV + 052
            # Linha base 400 chars
            trailer_child = " " * 400
            # Tipo
            trailer_child = trailer_child[:0] + "052" + trailer_child[3:]
            # Numéricos com zero-padding nas posições certas:
            trailer_child = _write_number(trailer_child, LAYOUT_POS["052"]["qtde_matrizes"], 1)
            trailer_child = _write_number(trailer_child, LAYOUT_POS["052"]["qtde_registros"], qtd_registros)
            trailer_child = _write_number(trailer_child, LAYOUT_POS["052"]["pv_solicitante"], int(pv))
            trailer_child = _write_number(trailer_child, LAYOUT_POS["052"]["qtd_cred_norm"], qtd_cred_norm)
            trailer_child = _write_money(trailer_child, LAYOUT_POS["052"]["valor_rv"], valor_cred_norm)
            trailer_child = _write_number(trailer_child, LAYOUT_POS["052"]["qtd_ant"], qtd_ant)
            trailer_child = _write_money(trailer_child, LAYOUT_POS["052"]["valor_ant"], valor_ant)
            trailer_child = _write_number(trailer_child, LAYOUT_POS["052"]["qtd_aj_cred"], qtd_aj_cred)
            trailer_child = _write_money(trailer_child, LAYOUT_POS["052"]["valor_aj_cred"], valor_aj_cred)
            trailer_child = _write_number(trailer_child, LAYOUT_POS["052"]["qtd_aj_deb"], qtd_aj_deb)
            trailer_child = _write_money(trailer_child, LAYOUT_POS["052"]["valor_aj_deb"], valor_aj_deb)

            # === Montagem do arquivo filho ===
            child_name = f"{pv}_{data_emissao}_{nsa}_EEFI.txt"
            child_path = out_dir / child_name
            with child_path.open("w", encoding="utf-8") as f:
                f.write(hdr_child + "\n")
                for ln in registros:
                    f.write(ln + "\n")
                f.write(trailer_child + "\n")

            arquivos_gerados.append(child_path)
            logger.info(f"🧾 Filho gerado: {child_name} | PV={pv} | Total={total_pv}")

        # === Validação global (se houver trailer no MÃE) ===
        ok = True
        if tem_052:
            ok = (soma_filhos == total_mae_052)
            if ok:
                logger.info(f"✅ Validação OK | soma_filhos={soma_filhos} == trailer_mae={total_mae_052}")
            else:
                logger.error(f"❌ Validação FALHOU | soma_filhos={soma_filhos} != trailer_mae={total_mae_052}")

        logger.info(f"📋 PVs: {', '.join(pv_logs)}")

        return {
            "nsa": nsa,
            "output_dir": str(out_dir),
            "files_generated": [str(p) for p in arquivos_gerados],
            "report_csv": "",
            "sum_pvs": soma_filhos,
            "total_052": total_mae_052,
            "ok": ok,
            "message": f"EEFI processado ({'032' if tem_032 else '040'})",
        }

    except Exception as e:
        logger.exception(f"❌ Erro EEFI: {e}")
        return {
            "nsa": "000000",
            "output_dir": "",
            "files_generated": [],
            "report_csv": "",
            "sum_pvs": 0,
            "total_052": 0,
            "ok": False,
            "message": f"Erro: {str(e)}",
        }
