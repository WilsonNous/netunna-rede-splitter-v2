# =============================================================
# modules/eefi_processor.py
# Extrato Financeiro – EEFI (REDE / NETUNNA)
# v5.6 | Netunna Splitter Framework
# =============================================================

from pathlib import Path
from typing import Dict, List, Tuple, Optional
import re
import logging

logger = logging.getLogger("splitter")
logger.setLevel(logging.INFO)

# -------------------------------------------------------------
# Layout Posições (índices 0-based p/ slicing)
# -------------------------------------------------------------
LAYOUT_POS = {
    "030": {  # Header
        "tipo": (0, 3),
        "data_emissao": (3, 11),          # DDMMAAAA
        "sequencia": (78, 81),            # NSA (6)
        "pv_grupo_matriz": (81, 90),      # PV grupo/matriz (9) -> sobrescrevemos com PV do filho
    },

    # 032 – Header Matriz / PV (quando existir)
    "032": {"tipo": (0, 3), "pv": (3, 12)},

    # Registros financeiros (somam no trailer do filho)
    "034": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46)},  # Crédito normal (C)
    "036": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46)},  # Antecipação (C)
    "043": {"tipo": (0, 3), "pv": (3, 12), "valor": (48, 63)},  # Ajuste a crédito (C)
    # 035 é informativo/detalhe de ocorrência. Neste layout não compõe o trailer 052.
    "035": {"tipo": (0, 3), "pv": (3, 12), "valor": (29, 44)},
    "038": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46)},  # Ajuste a débito

    # 040 – Serasa (NÃO soma financeiramente; mas precisa do PV para roteamento do filho)
    "040": {"tipo": (0, 3), "pv": (3, 12), "valor": (12, 27)},  # valor pode vir 0; ignoramos na soma

    # 045 – Cancelamento (entra como DÉBITO); o PV pode variar de posição em alguns arquivos
    # vamos tentar várias faixas conhecidas; a "principal" abaixo:
    # 045 é detalhe complementar/cancelamento e não compõe diretamente o trailer 052.
    # O valor observado no layout atual fica em 31:46, mas é mantido apenas para auditoria.
    "045": {"tipo": (0, 3), "pv": (3, 12), "valor": (31, 46)},

    # 052 – Trailer (mãe e filhos)
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
    num = "".join(ch for ch in num_txt if ch.isdigit())
    return int(num or "0")

def _write_fixed(line: str, start: int, end: int, value: str) -> str:
    width = end - start
    s = value[:width].ljust(width)
    return line[:start] + s + line[end:]

def _write_number(line: str, rng: Tuple[int, int], value: int) -> str:
    width = rng[1] - rng[0]
    s = f"{value:0>{width}}"
    return line[:rng[0]] + s + line[rng[1]:]

def _write_money(line: str, rng: Tuple[int, int], value_cents: int) -> str:
    width = rng[1] - rng[0]
    s = f"{value_cents:0>{width}}"
    return line[:rng[0]] + s + line[rng[1]:]

def _extract_pv(line: str, tipo: str) -> Optional[str]:
    """
    Tenta extrair o PV pelo layout do tipo; se falhar, tenta alternativas;
    por fim, tenta o primeiro bloco de 9 dígitos nos 40 primeiros chars.
    """
    # 1) Pela faixa padrão do tipo, se existir
    if tipo in LAYOUT_POS and "pv" in LAYOUT_POS[tipo]:
        pv = _slice(line, LAYOUT_POS[tipo]["pv"]).strip()
        if pv.isdigit() and len(pv) == 9:
            return pv

    # 2) Alternativas específicas (variações vistas em campo)
    #   - alguns 045 trazem PV em 13–22 ou 22–31
    alt_ranges = []
    if tipo == "045":
        alt_ranges = [(12, 21), (13, 22), (22, 31), (3, 12)]
    elif tipo in ("034", "035", "036", "038", "043"):
        # já tentamos (3,12); mas em algumas bases pode vir deslocado
        alt_ranges = [(13, 22), (22, 31)]
    elif tipo == "040":
        # normalmente (3,12) resolve; tentativas extra por segurança
        alt_ranges = [(13, 22), (22, 31)]

    for a, b in alt_ranges:
        seg = line[a:b].strip()
        if seg.isdigit() and len(seg) == 9:
            return seg

    # 3) Fallback: pega o primeiro bloco de 9 dígitos perto do início
    m = re.search(r"(\d{9})", line[:60])
    if m:
        return m.group(1)

    return None

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

        # -----------------------------------------------------
        # Agrupamento por PV (robusto por linha)
        # -----------------------------------------------------
        pv_map: Dict[str, List[str]] = {}

        for ln in lines:
            tipo = ln[:3]

            # Header 030 e trailer 052 do MÃE não entram nos filhos
            if tipo in ("030", "052"):
                continue

            if modo == "completo" and tipo == "032":
                pv = _extract_pv(ln, "032")
                if pv:
                    pv_map.setdefault(pv, []).append(ln)
                continue

            # todos os tipos de detalhe (040, 045, 034, 035, 036, 038, 043)
            if tipo in ("040", "045", "034", "035", "036", "038", "043"):
                pv = _extract_pv(ln, tipo)
                if not pv:
                    logger.warning(f"⚠️ Não consegui identificar PV no registro {tipo}: {ln[:60]}...")
                    continue
                pv_map.setdefault(pv, []).append(ln)

        # -----------------------------------------------------
        # Geração dos filhos (030 + registros PV + 052)
        # -----------------------------------------------------
        out_dir = Path(output_root) / f"NSA_{nsa}"
        out_dir.mkdir(parents=True, exist_ok=True)

        arquivos_gerados: List[Path] = []
        soma_filhos = 0
        pv_logs = []

        for pv, registros in pv_map.items():
            # Totais por PV (somente financeiros)
            qtd_cred_norm = qtd_ant = qtd_aj_cred = qtd_aj_deb = 0
            valor_cred_norm = valor_ant = valor_aj_cred = valor_aj_deb = 0

            for ln in registros:
                tipo = ln[:3]
                if tipo in ("034", "036", "043", "035", "038", "045"):
                    # extrai valor conforme tipo; se não houver "valor" no layout, ignora
                    if "valor" in LAYOUT_POS.get(tipo, {}):
                        valor = _to_int_cents(_slice(ln, LAYOUT_POS[tipo]["valor"]))
                    else:
                        valor = 0
                    if tipo in ("034",):
                        qtd_cred_norm += 1
                        valor_cred_norm += valor
                    elif tipo == "036":
                        qtd_ant += 1
                        valor_ant += valor
                    elif tipo == "043":
                        qtd_aj_cred += 1
                        valor_aj_cred += valor
                    elif tipo == "038":
                        qtd_aj_deb += 1
                        valor_aj_deb += valor
                    elif tipo in ("035", "045"):
                        # Registros informativos/complementares: permanecem no filho,
                        # mas não entram no fechamento financeiro do trailer 052.
                        pass
                elif tipo == "040":
                    # Serasa: incluímos no arquivo do PV, mas NÃO somamos
                    pass

            total_pv = valor_cred_norm + valor_ant + valor_aj_cred - valor_aj_deb
            soma_filhos += total_pv
            pv_logs.append(f"{pv}={total_pv}")

            # Header 030 do filho com PV sobrescrito em 082–090
            hdr_child = _write_fixed(
                header_030,
                LAYOUT_POS["030"]["pv_grupo_matriz"][0],
                LAYOUT_POS["030"]["pv_grupo_matriz"][1],
                f"{int(pv):0>9}",
            )

            # Trailer 052 do filho
            qtd_registros = 2 + len(registros)  # 030 + registros + 052
            trailer_child = (
                f"052"
                f"{1:0>4}"                                # qtde_matrizes
                f"{qtd_registros:0>6}"                    # qtde_registros
                f"{int(pv):0>9}"                          # pv solicitante
                f"{qtd_cred_norm:0>4}"                    # qtd créditos normais
                f"{valor_cred_norm:0>15}"                 # valor créditos normais
                f"{qtd_ant:0>6}"                          # qtd antecipações
                f"{valor_ant:0>15}"                       # valor antecipações
                f"{qtd_aj_cred:0>4}"                      # qtd ajustes crédito
                f"{valor_aj_cred:0>15}"                   # valor ajustes crédito
                f"{qtd_aj_deb:0>4}"                       # qtd ajustes débito
                f"{valor_aj_deb:0>15}"                    # valor ajustes débito
            ).ljust(400)

            # Arquivo filho
            child_name = f"{pv}_{data_emissao}_{nsa}_EEFI.txt"
            child_path = out_dir / child_name
            with child_path.open("w", encoding="utf-8") as f:
                f.write(hdr_child + "\n")
                for ln in registros:
                    f.write(ln + "\n")
                f.write(trailer_child + "\n")

            arquivos_gerados.append(child_path)
            logger.info(f"🧾 Filho gerado: {child_name} | PV={pv} | Total={total_pv}")

        # Validação global (se houver trailer 052 no MÃE)
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
            "message": f"EEFI processado (modo={'032' if tem_032 else '040/045'})",
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
