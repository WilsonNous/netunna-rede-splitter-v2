# =============================================================
# modules/eefi_processor.py
# Extrato Financeiro – EEFI (REDE / NETUNNA)
# v5.4 | Netunna Splitter Framework
# =============================================================

from pathlib import Path
from typing import Dict, List, Tuple
import logging

logger = logging.getLogger("splitter")
logger.setLevel(logging.INFO)

# =============================================================
# 📋 Layout Posições (EEFI v4.00 – 05/2023)
# =============================================================
LAYOUT_POS = {
    "030": {"tipo": (0, 3), "data_emissao": (3, 11), "sequencia": (75, 81)},
    "032": {"tipo": (0, 3), "pv": (3, 12)},
    "034": {"tipo": (0, 3), "valor": (31, 46)},  # Crédito
    "035": {"tipo": (0, 3), "valor": (29, 44)},  # Débito
    "036": {"tipo": (0, 3), "valor": (31, 46)},  # Antecipação
    "038": {"tipo": (0, 3), "valor": (31, 46)},  # Débito bancário
    "040": {"tipo": (0, 3), "pv": (3, 12), "valor": (12, 27)},  # Simplificado
    "043": {"tipo": (0, 3), "valor": (48, 63)},  # Ajuste crédito
    "052": {  # Trailer geral / filho
        "tipo": (0, 3),
        "qtde_matrizes": (3, 7),
        "qtde_registros": (7, 13),
        "pv": (13, 22),
        "qtde_cred_norm": (22, 26),
        "valor_rv": (26, 41),
        "qtde_ant": (41, 47),
        "valor_ant": (47, 62),
        "qtde_aj_cred": (62, 66),
        "valor_aj_cred": (66, 81),
        "qtde_aj_deb": (81, 85),
        "valor_aj_deb": (85, 100),
    },
}

# =============================================================
# ⚙️ Utilitários
# =============================================================
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
        logger.info(f"🟢 Iniciando processamento EEFI híbrido | arquivo={file_path}")
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

        modo = "completo" if tem_032 else "simplificado" if tem_040 else "desconhecido"
        logger.info(f"🧩 Layout detectado: {modo.upper()}")

        # Trailer do arquivo mãe (052)
        total_052 = 0
        if tem_052:
            trailer_052_line = next(ln for ln in reversed(lines) if ln.startswith("052"))
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
        if modo == "completo":
            current_pv = None
            for ln in lines:
                tipo = ln[0:3]
                if tipo == "032":
                    current_pv = _slice(ln, LAYOUT_POS["032"]["pv"]).strip()
                    pv_map.setdefault(current_pv, []).append(ln)
                elif tipo in ("034", "035", "036", "038", "043"):
                    if current_pv:
                        pv_map[current_pv].append(ln)
        elif modo == "simplificado":
            for ln in lines:
                if ln.startswith("040"):
                    pv = _slice(ln, LAYOUT_POS["040"]["pv"]).strip()
                    pv_map.setdefault(pv, []).append(ln)
        else:
            raise ValueError("Layout EEFI não reconhecido (faltam 032 ou 040).")

        out_dir = Path(output_root) / f"NSA_{nsa}"
        out_dir.mkdir(parents=True, exist_ok=True)

        arquivos_gerados = []
        soma_total_arquivos = 0
        pv_totais = []

        # -----------------------------------------------------
        # Processar PVs e gerar filhos
        # -----------------------------------------------------
        for pv, registros in pv_map.items():
            qtd_registros = 2 + len(registros)  # header + registros + trailer
            qtd_cred_norm = qtd_ant = qtd_aj_cred = qtd_aj_deb = 0
            valor_cred_norm = valor_ant = valor_aj_cred = valor_aj_deb = 0

            for ln in registros:
                tipo = ln[0:3]
                if tipo in ("034", "036", "043", "035", "038", "040"):
                    valor = _to_int_cents(_slice(ln, LAYOUT_POS[tipo]["valor"]))
                    if valor == 0 and tipo == "040":
                        logger.warning(f"⚠️ Valor líquido zerado no registro {tipo} PV={pv} — mantendo total 0.")
                    if tipo in ("034", "040"):
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
            soma_total_arquivos += total_pv
            pv_totais.append((pv, total_pv))

            # Trailer 052 recalculado e fiel ao layout oficial
            trailer_052 = (
                f"052"
                f"{1:0>4}"                                # qtde_matrizes
                f"{qtd_registros:0>6}"                    # qtde_registros
                f"{int(pv):0>9}"                          # PV
                f"{qtd_cred_norm:0>4}"                    # qtd créditos normais
                f"{valor_cred_norm:0>15}"                 # valor créditos normais
                f"{qtd_ant:0>6}"                          # qtd antecipações
                f"{valor_ant:0>15}"                       # valor antecipações
                f"{qtd_aj_cred:0>4}"                      # qtd ajustes crédito
                f"{valor_aj_cred:0>15}"                   # valor ajustes crédito
                f"{qtd_aj_deb:0>4}"                       # qtd ajustes débito
                f"{valor_aj_deb:0>15}"                    # valor ajustes débito
            ).ljust(400)

            # Gera o arquivo filho
            child_name = f"{pv}_{data_emissao}_{nsa}_EEFI.txt"
            child_path = out_dir / child_name
            with child_path.open("w", encoding="utf-8") as f:
                f.write(header_030 + "\n")
                for ln in registros:
                    f.write(ln + "\n")
                f.write(trailer_052 + "\n")

            arquivos_gerados.append(child_path)
            logger.info(f"🧾 Gerado: {child_name} | Total={total_pv}")

        # -----------------------------------------------------
        # Validação final
        # -----------------------------------------------------
        ok = True
        if tem_052:
            ok = soma_total_arquivos == total_052
            if ok:
                logger.info(f"✅ Validação OK | Soma filhos={soma_total_arquivos} == Trailer 052={total_052}")
            else:
                logger.error(f"❌ Validação FALHOU | Soma filhos={soma_total_arquivos} != Trailer 052={total_052}")
        else:
            logger.info("⚙️ Arquivo mãe sem trailer 052 — validação global não aplicada.")

        # Log dos PVs processados
        pv_listagem = ", ".join([f"{pv}={valor}" for pv, valor in pv_totais])
        logger.info(f"📋 PVs processados: {pv_listagem}")

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
