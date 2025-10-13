import os
import re
from collections import defaultdict, Counter
from utils.file_utils import ensure_outfile
from utils.validation_utils import validar_totais, to_centavos

# -------------------------------------------------------------
# Funções auxiliares
# -------------------------------------------------------------
def _pad(n: int, width: int) -> str:
    return str(n).zfill(width)

def _val(n: int) -> str:
    # Layout REDE usa 9(15)V99 sem sinal
    return str(abs(int(n))).zfill(15)

def _extrair_data_nsa(header_parts: list[str], nome_arquivo: str):
    """Extrai data (DDMMAA) e NSA do header EEVD"""
    data_ref = "000000"
    nsa = "000"
    if len(header_parts) > 3:
        campo_data_mov = header_parts[3].strip()
        if re.fullmatch(r"\d{8}", campo_data_mov):
            data_ref = f"{campo_data_mov[:2]}{campo_data_mov[2:4]}{campo_data_mov[4:6]}"
    if len(header_parts) > 7:
        campo_nsa = header_parts[7].strip()
        if campo_nsa.isdigit():
            nsa = campo_nsa[-3:].zfill(3)
    if data_ref == "000000":
        m = re.search(r"(\d{6,8})", nome_arquivo)
        if m:
            data_ref = m.group(1)[-6:]
    if nsa == "000":
        m = re.search(r"(\d{3})\D*\.[0-9]+$", nome_arquivo)
        if m:
            nsa = m.group(1)
    print(f"🧠 Data extraída: {data_ref} | NSA extraído: {nsa} | Origem: {os.path.basename(nome_arquivo)}")
    return data_ref, nsa

# -------------------------------------------------------------
# Função principal
# -------------------------------------------------------------
def process_eevd(input_path: str, output_dir: str, error_dir: str = "erro"):
    """
    EEVD Splitter — v4.4
      • 01  RV Originais
      • 011 Cancelamentos
      • 05  CV detalhados
      • 13  CV e-commerce
      • 20  CV Recarga (via RV→PV)
      • 08  Desagendamento
      • 09  Pré-datadas liquidadas
      • 11/17 Ajustes Net
      • 18 Negociadas / 19 IC+
    """

    print("🟢 Processando EEVD (Vendas Débito)")
    filename = os.path.basename(input_path)

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f if l.strip()]

    if len(lines) < 2:
        raise ValueError("Arquivo EEVD vazio ou sem trailer.")

    header_line = lines[0]
    header_parts = [p.strip() for p in header_line.split(",")]

    # Trailer mãe (último 04)
    last_04_idx = max(i for i, l in enumerate(lines) if l.split(",")[0] == "04") if any("04" in l[:3] for l in lines) else len(lines)-1
    trailer_line = lines[last_04_idx]
    trailer_parts = [p.strip() for p in trailer_line.split(",")]
    detalhes = lines[1:last_04_idx]

    data_ref, nsa = _extrair_data_nsa(header_parts, filename)
    lote_dir = os.path.join(output_dir, f"NSA_{nsa}")
    os.makedirs(lote_dir, exist_ok=True)
    print(f"📂 Criado diretório de saída: {lote_dir}")

    registros_por_pv = defaultdict(list)
    stats_pv = defaultdict(lambda: {
        "qtd_rv": 0, "qtd_cv": 0,
        "bruto": 0, "desconto": 0, "liquido": 0,
        "bruto_pred": 0, "desc_pred": 0, "liq_pred": 0
    })
    contagem_tipos = Counter()
    rv_to_pv = {}

    # -------------------------------------------------------------
    # Parsing de detalhes por tipo
    # -------------------------------------------------------------
    for raw in detalhes:
        parts = [p.strip() for p in raw.split(",")]
        if not parts:
            continue
        t = parts[0]
        contagem_tipos[t] += 1
        pv = None

        if t == "01":  # Resumo de vendas
            pv = parts[1] if len(parts) > 1 else None
            if len(parts) > 4:
                rv_to_pv[parts[4]] = pv
            qtd_cv = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0
            bruto = to_centavos(parts[6]) if len(parts) > 6 else 0
            desconto = to_centavos(parts[7]) if len(parts) > 7 else 0
            liquido = to_centavos(parts[8]) if len(parts) > 8 else 0
            tipo = (parts[9] if len(parts) > 9 else "").upper()

            if pv:
                s = stats_pv[pv]
                s["qtd_rv"] += 1
                s["qtd_cv"] += qtd_cv
                s["bruto"] += bruto
                s["desconto"] += desconto
                s["liquido"] += liquido
                if tipo == "P":
                    s["bruto_pred"] += bruto
                    s["desc_pred"] += desconto
                    s["liq_pred"] += liquido
                registros_por_pv[pv].append(parts)
            continue

        if t == "011":  # Cancelamento de venda
            pv = parts[1] if len(parts) > 1 else None
            if pv:
                registros_por_pv[pv].append(parts)
                s = stats_pv[pv]
                s["qtd_cv"] += 1
                # ⚠️ Cancelamento não soma nos totais financeiros
                # Mantém apenas para fins de relatório / geração do arquivo filho
            continue

        if t in {"05", "13"}:
            pv = parts[1] if len(parts) > 1 else None
            if pv:
                registros_por_pv[pv].append(parts)
                stats_pv[pv]["qtd_cv"] += 1
            continue

        if t == "20":
            rv = parts[3] if len(parts) > 3 else (parts[2] if len(parts) > 2 else None)
            if rv and rv in rv_to_pv:
                pv = rv_to_pv[rv]
                registros_por_pv[pv].append(parts)
                stats_pv[pv]["qtd_cv"] += 1
            elif len(registros_por_pv) == 1:
                pv_unico = next(iter(registros_por_pv.keys()))
                registros_por_pv[pv_unico].append(parts)
                stats_pv[pv_unico]["qtd_cv"] += 1
            continue

        if t == "08":
            pv = parts[1] if len(parts) > 1 else None
        elif t == "09":
            pv = parts[1] if len(parts) > 1 else None
        elif t == "11":
            pv = parts[2] if len(parts) > 2 else None
        elif t == "17":
            pv = parts[5] if len(parts) > 5 else None
        elif t == "18":
            pv = parts[2] if len(parts) > 2 else None
        elif t == "19":
            pv = parts[2] if len(parts) > 2 else None

        if pv:
            registros_por_pv[pv].append(parts)

    # -------------------------------------------------------------
    # Verificação de movimento
    # -------------------------------------------------------------
    TIPOS_MOVIMENTO = {"01", "011", "05", "13", "20", "08", "09", "11", "17", "18", "19"}
    tem_movimento = any(contagem_tipos[t] > 0 for t in TIPOS_MOVIMENTO)

    if not tem_movimento:
        print("ℹ️ Arquivo sem movimento real (00 + 04). Gerando arquivo vazio.")
        nome_arquivo = f"SEM_MOV_{data_ref}_{nsa}_EEVD.txt"
        out_path = ensure_outfile(lote_dir, nome_arquivo)
        zeros15 = "0".zfill(15)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(",".join(header_parts) + "\n")
            trailer_sem = [
                "04",
                header_parts[1] if len(header_parts) > 1 else "0",
                _pad(0, 6), _pad(0, 6),
                zeros15, zeros15, zeros15,
                zeros15, zeros15, zeros15,
                _pad(2, 6)
            ]
            f.write(",".join(trailer_sem) + "\n")
        return {
            "arquivo": filename,
            "data_ref": data_ref,
            "nsa": nsa,
            "output_dir": lote_dir,
            "total_trailer": 0,
            "total_processado": 0,
            "status": "OK",
            "detalhe": "Sem movimento real"
        }

    # -------------------------------------------------------------
    # Validação de totais (com trailer-mãe 04)
    # -------------------------------------------------------------
    total_bruto_trailer = to_centavos(trailer_parts[4] if len(trailer_parts) > 4 else "0")
    total_desc_trailer  = to_centavos(trailer_parts[5] if len(trailer_parts) > 5 else "0")
    total_liq_trailer   = to_centavos(trailer_parts[6] if len(trailer_parts) > 6 else "0")

    soma_bruto_total = sum(s["bruto"] for s in stats_pv.values())
    soma_desc_total  = sum(s["desconto"] for s in stats_pv.values())
    soma_liq_total   = sum(s["liquido"] for s in stats_pv.values())

    det_bruto = validar_totais(total_bruto_trailer, soma_bruto_total)
    det_desc  = validar_totais(total_desc_trailer,  soma_desc_total)
    det_liq   = validar_totais(total_liq_trailer,   soma_liq_total)

    status_geral = "OK" if (
        total_bruto_trailer == soma_bruto_total and
        total_desc_trailer  == soma_desc_total  and
        total_liq_trailer   == soma_liq_total
    ) else "ERRO"

    print(f"✅ Bruto: trailer {total_bruto_trailer} | proc {soma_bruto_total} | {'OK' if total_bruto_trailer==soma_bruto_total else 'ERRO'}")
    print(f"✅ Desc : trailer {total_desc_trailer}  | proc {soma_desc_total}  | {'OK' if total_desc_trailer==soma_desc_total else 'ERRO'}")
    print(f"✅ Líq  : trailer {total_liq_trailer}   | proc {soma_liq_total}   | {'OK' if total_liq_trailer==soma_liq_total else 'ERRO'}")

    # -------------------------------------------------------------
    # Geração dos filhos por PV (matriz só se tiver movimento)
    # -------------------------------------------------------------
    gerados = []
    matriz_ou_grupo = header_parts[1] if len(header_parts) > 1 else "0"

    for pv, regs in registros_por_pv.items():
        if not regs:
            continue

        # Matriz só gera arquivo se houver movimento próprio
        if pv == matriz_ou_grupo:
            has_mov_matriz = any(r[0] in TIPOS_MOVIMENTO for r in regs)
            if not has_mov_matriz:
                print(f"⚪ Ignorado PV matriz/grupo {pv} (sem movimento próprio)")
                continue

        s = stats_pv[pv]
        bruto = int(s["bruto"])
        desc  = int(s["desconto"])
        liq   = int(s["liquido"])
        bruto_pred = int(s["bruto_pred"])
        desc_pred  = int(s["desc_pred"])
        liq_pred   = int(s["liq_pred"])
        qtd_rv = int(s["qtd_rv"])
        qtd_cv = int(s["qtd_cv"])

        header_pv = header_parts.copy()
        header_pv[1] = pv
        linhas_out = [",".join(header_pv)] + [",".join(p) for p in regs]

        trailer_id = matriz_ou_grupo

        reg02 = ["02", trailer_id, _pad(qtd_rv,3), _pad(qtd_cv,6),
                 _val(bruto), _val(desc), _val(liq),
                 _val(bruto_pred), _val(desc_pred), _val(liq_pred)]

        reg03 = reg02.copy(); reg03[0] = "03"

        total_registros = len(linhas_out) + 3  # 00 + detalhes + 02/03/04
        reg04 = ["04", trailer_id, _pad(qtd_rv,6), _pad(qtd_cv,6),
                 _val(bruto), _val(desc), _val(liq),
                 _val(bruto_pred), _val(desc_pred), _val(liq_pred),
                 _pad(total_registros,6)]

        linhas_out += [",".join(reg02), ",".join(reg03), ",".join(reg04)]

        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVD.txt"
        out_path = ensure_outfile(lote_dir, nome_arquivo)
        with open(out_path,"w",encoding="utf-8") as f:
            f.write("\n".join(linhas_out)+"\n")
        gerados.append(out_path)

        print(f"🧾 Gerado: {os.path.basename(out_path)} → {lote_dir}")
        print(f"   ↳ Totais PV {pv}: Bruto={bruto} | Desc={desc} | Líq={liq} | RV={qtd_rv} | CV={qtd_cv}")

    return {
        "arquivo": filename,
        "data_ref": data_ref,
        "nsa": nsa,
        "output_dir": lote_dir,
        "filhos": gerados,
        "totais_trailer_mae": {
            "bruto": total_bruto_trailer,
            "desconto": total_desc_trailer,
            "liquido": total_liq_trailer
        },
        "totais_processados": {
            "bruto": soma_bruto_total,
            "desconto": soma_desc_total,
            "liquido": soma_liq_total
        },
        "status": status_geral,
        "detalhe": {"bruto": det_bruto, "desconto": det_desc, "liquido": det_liq},
        "contagem_registros": dict(contagem_tipos)
    }
