import os
import re
from collections import defaultdict, Counter
from utils.file_utils import ensure_outfile
from utils.validation_utils import validar_totais, to_centavos

def limpar_diretorio(dir_path: str):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        return
    for fname in os.listdir(dir_path):
        fpath = os.path.join(dir_path, fname)
        if os.path.isfile(fpath):
            os.remove(fpath)
    print(f"🧹 Diretório '{dir_path}' limpo antes do novo processamento.")

def _extrair_data_nsa(header_parts: list[str], nome_arquivo: str):
    # 00, <matriz/grupo>, <emissão>, <movimento>, ..., <sequência>, ...
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

def _pad(n: int, width: int) -> str:
    return str(n).zfill(width)

def _val(n: int) -> str:
    # campos 9(15)V99 são enviados sem separador decimal (centavos já no inteiro)
    return str(n).zfill(15)

def process_eevd(input_path: str, output_dir: str, error_dir: str = "erro"):
    """EEVD (Débito) — Split por PV + trailers 02/03/04 recalculados + validação do 04 (mãe)."""
    print("🟢 Processando EEVD (Vendas Débito)")
    filename = os.path.basename(input_path)

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f if l.strip()]

    if len(lines) < 2:
        raise ValueError("Arquivo EEVD vazio ou sem trailer.")

    header_line = lines[0]
    header_parts = [p.strip() for p in header_line.split(",")]

    # O trailer do arquivo é o ÚLTIMO registro tipo 04
    # (há arquivos com 02/03 no meio)
    last_04_idx = max(i for i, l in enumerate(lines) if l.split(",")[0] == "04")
    trailer_line = lines[last_04_idx]
    trailer_parts = [p.strip() for p in trailer_line.split(",")]

    detalhes = lines[1:last_04_idx]  # tudo entre 00 e o 04 final

    data_ref, nsa = _extrair_data_nsa(header_parts, filename)

    # Subpasta por NSA
    lote_dir = os.path.join(output_dir, f"NSA_{nsa}")
    os.makedirs(lote_dir, exist_ok=True)
    print(f"📂 Criado diretório de saída: {lote_dir}")

    # Estruturas
    # - manter ordem: percorremos 'detalhes' na ordem do arquivo
    registros_por_pv = defaultdict(list)
    stats_pv = defaultdict(lambda: {
        "qtd_rv": 0,
        "qtd_cv": 0,
        "bruto": 0,
        "desconto": 0,
        "liquido": 0,
        "bruto_pred": 0,
        "desc_pred": 0,
        "liq_pred": 0
    })
    contagem_tipos = Counter()
    rv_to_pv = {}  # mapeia RV -> PV (para amarrar 05/13/20 quando preciso)

    # Campos por tipo segundo layout (csv por vírgula)
    # 01: [0]=tipo, [1]=PV, [2]=data_cred, [3]=data_rv, [4]=RV, [5]=qtd_cv, [6]=bruto, [7]=desc, [8]=liq, [9]=tipo(D/P) ...
    # 05: [1]=PV, [2]=RV ...
    # 13: [1]=PV, [2]=RV ...
    # 20: [2]=matriz/grupo, [3]=RV (!!) -> vamos mapear RV -> PV
    # 08,09,11,17,18,19 têm PV em colunas específicas (abaixo tratadas)

    for raw in detalhes:
        parts = [p.strip() for p in raw.split(",")]
        if not parts:
            continue
        t = parts[0]
        contagem_tipos[t] += 1

        # Ignorar trailers intermediários 02/03/04 originais ao remontar os filhos
        if t in {"02", "03", "04"}:
            continue

        # Identificar PV do registro
        pv = None
        if t == "01":
            if len(parts) > 4:
                rv_to_pv[parts[4]] = parts[1]  # RV -> PV
            pv = parts[1]

            # Acumular totais a partir do 01 (fonte da verdade para 04)
            qtd_cv = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0
            bruto = to_centavos(parts[6]) if len(parts) > 6 else 0
            desconto = to_centavos(parts[7]) if len(parts) > 7 else 0
            liquido = to_centavos(parts[8]) if len(parts) > 8 else 0
            tipo_resumo = (parts[9] if len(parts) > 9 else "").upper()

            s = stats_pv[pv]
            s["qtd_rv"] += 1
            s["qtd_cv"] += qtd_cv
            s["bruto"] += bruto
            s["desconto"] += desconto
            s["liquido"] += liquido
            if tipo_resumo == "P":  # pré-datado
                s["bruto_pred"] += bruto
                s["desc_pred"] += desconto
                s["liq_pred"] += liquido

            registros_por_pv[pv].append(parts)
            continue

        if t in {"05", "13"}:
            pv = parts[1] if len(parts) > 1 else None
            if pv:
                stats_pv[pv]["qtd_cv"] += 1
                registros_por_pv[pv].append(parts)
            continue

        if t == "20":
            # 20 usa matriz/grupo em [1] e RV em [2] (pelo layout),
            # no PDF, a tabela mostra [2]=matriz e [3]=RV. Vamos suportar ambos cenários:
            rv = None
            if len(parts) > 3 and parts[3].isdigit():
                rv = parts[3]
            elif len(parts) > 2 and parts[2].isdigit():
                rv = parts[2]
            if rv and rv in rv_to_pv:
                pv = rv_to_pv[rv]
                stats_pv[pv]["qtd_cv"] += 1
                registros_por_pv[pv].append(parts)
            continue

        if t == "08":  # Desagendamento
            pv = parts[1] if len(parts) > 1 else None
        elif t == "09":  # Pré-datadas liquidadas
            pv = parts[1] if len(parts) > 1 else None
        elif t == "11":  # Ajustes Net
            pv = parts[2] if len(parts) > 2 else None
        elif t == "17":  # Ajustes Net (E-comm)
            pv = parts[5] if len(parts) > 5 else None
        elif t == "18":  # Negociadas/Liquidadas
            pv = parts[2] if len(parts) > 2 else None
        elif t == "19":  # IC+
            pv = parts[2] if len(parts) > 2 else None

        if pv:
            registros_por_pv[pv].append(parts)

    # Cenário sem movimento (nenhum 01)
    if not stats_pv:
        print("ℹ️ Arquivo sem movimento (apenas 00 e 04). Gerando relatório simples…")
        nome_arquivo = f"SEM_MOV_{data_ref}_{nsa}_EEVD.txt"
        out_path = ensure_outfile(lote_dir, nome_arquivo)
        zeros15 = "0".zfill(15)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(",".join(header_parts) + "\n")
            # 04: total arquivo zerado + total de registros (2: header+trailer)
            trailer_sem = [
                "04",
                header_parts[1] if len(header_parts) > 1 else "0",
                _pad(0, 6),
                _pad(0, 6),
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
            "detalhe": "Sem movimento: 0 = 0"
        }

    # Totais do arquivo-mãe (04)
    total_bruto_trailer = to_centavos(trailer_parts[4] if len(trailer_parts) > 4 else "0")
    total_desc_trailer  = to_centavos(trailer_parts[5] if len(trailer_parts) > 5 else "0")
    total_liq_trailer   = to_centavos(trailer_parts[6] if len(trailer_parts) > 6 else "0")

    # Somatórios processados (a partir de 01)
    soma_bruto_total   = sum(s["bruto"]   for s in stats_pv.values())
    soma_desc_total    = sum(s["desconto"] for s in stats_pv.values())
    soma_liq_total     = sum(s["liquido"]  for s in stats_pv.values())

    # Validações (3 vias)
    det_bruto = validar_totais(total_bruto_trailer, soma_bruto_total)
    det_desc  = validar_totais(total_desc_trailer,  soma_desc_total)
    det_liq   = validar_totais(total_liq_trailer,   soma_liq_total)
    status_geral = "OK" if (total_bruto_trailer == soma_bruto_total
                            and total_desc_trailer == soma_desc_total
                            and total_liq_trailer == soma_liq_total) else "ERRO"

    print(f"✅ Bruto: trailer {total_bruto_trailer} | proc {soma_bruto_total} | {'OK' if total_bruto_trailer==soma_bruto_total else 'ERRO'}")
    print(f"✅ Desc : trailer {total_desc_trailer}  | proc {soma_desc_total}  | {'OK' if total_desc_trailer==soma_desc_total else 'ERRO'}")
    print(f"✅ Líq  : trailer {total_liq_trailer}   | proc {soma_liq_total}   | {'OK' if total_liq_trailer==soma_liq_total else 'ERRO'}")

    # Geração dos filhos por PV
    gerados = []
    matriz_ou_grupo = header_parts[1] if len(header_parts) > 1 else "0"

    for pv, registros in registros_por_pv.items():
        s = stats_pv[pv]
        # Reescrever 00 ajustando a 2ª coluna para PV (mantendo teu comportamento atual)
        header_parts_pv = header_parts.copy()
        if len(header_parts_pv) > 1:
            header_parts_pv[1] = pv
        header_line_pv = ",".join(header_parts_pv)

        linhas_out = [header_line_pv]

        # Mantém ordem dos registros daquele PV
        for p in registros:
            linhas_out.append(",".join(p))

        # 02 — Total do ponto de venda
        reg02 = [
            "02",
            matriz_ou_grupo,                        # Nº filiação matriz/grupo (layout)
            _pad(s["qtd_rv"], 3),                  # qtd RV acatados
            _pad(s["qtd_cv"], 6),                  # qtd comprovantes
            _val(s["bruto"]),
            _val(s["desconto"]),
            _val(s["liquido"]),
            _val(s["bruto_pred"]),
            _val(s["desc_pred"]),
            _val(s["liq_pred"])
        ]
        linhas_out.append(",".join(reg02))

        # 03 — Total da matriz (no filho = mesmo total do PV)
        reg03 = reg02.copy()
        reg03[0] = "03"
        linhas_out.append(",".join(reg03))

        # 04 — Total do arquivo (no filho = totals do PV) + total de REGISTROS
        total_registros = len(linhas_out) + 1  # +1 do próprio 04
        reg04 = [
            "04",
            matriz_ou_grupo,
            _pad(s["qtd_rv"], 6),                  # qtd RV
            _pad(s["qtd_cv"], 6),                  # qtd CV
            _val(s["bruto"]),
            _val(s["desconto"]),
            _val(s["liquido"]),
            _val(s["bruto_pred"]),
            _val(s["desc_pred"]),
            _val(s["liq_pred"]),
            _pad(total_registros + 1, 6)           # somando header+...+04 (linha atual)
        ]
        linhas_out.append(",".join(reg04))

        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVD.txt"
        out_path = ensure_outfile(lote_dir, nome_arquivo)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(linhas_out) + "\n")

        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)} → {lote_dir}")

    return {
        "arquivo": filename,
        "data_ref": data_ref,
        "nsa": nsa,
        "output_dir": lote_dir,
        "filhos": gerados,
        "totais_trailer_mae": {
            "bruto": total_bruto_trailer, "desconto": total_desc_trailer, "liquido": total_liq_trailer
        },
        "totais_processados": {
            "bruto": soma_bruto_total, "desconto": soma_desc_total, "liquido": soma_liq_total
        },
        "status": status_geral,
        "detalhe": {
            "bruto": det_bruto, "desconto": det_desc, "liquido": det_liq
        },
        "contagem_registros": dict(contagem_tipos)
    }
