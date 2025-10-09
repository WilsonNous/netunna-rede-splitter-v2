import os
import re
from collections import defaultdict
from utils.file_utils import ensure_outfile
from utils.validation_utils import validar_totais, to_centavos


def _ddmmaa_from_yyyymmdd8(d8: str) -> str:
    """
    Converte 'DDMMAAAA' do header (8 dígitos) para 'DDMMAA'.
    Ex.: '05102025' -> '051025'
    """
    d8 = (d8 or "").strip()
    if re.fullmatch(r"\d{8}", d8):
        dd = d8[0:2]
        mm = d8[2:4]
        aa = d8[6:8]
        return f"{dd}{mm}{aa}"
    return "000000"


def process_eevd(input_path: str, output_dir: str):
    """
    Processa arquivo EEVD (Vendas Débito) validando por VALORES em centavos.

    Layout relevante (linhas CSV):
      - Header (tipo '00'):
          [0]=00, [1]=PV-MATRIZ, [2]=DDMMAAAA, ... [7]=NSA ...
      - Detalhe (tipo '01'):
          [0]=01, [1]=PV, [2]=DDMMAAAA, [3]=..., [4]=NSU/controle (não é valor),
          [5]=..., [6]=BRUTO (centavos), [7]=DESCONTO (centavos), [8]=LIQUIDO (centavos), ...
      - Trailer (tipo '04'):
          [4]=TOTAL_BRUTO (centavos), [5]=TOTAL_DESCONTO (centavos), [6]=TOTAL_LIQUIDO (centavos)

    Nome do arquivo gerado: PV_DDMMAA_NSA_EEVD.txt
    """
    print("🟢 Processando EEVD (Vendas Débito)")

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f if l.strip()]

    if not lines:
        raise ValueError("Arquivo EEVD vazio.")

    header_line = lines[0]
    trailer_line = lines[-1]
    detalhes = lines[1:-1]

    header_parts = [p.strip() for p in header_line.split(",")]
    trailer_parts = [p.strip() for p in trailer_line.split(",")]

    # Data (DDMMAAA A -> DDMMAA) e NSA do arquivo-mãe
    data_ref = _ddmmaa_from_yyyymmdd8(header_parts[2] if len(header_parts) > 2 else "")
    nsa = (header_parts[7] if len(header_parts) > 7 else "000")[-3:].zfill(3)

    # Agrupar por PV e acumular valores corretos (6=bruto, 7=desconto, 8=liquido)
    grupos = defaultdict(list)
    totais_pv = defaultdict(lambda: {"bruto": 0, "desconto": 0, "liquido": 0})

    for line in detalhes:
        parts = [p.strip() for p in line.split(",")]
        if not parts or parts[0] != "01":
            continue
        # defensivo
        while len(parts) < 9:
            parts.append("")

        pv = parts[1]
        bruto = to_centavos(parts[6])      # CORRETO: valor bruto em centavos
        desconto = to_centavos(parts[7])   # CORRETO
        liquido = to_centavos(parts[8])    # CORRETO

        grupos[pv].append(parts)
        totais_pv[pv]["bruto"] += bruto
        totais_pv[pv]["desconto"] += desconto
        totais_pv[pv]["liquido"] += liquido

    # Gerar filhos por PV
    gerados = []
    soma_bruto_total = 0

    for pv, registros in grupos.items():
        bruto = totais_pv[pv]["bruto"]
        desconto = totais_pv[pv]["desconto"]
        liquido = totais_pv[pv]["liquido"]
        soma_bruto_total += bruto

        # Header do filho = header do mãe, mas com PV do filho
        header_parts_pv = header_parts.copy()
        if len(header_parts_pv) < 8:
            # garante índice até [7]
            header_parts_pv += [""] * (8 - len(header_parts_pv))

        header_parts_pv[1] = pv  # PV do filho
        header_line_pv = ",".join(header_parts_pv)

        # Trailer do filho = trailer do mãe, mas com PV e totais do filho
        trailer_parts_pv = trailer_parts.copy()
        while len(trailer_parts_pv) < 11:
            trailer_parts_pv.append("0")

        trailer_parts_pv[1] = pv
        # campos de contagem: usar a quantidade de detalhes do PV (mantém consistência mínima)
        trailer_parts_pv[2] = str(len(registros)).zfill(6)
        trailer_parts_pv[3] = str(len(registros)).zfill(6)
        # TOTAIS EM CENTAVOS (15 dígitos)
        trailer_parts_pv[4] = str(bruto).zfill(15)
        trailer_parts_pv[5] = str(desconto).zfill(15)
        trailer_parts_pv[6] = str(liquido).zfill(15)

        trailer_line_pv = ",".join(trailer_parts_pv)

        # Nome final: PV_DDMMAA_NSA_EEVD.txt
        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVD.txt"
        out_path = ensure_outfile(output_dir, nome_arquivo)

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(header_line_pv + "\n")
            for p in registros:
                f.write(",".join(p) + "\n")
            f.write(trailer_line_pv + "\n")

        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)}")

    # === Validação do arquivo-mãe por valores (TOTAL BRUTO) ===
    total_trailer = to_centavos(trailer_parts[4] if len(trailer_parts) > 4 else "0")
    detalhe = validar_totais(total_trailer, soma_bruto_total)
    status = "OK" if total_trailer == soma_bruto_total else "ERRO"

    print(f"✅ Total trailer: {total_trailer} | Processado: {soma_bruto_total} | {status}")

    return {
        "total_trailer": total_trailer,
        "total_processado": soma_bruto_total,
        "status": status,
        "detalhe": detalhe,
    }
