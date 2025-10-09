import os
import csv
from collections import defaultdict
from datetime import datetime
from utils.file_utils import sanitize_filename, ensure_outfile
from utils.validation_utils import validar_totais

def process_eevd(input_path, output_dir):
    """Processa arquivo EEVD (Vendas Débito) — modular v3"""
    print("🟢 Processando EEVD (Vendas Débito)")
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        lines = [l.strip() for l in f if l.strip()]

    if not lines:
        raise ValueError("Arquivo EEVD vazio.")

    header_line = lines[0]
    trailer_line = lines[-1]
    detalhes = lines[1:-1]

    header_parts = header_line.split(',')
    trailer_parts = trailer_line.split(',')

    data_ref = header_parts[2][-6:]  # DDMMAA
    nsa = header_parts[7][-3:].zfill(3)

    grupos = defaultdict(list)
    totais_pv = {}

    for line in detalhes:
        parts = [p.strip() for p in line.split(',')]
        if len(parts) < 8: 
            continue
        if parts[0] == "01":
            pv = parts[1]
            bruto = int(parts[4]) if parts[4].isdigit() else 0
            desconto = int(parts[5]) if parts[5].isdigit() else 0
            liquido = int(parts[6]) if parts[6].isdigit() else 0
            grupos[pv].append(parts)
            totais_pv.setdefault(pv, {"bruto":0,"desconto":0,"liquido":0})
            totais_pv[pv]["bruto"] += bruto
            totais_pv[pv]["desconto"] += desconto
            totais_pv[pv]["liquido"] += liquido

    gerados = []
    soma_bruto_total = soma_liquido_total = 0

    for pv, registros in grupos.items():
        bruto = totais_pv[pv]["bruto"]
        desconto = totais_pv[pv]["desconto"]
        liquido = totais_pv[pv]["liquido"]
        soma_bruto_total += bruto
        soma_liquido_total += liquido

        # Novo header com código do PV
        header_parts_pv = header_parts.copy()
        header_parts_pv[1] = pv
        header_line_pv = ",".join(header_parts_pv)

        # Novo trailer com totais do PV
        trailer_parts_pv = trailer_parts.copy()
        trailer_parts_pv[1] = pv
        trailer_parts_pv[2] = str(len(registros)).zfill(6)
        trailer_parts_pv[4] = str(bruto).zfill(15)
        trailer_parts_pv[5] = str(desconto).zfill(15)
        trailer_parts_pv[6] = str(liquido).zfill(15)
        trailer_line_pv = ",".join(trailer_parts_pv)

        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVD.txt"
        out_path = ensure_outfile(output_dir, nome_arquivo)

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(header_line_pv + "\n")
            for p in registros:
                f.write(",".join(p) + "\n")
            f.write(trailer_line_pv + "\n")

        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)}")

    total_trailer = int(trailer_parts[4]) if trailer_parts[4].isdigit() else 0
    resultado = validar_totais(total_trailer, soma_bruto_total)

    return {
        "total_trailer": total_trailer,
        "total_processado": soma_bruto_total,
        "detalhe": resultado,
    }
