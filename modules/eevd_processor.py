import os
import csv
from collections import defaultdict
from datetime import datetime
from utils.file_utils import sanitize_filename, ensure_outfile
from utils.validation_utils import validar_totais


def to_centavos(valor_str: str) -> int:
    """Converte string numérica do layout (sem ponto) para inteiro em centavos."""
    if not valor_str:
        return 0
    valor_str = valor_str.strip()
    if valor_str.isdigit():
        return int(valor_str)
    return 0


def process_eevd(input_path, output_dir):
    """Processa arquivo EEVD (Vendas Débito) — versão v3 validando por valores."""
    print("🟢 Processando EEVD (Vendas Débito)")

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f if l.strip()]

    if not lines:
        raise ValueError("Arquivo EEVD vazio.")

    # Header e trailer do arquivo-mãe
    header_line = lines[0]
    trailer_line = lines[-1]
    detalhes = lines[1:-1]

    header_parts = [p.strip() for p in header_line.split(",")]
    trailer_parts = [p.strip() for p in trailer_line.split(",")]

    data_ref = header_parts[2][-6:]  # DDMMAA
    nsa = header_parts[7][-3:].zfill(3)

    grupos = defaultdict(list)
    totais_pv = {}

    for line in detalhes:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 8:
            continue

        if parts[0] == "01":  # Detalhe
            pv = parts[1]
            bruto = to_centavos(parts[4])
            desconto = to_centavos(parts[5])
            liquido = to_centavos(parts[6])

            grupos[pv].append(parts)
            if pv not in totais_pv:
                totais_pv[pv] = {"bruto": 0, "desconto": 0, "liquido": 0}
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

        # Header personalizado (PV filho)
        header_parts_pv = header_parts.copy()
        header_parts_pv[1] = pv
        header_line_pv = ",".join(header_parts_pv)

        # Trailer personalizado (PV filho)
        trailer_parts_pv = trailer_parts.copy()
        trailer_parts_pv[1] = pv
        trailer_parts_pv[2] = str(len(registros)).zfill(6)
        trailer_parts_pv[4] = str(bruto).zfill(15)
        trailer_parts_pv[5] = str(desconto).zfill(15)
        trailer_parts_pv[6] = str(liquido).zfill(15)
        trailer_line_pv = ",".join(trailer_parts_pv)

        # Nome do arquivo final: PV_DDMMAA_NSA_EEVD.txt
        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVD.txt"
        out_path = ensure_outfile(output_dir, nome_arquivo)

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(header_line_pv + "\n")
            for p in registros:
                f.write(",".join(p) + "\n")
            f.write(trailer_line_pv + "\n")

        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)}")

    # === Validação de totais (baseada em centavos) ===
    total_trailer = to_centavos(trailer_parts[4])
    diferenca = soma_bruto_total - total_trailer
    if diferenca == 0:
        detalhe = "Validação OK — valores totais consistentes."
        status = "OK"
    else:
        detalhe = f"Divergência de valor: {diferenca:+,} centavos."
        status = "ERRO"

    print(f"✅ Total trailer: {total_trailer} | Processado: {soma_bruto_total} | {status}")

    return {
        "total_trailer": total_trailer,
        "total_processado": soma_bruto_total,
        "status": status,
        "detalhe": detalhe,
    }
