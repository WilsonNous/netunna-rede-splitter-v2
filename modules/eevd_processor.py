import os
from collections import defaultdict
from datetime import datetime
from utils.file_utils import ensure_outfile
from utils.validation_utils import validar_totais


def to_centavos(valor_str: str) -> int:
    """Converte string numérica do layout Rede (14 dígitos) para inteiro em centavos."""
    if not valor_str:
        return 0
    valor_str = valor_str.strip().replace(".", "").replace(",", "")
    if not valor_str.isdigit():
        return 0
    return int(valor_str)  # todos os valores já são expressos em centavos no layout Rede


def process_eevd(input_path, output_dir):
    """Processa arquivo EEVD (Vendas Débito) — v3 com trailer recalculado."""
    print("🟢 Processando EEVD (Vendas Débito)")

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f if l.strip()]

    if not lines:
        raise ValueError("Arquivo EEVD vazio.")

    # Header e trailer originais
    header_line = lines[0]
    trailer_line = lines[-1]
    detalhes = lines[1:-1]

    header_parts = [p.strip() for p in header_line.split(",")]
    trailer_parts = [p.strip() for p in trailer_line.split(",")]

    data_ref = header_parts[2][-6:]  # DDMMAA
    nsa = header_parts[7][-3:].zfill(3)

    grupos = defaultdict(list)
    totais_pv = {}

    # ======== Coleta de registros e somas por PV ========
    for line in detalhes:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 9 or parts[0] != "01":
            continue

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

    # ======== Geração de arquivos filhos ========
    soma_bruto_total = 0
    soma_liquido_total = 0
    soma_desconto_total = 0
    gerados = []

    for pv, registros in grupos.items():
        bruto = totais_pv[pv]["bruto"]
        desconto = totais_pv[pv]["desconto"]
        liquido = totais_pv[pv]["liquido"]

        soma_bruto_total += bruto
        soma_liquido_total += liquido
        soma_desconto_total += desconto

        # Novo header (PV individual)
        header_parts_pv = header_parts.copy()
        header_parts_pv[1] = pv
        header_line_pv = ",".join(header_parts_pv)

        # Novo trailer calculado com base nos detalhes
        trailer_parts_pv = [
            "04",  # tipo de registro trailer
            pv,
            str(len(registros)).zfill(6),  # quantidade de registros detalhe
            "000049",                      # campo fixo conforme layout original
            str(bruto).zfill(15),          # total bruto
            str(desconto).zfill(15),       # total desconto
            str(liquido).zfill(15),        # total líquido
            "000000000000000",             # campos reservados
            "000000000000000",
            "000000000000000",
            "000107"                       # código fixo de fechamento
        ]
        trailer_line_pv = ",".join(trailer_parts_pv)

        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVD.txt"
        out_path = ensure_outfile(output_dir, nome_arquivo)

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(header_line_pv + "\n")
            for p in registros:
                f.write(",".join(p) + "\n")
            f.write(trailer_line_pv + "\n")

        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)} — Bruto={bruto} Desconto={desconto} Líquido={liquido}")

    # ======== Validação global ========
    total_trailer_bruto = to_centavos(trailer_parts[4])
    resultado = validar_totais(total_trailer_bruto, soma_bruto_total)

    print(f"✅ Total trailer: {total_trailer_bruto:,} | Processado: {soma_bruto_total:,}")
    print(f"🏁 {resultado}")

    return {
        "total_trailer": total_trailer_bruto,
        "total_processado": soma_bruto_total,
        "status": "OK" if "OK" in resultado else "ERRO",
        "detalhe": resultado,
    }
