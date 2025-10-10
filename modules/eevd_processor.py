import os
import re
from collections import defaultdict
from utils.file_utils import ensure_outfile
from utils.validation_utils import validar_totais, to_centavos


def limpar_diretorio(dir_path: str):
    """Limpa diretório antes de novo processamento."""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
        return
    for fname in os.listdir(dir_path):
        fpath = os.path.join(dir_path, fname)
        if os.path.isfile(fpath):
            os.remove(fpath)
    print(f"🧹 Diretório '{dir_path}' limpo antes do novo processamento.")


def _extrair_data_nsa(header_parts: list[str], nome_arquivo: str):
    """
    Extrai data (DDMMAA) e NSA do header EEVD.
    Exemplo header:
    00,020770677,07102025,06102025,Movimentacao diaria - Cartoes de Debito,Redecard,VENTUNO/FORTE             ,000043,DIARIO,...
    """
    data_ref = "000000"
    nsa = "000"

    # Campo 2 = Data do movimento (DDMMAAAA)
    if len(header_parts) > 2:
        campo_data = header_parts[2].strip()
        if re.fullmatch(r"\d{8}", campo_data):
            data_ref = f"{campo_data[:2]}{campo_data[2:4]}{campo_data[4:6]}"

    # Campo 7 = NSA (ex: 000043 → 043)
    if len(header_parts) > 7:
        campo_nsa = header_parts[7].strip()
        if campo_nsa.isdigit():
            nsa = campo_nsa[-3:].zfill(3)

    # Fallback via nome do arquivo, se algo vier errado
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


def process_eevd(input_path: str, output_dir: str, error_dir: str = "erro"):
    """Processa arquivo EEVD (Vendas Débito) — v3.7 com separação por lote NSA."""

    print("🟢 Processando EEVD (Vendas Débito)")

    filename = os.path.basename(input_path)
    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f if l.strip()]

    if not lines:
        raise ValueError("Arquivo EEVD vazio.")

    header_line = lines[0]
    trailer_line = lines[-1]
    detalhes = lines[1:-1]

    header_parts = [p.strip() for p in header_line.split(",")]
    trailer_parts = [p.strip() for p in trailer_line.split(",")]

    # 🔹 Extração segura de Data e NSA
    data_ref, nsa = _extrair_data_nsa(header_parts, filename)

    # 🔹 Cria subpasta específica por NSA
    lote_dir = os.path.join(output_dir, f"NSA_{nsa}")
    os.makedirs(lote_dir, exist_ok=True)
    print(f"📂 Criado diretório de saída: {lote_dir}")

    grupos = defaultdict(list)
    totais_pv = defaultdict(lambda: {"bruto": 0, "desconto": 0, "liquido": 0})
    soma_bruto_total = 0

    tipos_validos = ("01", "011", "012", "013")
    tipos_somaveis = ("01", "012", "013")

    for line in detalhes:
        parts = [p.strip() for p in line.split(",")]
        if not parts or parts[0] not in tipos_validos:
            continue

        while len(parts) < 9:
            parts.append("")

        tipo_registro = parts[0]
        pv = parts[1]
        bruto = to_centavos(parts[6])
        desconto = to_centavos(parts[7])
        liquido = to_centavos(parts[8])

        grupos[pv].append(parts)
        totais_pv[pv]["bruto"] += bruto
        totais_pv[pv]["desconto"] += desconto
        totais_pv[pv]["liquido"] += liquido

        if tipo_registro in tipos_somaveis:
            soma_bruto_total += bruto

    gerados = []
    for pv, registros in grupos.items():
        bruto = totais_pv[pv]["bruto"]
        desconto = totais_pv[pv]["desconto"]
        liquido = totais_pv[pv]["liquido"]

        header_parts_pv = header_parts.copy()
        header_parts_pv[1] = pv
        header_line_pv = ",".join(header_parts_pv)

        trailer_parts_pv = trailer_parts.copy()
        while len(trailer_parts_pv) < 11:
            trailer_parts_pv.append("0")
        trailer_parts_pv[1] = pv
        trailer_parts_pv[2] = str(len(registros)).zfill(6)
        trailer_parts_pv[4] = str(bruto).zfill(15)
        trailer_parts_pv[5] = str(desconto).zfill(15)
        trailer_parts_pv[6] = str(liquido).zfill(15)
        trailer_line_pv = ",".join(trailer_parts_pv)

        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVD.txt"
        out_path = ensure_outfile(lote_dir, nome_arquivo)

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(header_line_pv + "\n")
            for p in registros:
                f.write(",".join(p) + "\n")
            f.write(trailer_line_pv + "\n")

        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)} → {lote_dir}")

    total_trailer = to_centavos(trailer_parts[4] if len(trailer_parts) > 4 else "0")
    detalhe = validar_totais(total_trailer, soma_bruto_total)
    status = "OK" if total_trailer == soma_bruto_total else "ERRO"

    print(f"✅ Total trailer: {total_trailer} | Processado: {soma_bruto_total} | {status}")

    return {
        "arquivo": filename,
        "data_ref": data_ref,
        "nsa": nsa,
        "output_dir": lote_dir,
        "total_trailer": total_trailer,
        "total_processado": soma_bruto_total,
        "status": status,
        "detalhe": detalhe,
    }

