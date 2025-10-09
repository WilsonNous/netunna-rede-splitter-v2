import os
import re
from collections import defaultdict
from utils.file_utils import ensure_outfile
from utils.validation_utils import validar_totais, to_centavos

# ===========================================
# CONFIGURAÇÕES DE LIMPEZA
# ===========================================
PRESERVAR_OUTPUT = False   # Mantém os arquivos gerados no output?
PRESERVAR_ERRO = False     # Mantém os arquivos na pasta erro?
# -------------------------------------------


def _ddmmaa_from_yyyymmdd8(d8: str) -> str:
    """Converte 'DDMMAAAA' do header (8 dígitos) para 'DDMMAA'."""
    d8 = (d8 or "").strip()
    if re.fullmatch(r"\d{8}", d8):
        return f"{d8[:2]}{d8[2:4]}{d8[6:8]}"
    return "000000"


def limpar_diretorio(dir_path: str, preservar: bool = False):
    """Remove arquivos do diretório, caso não seja preservado."""
    if not preservar:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            return
        for fname in os.listdir(dir_path):
            fpath = os.path.join(dir_path, fname)
            if os.path.isfile(fpath):
                os.remove(fpath)
        print(f"🧹 Diretório '{dir_path}' limpo antes do novo processamento.")


def process_eevd(input_path: str, output_dir: str, error_dir: str = "erro"):
    """Processa arquivo EEVD (Vendas Débito) validando por VALORES em centavos."""
    print("🟢 Processando EEVD (Vendas Débito)")

    # === LIMPEZA DE CACHE E DIRETÓRIOS ===
    limpar_diretorio(output_dir, PRESERVAR_OUTPUT)
    limpar_diretorio(error_dir, PRESERVAR_ERRO)

    # === Inicializa estruturas locais (reset total) ===
    grupos = defaultdict(list)
    totais_pv = defaultdict(lambda: {"bruto": 0, "desconto": 0, "liquido": 0})
    soma_bruto_total = 0

    # --- Leitura do arquivo ---
    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f if l.strip()]

    if not lines:
        raise ValueError("Arquivo EEVD vazio.")

    header_line = lines[0]
    trailer_line = lines[-1]
    detalhes = lines[1:-1]

    header_parts = [p.strip() for p in header_line.split(",")]
    trailer_parts = [p.strip() for p in trailer_line.split(",")]

    # Data (DDMMAA) e NSA
    data_ref = _ddmmaa_from_yyyymmdd8(header_parts[2] if len(header_parts) > 2 else "")
    nsa = (header_parts[7] if len(header_parts) > 7 else "000")[-3:].zfill(3)

    # === AGRUPAMENTO E SOMA POR PV ===
    for line in detalhes:
        parts = [p.strip() for p in line.split(",")]
        if not parts or parts[0] != "01":
            continue

        # Garante pelo menos 9 colunas
        while len(parts) < 9:
            parts.append("")

        pv = parts[1]
        bruto = to_centavos(parts[6])
        desconto = to_centavos(parts[7])
        liquido = to_centavos(parts[8])

        grupos[pv].append(parts)
        totais_pv[pv]["bruto"] += bruto
        totais_pv[pv]["desconto"] += desconto
        totais_pv[pv]["liquido"] += liquido

    # === GERAÇÃO DOS ARQUIVOS FILHOS ===
    gerados = []
    for pv, registros in grupos.items():
        bruto = totais_pv[pv]["bruto"]
        desconto = totais_pv[pv]["desconto"]
        liquido = totais_pv[pv]["liquido"]
        soma_bruto_total += bruto

        # Header do filho (PV individual)
        header_parts_pv = header_parts.copy()
        if len(header_parts_pv) < 8:
            header_parts_pv += [""] * (8 - len(header_parts_pv))
        header_parts_pv[1] = pv
        header_line_pv = ",".join(header_parts_pv)

        # Trailer do filho com totais do PV
        trailer_parts_pv = trailer_parts.copy()
        while len(trailer_parts_pv) < 11:
            trailer_parts_pv.append("0")

        trailer_parts_pv[1] = pv
        trailer_parts_pv[2] = str(len(registros)).zfill(6)
        trailer_parts_pv[3] = str(len(registros)).zfill(6)
        trailer_parts_pv[4] = str(bruto).zfill(15)
        trailer_parts_pv[5] = str(desconto).zfill(15)
        trailer_parts_pv[6] = str(liquido).zfill(15)

        trailer_line_pv = ",".join(trailer_parts_pv)

        # Nome do arquivo final
        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVD.txt"
        out_path = ensure_outfile(output_dir, nome_arquivo)

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(header_line_pv + "\n")
            for p in registros:
                f.write(",".join(p) + "\n")
            f.write(trailer_line_pv + "\n")

        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)}")

    # === VALIDAÇÃO FINAL ===
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
