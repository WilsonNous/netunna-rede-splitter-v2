import os
import re
from collections import defaultdict
from datetime import datetime

# =============== utilidades ===============

INVALID_FN_CHARS = re.compile(r'[^A-Za-z0-9._-]')

def sanitize_filename(name: str) -> str:
    """Remove caracteres inválidos para nome de arquivo."""
    s = INVALID_FN_CHARS.sub('_', name.strip())
    return re.sub(r'_+', '_', s)

def ensure_outfile(path_dir: str, filename: str) -> str:
    os.makedirs(path_dir, exist_ok=True)
    filename = sanitize_filename(filename)
    return os.path.join(path_dir, filename)

def safe_slice(s: str, start: int, end: int) -> str:
    """Slice seguro para strings."""
    return s[start:end] if len(s) > start else ""

# =============== detecção ===============

def process_file(input_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.basename(input_path).upper()

    if "EEVC" in filename:
        print("🟠 Detectado arquivo EEVC (Vendas Crédito)")
        return process_eevc(input_path, output_dir)
    elif "EEVD" in filename:
        print("🟢 Detectado arquivo EEVD (Vendas Débito)")
        return process_eevd(input_path, output_dir)
    elif "EEFI" in filename:
        print("🔵 Detectado arquivo EEFI (Financeiro)")
        return process_eefi(input_path, output_dir)

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        header = f.readline().strip()

    if header.startswith("002"):
        return process_eevc(input_path, output_dir)
    elif header.startswith("00"):
        return process_eevd(input_path, output_dir)
    elif header.startswith("03"):
        return process_eefi(input_path, output_dir)
    else:
        print("❌ Tipo de arquivo desconhecido.")
        return []

# ============================================
# Parser EEVC (Crédito)
# ============================================
def process_eevc(input_path, output_dir):
    """
    EEVC (Crédito):
    - Data do Movimento: posições 3–10 (DDMMAAAA)
    - NSA: posições 67–73
    Nome: <estab>_<ddmmaa>_<nsa>_EEVC.txt
    """
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        lines = [l.rstrip('\n') for l in f]

    if not lines:
        print("Arquivo vazio.")
        return []

    header_arquivo = None
    trailer_arquivo = None
    grupos = defaultdict(list)
    current_estab = None
    data_movimento = "000000"
    nsa = "000"

    for line in lines:
        tipo = line[:3]
        if tipo == "002":
            header_arquivo = line
            # Data
            raw_data = line[3:11].strip()
            if re.fullmatch(r'\d{8}', raw_data):
                data_movimento = raw_data[:4] + raw_data[-2:]
            # NSA
            nsa_raw = line[71:77].strip()
            if re.fullmatch(r'\d{1,6}', nsa_raw):
                nsa = nsa_raw[-3:].zfill(3)
        elif tipo == "004":
            pv = line[3:12].strip()
            current_estab = pv
            grupos[pv].append(line)
        elif tipo == "026":
            if current_estab:
                grupos[current_estab].append(line)
                current_estab = None
        elif tipo == "028":
            trailer_arquivo = line
        else:
            if current_estab:
                grupos[current_estab].append(line)

    gerados = []
    for estab, blocos in grupos.items():
        nome = f"{estab}_{data_movimento}_{nsa}_EEVC.txt"
        out_path = ensure_outfile(output_dir, nome)
        with open(out_path, 'w', encoding='utf-8') as f:
            if header_arquivo:
                f.write(header_arquivo + '\n')
            for linha in blocos:
                f.write(linha + '\n')
            if trailer_arquivo:
                f.write(trailer_arquivo + '\n')
        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)}")

    print(f"✅ {len(gerados)} arquivos EEVC gerados.")
    return gerados


# ============================================
# Parser EEVD (Débito) - CSV delimitado
# ============================================
def process_eevd(input_path, output_dir):
    """
    EEVD: CSV (vírgula).
    - Data do Movimento: coluna 2 (DDMMAAAA)
    - NSA: coluna 8 (numérico)
    Nome: <estab>_<ddmmaa>_<nsa>_EEVD.txt
    """
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()

    if not lines:
        print("Arquivo vazio.")
        return []

    header_arquivo = None
    trailer_arquivo = None
    grupos = defaultdict(list)
    data_movimento = "000000"
    nsa = "000"

    for line in lines:
        parts = [p.strip() for p in line.split(",")]
        tipo = parts[0]
        if tipo == "00":
            header_arquivo = line
            # Corrigido: data está na 2ª vírgula (coluna 2)
            if len(parts) > 2 and re.fullmatch(r'\d{8}', parts[2]):
                d = parts[2]
                data_movimento = d[:4] + d[-2:]
            if len(parts) > 7 and parts[7].isdigit():
                nsa = parts[7][-3:].zfill(3)
        elif tipo == "04":
            trailer_arquivo = line
        elif len(parts) > 1:
            pv = parts[1]
            grupos[pv].append(line)

    gerados = []
    for estab, blocos in grupos.items():
        nome = f"{estab}_{data_movimento}_{nsa}_EEVD.txt"
        out_path = ensure_outfile(output_dir, nome)
        with open(out_path, 'w', encoding='utf-8') as f:
            if header_arquivo:
                f.write(header_arquivo)
            for linha in blocos:
                f.write(linha)
            if trailer_arquivo:
                f.write(trailer_arquivo)
        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)}")

    print(f"✅ {len(gerados)} arquivos EEVD gerados.")
    return gerados


# ============================================
# Parser EEFI (Financeiro)
# ============================================
def process_eefi(input_path, output_dir):
    """
    EEFI (Financeiro):
    - Header 030
    - Detalhes 040 (por PV)
    - Data do Movimento: posições 3–10 (DDMMAAAA)
    - NSA: posições 66–71
    Nome: <estab>_<ddmmaa>_<nsa>_EEFI.txt
    """
    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        lines = [l.rstrip('\n') for l in f]

    if not lines:
        print("Arquivo vazio.")
        return []

    header_arquivo = None
    trailer_arquivo = None
    grupos = defaultdict(list)
    data_movimento = "000000"
    nsa = "000"
    current_pv = None

    for line in lines:
        tipo = line[:3]
        if tipo == "030":
            header_arquivo = line
            # Data
            raw_data = line[3:11].strip()
            if re.fullmatch(r'\d{8}', raw_data):
                data_movimento = raw_data[:4] + raw_data[-2:]
            # NSA - corrigido para pegar os 3 últimos
            nsa_raw = line[75:81].strip()
            if re.fullmatch(r'\d{1,6}', nsa_raw):
                nsa = nsa_raw[-3:].zfill(3)
        elif tipo == "040":
            current_pv = line[2:11].strip() or "000000000"
            grupos[current_pv].append(line)
        elif tipo in ("050", "999"):
            trailer_arquivo = line
        else:
            if current_pv:
                grupos[current_pv].append(line)

    if not grupos:
        grupos["HEADER"] = lines

    gerados = []
    for estab, blocos in grupos.items():
        nome = f"{estab}_{data_movimento}_{nsa}_EEFI.txt"
        out_path = ensure_outfile(output_dir, nome)
        with open(out_path, 'w', encoding='utf-8') as f:
            if header_arquivo:
                f.write(header_arquivo + '\n')
            for linha in blocos:
                f.write(linha + '\n')
            if trailer_arquivo:
                f.write(trailer_arquivo + '\n')
        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)}")

    print(f"✅ {len(gerados)} arquivos EEFI gerados.")
    return gerados
