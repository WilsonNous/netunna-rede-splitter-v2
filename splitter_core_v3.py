import os
import re
import csv
from collections import defaultdict
from datetime import datetime

LOG_PATH = os.path.join("logs", "operacoes.csv")

# ==============================
# Funções utilitárias
# ==============================
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def log_result(arquivo, tipo, total_trailer, total_processado, status, detalhe):
    """Registra resultado no CSV"""
    ensure_dir(os.path.dirname(LOG_PATH))
    nova_linha = {
        "data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "arquivo": arquivo,
        "tipo": tipo,
        "total_trailer": total_trailer,
        "total_processado": total_processado,
        "status": status,
        "detalhe": detalhe
    }

    novo = not os.path.exists(LOG_PATH)
    with open(LOG_PATH, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=nova_linha.keys())
        if novo:
            writer.writeheader()
        writer.writerow(nova_linha)

def sanitize_filename(name: str) -> str:
    return re.sub(r'[^A-Za-z0-9._-]', '_', name.strip())

# ==============================
# Função principal
# ==============================
def process_file(input_path, output_dir, error_dir):
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(error_dir, exist_ok=True)

    filename = os.path.basename(input_path).upper()
    print(f"📥 Iniciando processamento de: {filename}")

    if "EEVC" in filename or "_VC_" in filename:
        tipo = "EEVC"
        resultado = process_eevc(input_path, output_dir)
    elif "EEVD" in filename or "_VD_" in filename:
        tipo = "EEVD"
        resultado = process_eevd(input_path, output_dir)
    elif "EEFI" in filename or "_FI_" in filename:
        tipo = "EEFI"
        resultado = process_eefi(input_path, output_dir)
    else:
        raise ValueError("Tipo de arquivo não reconhecido.")

    total_trailer = resultado.get("total_trailer", 0)
    total_processado = resultado.get("total_processado", 0)
    status = "OK" if total_trailer == total_processado else "ERRO"
    detalhe = (
        "Validação concluída sem divergências."
        if status == "OK"
        else f"Divergência de {abs(total_trailer - total_processado)} registros."
    )

    log_result(filename, tipo, total_trailer, total_processado, status, detalhe)
    print(f"✅ {filename}: {status} — {detalhe}")
    return {"status": status, "detalhe": detalhe}


# ==============================
# Processamento EEVC (Crédito)
# ==============================
def process_eevc(input_path, output_dir):
    total_trailer, total_proc = 0, 0
    grupos = defaultdict(list)
    header, trailer = None, None
    data_mov, nsa = "000000", "000"

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.rstrip("\n") for l in f]

    for line in lines:
        tipo = line[:3]
        if tipo == "002":
            header = line
            data_raw = line[3:11].strip()
            if re.fullmatch(r"\d{8}", data_raw):
                data_mov = data_raw[:4] + data_raw[-2:]
            nsa_raw = line[67:73].strip()
            if nsa_raw.isdigit():
                nsa = nsa_raw[-3:].zfill(3)
        elif tipo == "004":
            pv = line[3:12].strip()
            grupos[pv].append(line)
            total_proc += 1
        elif tipo == "028":
            trailer = line
            total_trailer += 1
        else:
            if grupos:
                pv = list(grupos.keys())[-1]
                grupos[pv].append(line)

    for pv, blocos in grupos.items():
        nome = sanitize_filename(f"{pv}_{data_mov}_{nsa}_EEVC.txt")
        path_out = os.path.join(output_dir, nome)
        with open(path_out, "w", encoding="utf-8") as out:
            if header:
                out.write(header + "\n")
            for b in blocos:
                out.write(b + "\n")
            if trailer:
                out.write(trailer + "\n")
        print(f"🧾 Gerado: {nome}")

    return {"total_trailer": total_trailer, "total_processado": total_proc}


# ==============================
# Processamento EEVD (Débito)
# ==============================
def process_eevd(input_path, output_dir):
    total_trailer, total_proc = 0, 0
    grupos = defaultdict(list)
    header, trailer = None, None
    data_mov, nsa = "000000", "000"

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    for line in lines:
        parts = [p.strip() for p in line.split(",")]
        tipo = parts[0]
        if tipo == "00":
            header = line.strip()
            if len(parts) > 2 and re.fullmatch(r"\d{8}", parts[2]):
                data_raw = parts[2]
                data_mov = data_raw[:4] + data_raw[-2:]
            if len(parts) > 7 and parts[7].isdigit():
                nsa = parts[7][-3:].zfill(3)
        elif tipo == "04":
            trailer = line.strip()
            total_trailer += 1
        elif tipo == "01" and len(parts) > 1:
            pv = parts[1]
            grupos[pv].append(line.strip())
            total_proc += 1
        else:
            if grupos:
                pv = list(grupos.keys())[-1]
                grupos[pv].append(line.strip())

    for pv, blocos in grupos.items():
        nome = sanitize_filename(f"{pv}_{data_mov}_{nsa}_EEVD.txt")
        path_out = os.path.join(output_dir, nome)
        with open(path_out, "w", encoding="utf-8") as out:
            if header:
                out.write(header + "\n")
            for b in blocos:
                out.write(b + "\n")
            if trailer:
                out.write(trailer + "\n")
        print(f"🧾 Gerado: {nome}")

    return {"total_trailer": total_trailer, "total_processado": total_proc}


# ==============================
# Processamento EEFI (Financeiro)
# ==============================
def process_eefi(input_path, output_dir):
    total_trailer, total_proc = 0, 0
    grupos = defaultdict(list)
    header, trailer = None, None
    data_mov, nsa = "000000", "000"

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.rstrip("\n") for l in f]

    for line in lines:
        tipo = line[:3]
        if tipo == "030":
            header = line
            data_raw = line[3:11].strip()
            if re.fullmatch(r"\d{8}", data_raw):
                data_mov = data_raw[:4] + data_raw[-2:]
            nsa_raw = line[66:71].strip()
            if nsa_raw.isdigit():
                nsa = nsa_raw[-3:].zfill(3)
        elif tipo == "040":
            pv = line[2:11].strip()
            grupos[pv].append(line)
            total_proc += 1
        elif tipo in ["050", "999"]:
            trailer = line
            total_trailer += 1
        else:
            if grupos:
                pv = list(grupos.keys())[-1]
                grupos[pv].append(line)

    for pv, blocos in grupos.items():
        nome = sanitize_filename(f"{pv}_{data_mov}_{nsa}_EEFI.txt")
        path_out = os.path.join(output_dir, nome)
        with open(path_out, "w", encoding="utf-8") as out:
            if header:
                out.write(header + "\n")
            for b in blocos:
                out.write(b + "\n")
            if trailer:
                out.write(trailer + "\n")
        print(f"🧾 Gerado: {nome}")

    return {"total_trailer": total_trailer, "total_processado": total_proc}
