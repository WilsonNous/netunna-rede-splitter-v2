import os
import re
import csv
from collections import defaultdict
from datetime import datetime

# ==============================
# Caminhos principais
# ==============================
LOG_PATH = os.path.join("logs", "operacoes.csv")

# ==============================
# Funções utilitárias
# ==============================
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def log_result(arquivo, tipo, total_trailer, total_processado, status, detalhe):
    """Registra resultado no CSV de logs"""
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

# ==============================
# Função principal
# ==============================
def process_file(input_path, output_dir, error_dir):
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(error_dir, exist_ok=True)

    filename = os.path.basename(input_path).upper()
    print(f"📥 Iniciando processamento de: {filename}")

    # Identifica tipo de arquivo
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
# Funções de processamento
# ==============================
def process_eevc(input_path, output_dir):
    """Processa arquivo EEVC (Vendas Crédito)"""
    total_trailer = 0
    total_proc = 0
    grupos = defaultdict(list)
    data_ref = extrair_data_nome(input_path)
    nsa = extrair_nsa_nome(input_path)

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f]

    for line in lines:
        tipo = line[:3]
        if tipo == "004":
            pv = line[3:12]
            grupos[pv].append(line)
            total_proc += 1
        elif tipo == "028":
            total_trailer += 1

    for pv, blocos in grupos.items():
        nome = f"{pv}_{data_ref}_{nsa}_EEVC.txt"
        with open(os.path.join(output_dir, nome), "w", encoding="utf-8") as f:
            f.write("\n".join(blocos))

    return {"total_trailer": total_trailer, "total_processado": total_proc}


def process_eevd(input_path, output_dir):
    """Processa arquivo EEVD (Vendas Débito)"""
    total_trailer = 0
    total_proc = 0
    grupos = defaultdict(list)
    data_ref = extrair_data_nome(input_path)
    nsa = extrair_nsa_nome(input_path)

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    for line in lines:
        parts = [p.strip() for p in line.split(",")]
        tipo = parts[0]
        if tipo == "01":
            pv = parts[1]
            grupos[pv].append(line)
            total_proc += 1
        elif tipo == "04":
            total_trailer += 1

    for pv, blocos in grupos.items():
        nome = f"{pv}_{data_ref}_{nsa}_EEVD.txt"
        with open(os.path.join(output_dir, nome), "w", encoding="utf-8") as f:
            f.writelines(blocos)

    return {"total_trailer": total_trailer, "total_processado": total_proc}


def process_eefi(input_path, output_dir):
    """Processa arquivo EEFI (Financeiro)"""
    total_trailer = 0
    total_proc = 0
    grupos = defaultdict(list)
    data_ref = extrair_data_nome(input_path)
    nsa = extrair_nsa_nome(input_path)

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f]

    for line in lines:
        tipo = line[:3]
        if tipo == "040":
            pv = line[2:11]
            grupos[pv].append(line)
            total_proc += 1
        elif tipo in ["050", "999"]:
            total_trailer += 1

    for pv, blocos in grupos.items():
        nome = f"{pv}_{data_ref}_{nsa}_EEFI.txt"
        with open(os.path.join(output_dir, nome), "w", encoding="utf-8") as f:
            f.write("\n".join(blocos))

    return {"total_trailer": total_trailer, "total_processado": total_proc}


# ==============================
# Funções auxiliares
# ==============================
def extrair_data_nome(caminho):
    """Extrai data DDMMAA do nome do arquivo"""
    nome = os.path.basename(caminho)
    m = re.search(r"(\d{6,8})", nome)
    if m:
        data_raw = m.group(1)
        return data_raw[-6:]  # pega os últimos 6 dígitos (DDMMAA)
    else:
        return datetime.now().strftime("%d%m%y")


def extrair_nsa_nome(caminho):
    """Extrai o NSA do nome do arquivo (últimos 3 dígitos antes da extensão)"""
    nome = os.path.basename(caminho)
    partes = nome.split(".")
    if len(partes) >= 3 and partes[-1].isdigit():
        return partes[-1][-3:]
    # tenta buscar NSA numérico dentro do nome
    m = re.search(r"(\d{3})\D*$", nome)
    return m.group(1) if m else "000"
