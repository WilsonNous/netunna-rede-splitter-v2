import os
import re
import csv
import smtplib
from collections import defaultdict
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json

from logger import log_operation
from validator import validate_file

LOG_PATH = os.path.join("logs", "operacoes.csv")
EMAIL_CONFIG = "config_email.json"

# ==============================
# Funções utilitárias
# ==============================
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def enviar_email_alerta(arquivo, tipo, total_trailer, total_proc, detalhe):
    """Envia e-mail se ocorrer divergência — com fallback seguro (sem travar app)."""
    if not os.path.exists(EMAIL_CONFIG):
        print("⚠️ Configuração de e-mail não encontrada.")
        return

    with open(EMAIL_CONFIG, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    smtp_server = cfg["smtp_server"]
    smtp_port = cfg["smtp_port"]
    smtp_user = cfg["username"]
    smtp_pass = cfg["password"]
    recipients = cfg["recipients"]

    assunto = f"⚠️ Divergência detectada no arquivo {arquivo}"
    corpo = f"""
    [Alerta Automático Netunna EDI]

    Arquivo: {arquivo}
    Tipo: {tipo}
    Trailer: {total_trailer}
    Processado: {total_proc}
    Detalhe: {detalhe}
    """

    try:
        msg = MIMEMultipart()
        msg["From"] = f"Netunna EDI Automations <{smtp_user}>"
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = assunto
        msg.attach(MIMEText(corpo, "plain", "utf-8"))

        with smtplib.SMTP(smtp_server, smtp_port, timeout=15) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, recipients, msg.as_string())

        print(f"📧 Alerta enviado para {recipients}")

    except Exception as e:
        fallback_msg = f"[{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}] Falha ao enviar e-mail: {e}"
        print(f"❌ {fallback_msg}")
        os.makedirs("logs", exist_ok=True)
        with open(os.path.join("logs", "email_falhas.log"), "a", encoding="utf-8") as logf:
            logf.write(fallback_msg + "\n")

# ==============================
# Função principal
# ==============================
def process_file(input_path, output_dir, error_dir):
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(error_dir, exist_ok=True)

    filename = os.path.basename(input_path).upper()
    tipo = "DESCONHECIDO"

    if "EEVC" in filename or "_VC_" in filename:
        tipo = "EEVC"
        print("🟠 Detectado arquivo EEVC (Vendas Crédito)")
        resultado = process_eevc(input_path, output_dir)
    elif "EEVD" in filename or "_VD_" in filename:
        tipo = "EEVD"
        print("🟢 Detectado arquivo EEVD (Vendas Débito)")
        resultado = process_eevd(input_path, output_dir)
    elif "EEFI" in filename or "_FI_" in filename:
        tipo = "EEFI"
        print("🔵 Detectado arquivo EEFI (Financeiro)")
        resultado = process_eefi(input_path, output_dir)
    else:
        raise ValueError("Tipo de arquivo não reconhecido.")

    # Validação e logging
    total_trailer = resultado.get("total_trailer", 0)
    total_processado = resultado.get("total_processado", 0)
    status = "OK" if total_trailer == total_processado else "ERRO"
    detalhe = "Validação concluída sem divergências." if status == "OK" else f"Divergência de {abs(total_trailer - total_processado)} registros."

    log_operation(filename, tipo, total_trailer, total_processado, status, detalhe)

    if status == "ERRO":
        enviar_email_alerta(filename, tipo, total_trailer, total_processado, detalhe)
        os.rename(input_path, os.path.join(error_dir, os.path.basename(input_path)))

    print(f"✅ {filename}: {status}")
    return {"status": status, "detalhe": detalhe}

# ==============================
# Processadores
# ==============================
def process_eevc(input_path, output_dir):
    total_trailer = 0
    total_proc = 0
    grupos = defaultdict(list)
    nsa = "000000"

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f]

    for line in lines:
        tipo = line[:3]
        if tipo == "002":
            nsa = line[66:72].strip()  # NSA recuperado
        elif tipo == "004":
            pv = line[3:12]
            grupos[pv].append(line)
            total_proc += 1
        elif tipo == "028":
            total_trailer += 1

    generated_files = []
    for pv, blocos in grupos.items():
        nome = f"{pv}_{nsa}_EEVC.txt"
        path = os.path.join(output_dir, nome)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(blocos))
        generated_files.append(path)

    return {"total_trailer": total_trailer, "total_processado": total_proc, "arquivos": generated_files}


def process_eevd(input_path, output_dir):
    total_trailer = 0
    total_proc = 0
    grupos = defaultdict(list)
    nsa = "000000"

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()

    for line in lines:
        parts = [p.strip() for p in line.split(",")]
        tipo = parts[0]
        if tipo == "00":
            nsa = parts[2][:6]
        elif tipo == "01":
            pv = parts[1]
            grupos[pv].append(line)
            total_proc += 1
        elif tipo == "04":
            total_trailer += 1

    generated_files = []
    for pv, blocos in grupos.items():
        nome = f"{pv}_{nsa}_EEVD.txt"
        path = os.path.join(output_dir, nome)
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(blocos)
        generated_files.append(path)

    return {"total_trailer": total_trailer, "total_processado": total_proc, "arquivos": generated_files}


def process_eefi(input_path, output_dir):
    total_trailer = 0
    total_proc = 0
    grupos = defaultdict(list)
    nsa = "000000"

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.strip() for l in f]

    for line in lines:
        tipo = line[:3]
        if tipo == "002":
            nsa = line[66:72].strip()
        elif tipo == "040":
            pv = line[2:11]
            grupos[pv].append(line)
            total_proc += 1
        elif tipo in ["050", "999"]:
            total_trailer += 1

    generated_files = []
    for pv, blocos in grupos.items():
        nome = f"{pv}_{nsa}_EEFI.txt"
        path = os.path.join(output_dir, nome)
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(blocos))
        generated_files.append(path)

    return {"total_trailer": total_trailer, "total_processado": total_proc, "arquivos": generated_files}
