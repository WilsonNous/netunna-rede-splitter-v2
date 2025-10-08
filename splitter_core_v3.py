import os
import re
import csv
import smtplib
from collections import defaultdict
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json

# ==============================
# Caminhos principais
# ==============================
LOG_PATH = os.path.join("logs", "operacoes.csv")
EMAIL_CONFIG = "config_email.json"

# ==============================
# Funções utilitárias
# ==============================
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def log_result(arquivo, tipo, total_trailer, total_processado, status, detalhe):
    """Registra resultado no CSV"""
    ensure_dir(os.path.dirname(LOG_PATH))
    nova_linha = {
        "data_hora": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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

def enviar_email_alerta(arquivo, tipo, total_trailer, total_proc, detalhe):
    """Envia e-mail se ocorrer divergência"""
    if not os.path.exists(EMAIL_CONFIG):
        print("⚠️ Arquivo de configuração de e-mail não encontrado.")
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
    Olá, equipe EDI Netunna 👋

    Durante o processamento automático, foi detectada uma divergência no arquivo {arquivo}.

    📁 Arquivo: {arquivo}
    📊 Tipo: {tipo}
    🔢 Total no trailer: {total_trailer}
    📈 Total processado: {total_proc}

    🟠 Detalhe: {detalhe}

    O arquivo foi movido para a pasta /erro para análise manual.
    """

    msg = MIMEMultipart()
    msg["From"] = f"Netunna EDI Automations <{smtp_user}>"
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = assunto
    msg.attach(MIMEText(corpo, "plain", "utf-8"))

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, recipients, msg.as_string())
        print(f"📧 Alerta de divergência enviado para {recipients}")
    except Exception as e:
        print(f"❌ Falha ao enviar e-mail: {e}")

# ==============================
# Função principal
# ==============================
def process_file(input_path, output_dir, error_dir):
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(error_dir, exist_ok=True)

    filename = os.path.basename(input_path).upper()

    if "EEVC" in filename or "_VC_" in filename:
        print("🟠 Detectado arquivo EEVC (Vendas Crédito)")
        return process_eevc(input_path, output_dir)
    
    elif "EEVD" in filename or "_VD_" in filename:
        print("🟢 Detectado arquivo EEVD (Vendas Débito)")
        return process_eevd(input_path, output_dir)
    
    elif "EEFI" in filename or "_FI_" in filename:
        print("🔵 Detectado arquivo EEFI (Financeiro)")
        return process_eefi(input_path, output_dir)
    else:
        raise ValueError("Tipo de arquivo não reconhecido.")

    # Validação: contagem de registros
    total_trailer = resultado.get("total_trailer", 0)
    total_processado = resultado.get("total_processado", 0)
    status = "OK" if total_trailer == total_processado else "ERRO"

    detalhe = "Validação concluída sem divergências." if status == "OK" else \
              f"Divergência na contagem de registros ({abs(total_trailer - total_processado)} de diferença)."

    log_result(filename, tipo, total_trailer, total_processado, status, detalhe)

    if status == "ERRO":
        enviar_email_alerta(filename, tipo, total_trailer, total_processado, detalhe)
        os.rename(input_path, os.path.join(error_dir, os.path.basename(input_path)))

    print(f"✅ {filename}: {status}")
    return {"status": status, "detalhe": detalhe}


# ==============================
# Funções de processamento
# ==============================
def process_eevc(input_path, output_dir):
    total_trailer = 0
    total_proc = 0
    grupos = defaultdict(list)

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
        nome = f"{pv}_EEVC.txt"
        with open(os.path.join(output_dir, nome), "w", encoding="utf-8") as f:
            f.write("\n".join(blocos))

    return {"total_trailer": total_trailer, "total_processado": total_proc}


def process_eevd(input_path, output_dir):
    total_trailer = 0
    total_proc = 0
    grupos = defaultdict(list)

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
        nome = f"{pv}_EEVD.txt"
        with open(os.path.join(output_dir, nome), "w", encoding="utf-8") as f:
            f.writelines(blocos)

    return {"total_trailer": total_trailer, "total_processado": total_proc}


def process_eefi(input_path, output_dir):
    total_trailer = 0
    total_proc = 0
    grupos = defaultdict(list)

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
        nome = f"{pv}_EEFI.txt"
        with open(os.path.join(output_dir, nome), "w", encoding="utf-8") as f:
            f.write("\n".join(blocos))

    return {"total_trailer": total_trailer, "total_processado": total_proc}
