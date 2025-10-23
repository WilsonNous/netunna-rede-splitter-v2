from flask import Flask, request, jsonify, render_template, send_from_directory, send_file
import os
import csv
import io
import zipfile
from datetime import datetime
import pytz  # ✅ para timezone Brasil
from splitter_core_v3 import process_file, LOG_PATH
from modules.processador_integridade import processar_integridade


app = Flask(__name__)

# Diretórios no ambiente Azure (montados via Path Mapping)
INPUT_DIR = "/home/input"
OUTPUT_DIR = "/home/output"
ERROR_DIR = "/home/erro"
LOG_DIR = "/home/logs"

# Caminho completo do CSV de logs
LOG_PATH = os.path.join(LOG_DIR, "operacoes.csv")

# Garante que todas as pastas existam (inclusive na primeira execução)
for d in [INPUT_DIR, OUTPUT_DIR, ERROR_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

# ✅ Timezone Brasil
TZ_BR = pytz.timezone("America/Sao_Paulo")

# ==============================
# Página principal (Painel)
# ==============================
@app.route("/")
def home():
    files_input = os.listdir(INPUT_DIR)
    files_output = os.listdir(OUTPUT_DIR)
    logs = []
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            logs = list(reader)[-50:]  # mostra últimos 50
    return render_template("index.html", files_input=files_input, files_output=files_output, logs=logs)

# ==============================
# API: Upload de arquivo (processamento automático)
# ==============================
@app.route("/api/upload", methods=["POST"])
def upload_file():
    """Recebe arquivo e processa automaticamente após upload."""
    if "file" not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado."}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"erro": "Nome de arquivo vazio."}), 400

    save_path = os.path.join(INPUT_DIR, file.filename)
    file.save(save_path)
    print(f"📤 Arquivo recebido: {file.filename}")

    # Processamento automático
    try:
        resultado = process_file(save_path, OUTPUT_DIR, ERROR_DIR)
        print(f"✅ Processado automaticamente: {file.filename}")

        # 🔹 Validação automática após o processamento
        tipo = resultado.get("tipo")
        nsa = resultado.get("nsa") or "000"
        arquivo_mae = save_path
        if tipo in ("EEVC", "EEVD", "EEFI"):
            try:
                valid = processar_integridade(tipo, arquivo_mae, OUTPUT_DIR)
                print(f"✅ Validação automática concluída: {valid.get('mensagem')}")
            except Exception as ve:
                print(f"⚠️ Erro na validação automática: {ve}")
        else:
            print(f"ℹ️ Tipo {tipo} não é elegível para validação automática.")

        return jsonify({
            "mensagem": f"Arquivo {file.filename} recebido e processado automaticamente.",
            "resultado": resultado
        }), 200
    except Exception as e:
        print(f"❌ Erro ao processar {file.filename}: {e}")
        return jsonify({"erro": str(e)}), 500

# ==============================
# API: Processar manualmente
# ==============================
@app.route("/api/process", methods=["POST"])
def process_endpoint():
    data = request.get_json()
    filename = data.get("filename")
    if not filename:
        return jsonify({"erro": "Nome do arquivo não informado."}), 400
    path_in = os.path.join(INPUT_DIR, filename)
    if not os.path.exists(path_in):
        return jsonify({"erro": f"Arquivo {filename} não encontrado."}), 404

    try:
        resultado = process_file(path_in, OUTPUT_DIR, ERROR_DIR)
        print(f"✅ Processado manualmente: {filename}")

        # 🔹 Validação automática também no processamento manual
        tipo = resultado.get("tipo")
        nsa = resultado.get("nsa") or "000"
        arquivo_mae = path_in
        if tipo in ("EEVC", "EEVD", "EEFI"):
            try:
                valid = processar_integridade(tipo, arquivo_mae, OUTPUT_DIR)
                print(f"✅ Validação automática concluída: {valid.get('mensagem')}")
            except Exception as ve:
                print(f"⚠️ Erro na validação automática: {ve}")
        else:
            print(f"ℹ️ Tipo {tipo} não é elegível para validação automática.")

        return jsonify({"mensagem": "Processado", "resultado": resultado}), 200
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

# ==============================
# API: Validação de Integridade
# ==============================
@app.route("/api/validate", methods=["POST"])
def api_validate():
    """
    Realiza a validação de integridade entre arquivo mãe e filhos por tipo.
    Espera JSON: { "tipo": "EEFI", "arquivo_mae": "VENTUNO_05102025.TXT", "nsa": "037" }
    """
    data = request.get_json()
    tipo = data.get("tipo")
    arquivo_mae = data.get("arquivo_mae")
    nsa = data.get("nsa")

    if not all([tipo, arquivo_mae, nsa]):
        return jsonify({"ok": False, "mensagem": "Campos obrigatórios: tipo, arquivo_mae, nsa"}), 400

    arquivo_path = os.path.join(INPUT_DIR, arquivo_mae)
    pasta_filhos = os.path.join(OUTPUT_DIR, f"NSA_{nsa}")

    if not os.path.exists(arquivo_path):
        return jsonify({"ok": False, "mensagem": f"Arquivo mãe não encontrado: {arquivo_mae}"}), 404

    if not os.path.exists(pasta_filhos):
        return jsonify({"ok": False, "mensagem": f"Pasta de filhos não encontrada: {pasta_filhos}"}), 404

    try:
        resultado = processar_integridade(tipo, arquivo_path, pasta_filhos)
        return jsonify(resultado), 200
    except Exception as e:
        print(f"❌ Erro na validação de integridade: {e}")
        return jsonify({"ok": False, "mensagem": str(e)}), 500

# ==============================
# API: Status / Logs
# ==============================
@app.route("/api/status", methods=["GET"])
def get_status():
    if not os.path.exists(LOG_PATH):
        return jsonify({"logs": []})
    with open(LOG_PATH, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        logs = list(reader)
    return jsonify({"logs": logs})

# ==============================
# API: Download individual
# ==============================
@app.route("/api/download/<filename>", methods=["GET"])
def download_file(filename):
    return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)

# ==============================
# API: Download ZIP consolidado
# ==============================
@app.route("/api/download-all", methods=["GET"])
def api_download_all():
    """Gera e envia um ZIP real contendo todos os arquivos processados."""
    base_output = "output"
    memory_file = io.BytesIO()

    with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(base_output):
            for f in files:
                file_path = os.path.join(root, f)
                arcname = os.path.relpath(file_path, base_output)
                zipf.write(file_path, arcname)
    memory_file.seek(0)

    zip_name = f"output_{datetime.now(TZ_BR).strftime('%Y%m%d_%H%M%S')}.zip"
    return send_file(
        memory_file,
        mimetype="application/zip",
        as_attachment=True,
        download_name=zip_name
    )

# ==============================
# API: Scan diretórios (ajustado com fuso horário Brasil)
# ==============================
@app.route("/api/scan", methods=["GET"])
def api_scan():
    """Lista arquivos de saída agrupados por subpasta NSA_xxx."""
    base_input = "input"
    base_output = "output"
    result = {"input": [], "output": []}

    # 🔹 Lista INPUT
    if os.path.exists(base_input):
        for f in sorted(os.listdir(base_input)):
            fpath = os.path.join(base_input, f)
            if os.path.isfile(fpath):
                dt_brasil = datetime.fromtimestamp(os.path.getmtime(fpath), TZ_BR)
                result["input"].append({
                    "nome": f,
                    "data_hora": dt_brasil.strftime("%d/%m/%Y %H:%M:%S")
                })

    # 🔹 Lista OUTPUT agrupando por subpasta NSA
    if os.path.exists(base_output):
        for root, dirs, files in os.walk(base_output):
            if not files:
                continue
            lote = os.path.basename(root)
            if not lote.startswith("NSA_"):
                continue
            for f in sorted(files):
                fpath = os.path.join(root, f)
                dt_brasil = datetime.fromtimestamp(os.path.getmtime(fpath), TZ_BR)
                result["output"].append({
                    "nome": f,
                    "lote": lote,
                    "data_hora": dt_brasil.strftime("%d/%m/%Y %H:%M:%S")
                })
    return jsonify(result)

# ==============================
# Execução
# ==============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
