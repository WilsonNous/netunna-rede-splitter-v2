from flask import Flask, request, jsonify, render_template, send_from_directory
import os
import csv
from datetime import datetime
from splitter_core_v3 import process_file, LOG_PATH

app = Flask(__name__)

# Diretórios padrão
INPUT_DIR = "input"
OUTPUT_DIR = "output"
ERROR_DIR = "erro"
LOG_DIR = "logs"

for d in [INPUT_DIR, OUTPUT_DIR, ERROR_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

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
# API: Upload de arquivo (agora processa automático)
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
        return jsonify({
            "mensagem": f"Arquivo {file.filename} recebido e processado automaticamente.",
            "resultado": resultado
        }), 200
    except Exception as e:
        print(f"❌ Erro ao processar {file.filename}: {e}")
        return jsonify({"erro": str(e)}), 500

# ==============================
# API: Processar
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
        return jsonify({"mensagem": "Processado", "resultado": resultado}), 200
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

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
# API: Download ZIP (corrigido)
# ==============================
from flask import send_file
import zipfile
from io import BytesIO

@app.route("/api/download-all", methods=["GET"])
def api_download_all():
    """Gera ZIPs individuais por NSA (ex: NSA_041.zip, NSA_042.zip, etc)."""
    base_output = "output"
    zip_dir = "zips"
    os.makedirs(zip_dir, exist_ok=True)

    created_zips = []
    for dirpath, dirnames, filenames in os.walk(base_output):
        if not filenames:
            continue
        lote = os.path.basename(dirpath)
        if not lote.startswith("NSA_"):
            continue

        zip_name = f"{lote}.zip"
        zip_path = os.path.join(zip_dir, zip_name)
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for f in filenames:
                full_path = os.path.join(dirpath, f)
                zipf.write(full_path, os.path.join(lote, f))
        created_zips.append(zip_name)

    return jsonify({"zips": created_zips})

# ==============================
# API: Scan diretórios (atualizada com data/hora)
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
                result["input"].append({
                    "nome": f,
                    "data_hora": datetime.fromtimestamp(os.path.getmtime(fpath)).strftime("%d/%m/%Y %H:%M:%S")
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
                result["output"].append({
                    "nome": f,
                    "lote": lote,
                    "data_hora": datetime.fromtimestamp(os.path.getmtime(os.path.join(root, f))).strftime("%d/%m/%Y %H:%M:%S")
                })
    return jsonify(result)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
