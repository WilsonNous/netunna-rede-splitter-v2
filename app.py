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
# API: Upload
# ==============================
@app.route("/api/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files:
        return jsonify({"erro": "Nenhum arquivo enviado."}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"erro": "Nome de arquivo vazio."}), 400
    save_path = os.path.join(INPUT_DIR, file.filename)
    file.save(save_path)
    print(f"📤 Arquivo recebido: {file.filename}")
    return jsonify({"mensagem": f"Arquivo {file.filename} recebido com sucesso."}), 200

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
# API: Download ZIP
# ==============================
@app.route("/api/download-all", methods=["GET"])
def download_all():
    import zipfile
    from io import BytesIO

    zip_stream = BytesIO()
    with zipfile.ZipFile(zip_stream, "w") as zf:
        for fname in os.listdir(OUTPUT_DIR):
            fpath = os.path.join(OUTPUT_DIR, fname)
            if os.path.isfile(fpath):
                zf.write(fpath, arcname=fname)
    zip_stream.seek(0)

    return send_from_directory(
        directory=OUTPUT_DIR,
        path=".",
        as_attachment=True,
        download_name="rede_splitter_output.zip"
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
