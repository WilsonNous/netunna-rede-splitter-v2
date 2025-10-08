from flask import Flask, request, jsonify, render_template, send_file, send_from_directory
import os
import csv
from io import BytesIO
import zipfile
from splitter_core import process_file

app = Flask(__name__)

# Diretórios padrão
INPUT_DIR = "input"
OUTPUT_DIR = "output"
ERROR_DIR = "erro"
LOG_DIR = "logs"

for d in [INPUT_DIR, OUTPUT_DIR, ERROR_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "operacoes.csv")

# ==============================
# Página principal (Dashboard)
# ==============================
@app.route("/")
def home():
    files_input = os.listdir(INPUT_DIR)
    files_output = os.listdir(OUTPUT_DIR)
    return render_template("index.html", files_input=files_input, files_output=files_output)

# ==============================
# API: Scan diretórios
# ==============================
@app.route("/api/scan", methods=["GET"])
def scan_files():
    """Lista arquivos disponíveis em input/output"""
    return jsonify({
        "input": os.listdir(INPUT_DIR),
        "output": os.listdir(OUTPUT_DIR),
        "erro": os.listdir(ERROR_DIR)
    })

# ==============================
# API: Upload de arquivo
# ==============================
@app.route("/api/upload", methods=["POST"])
def upload_file():
    """Recebe arquivo do robô local e salva na pasta /input"""
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
# API: Processar arquivo
# ==============================
@app.route("/api/process", methods=["POST"])
def process_endpoint():
    """Processa o arquivo informado e gera os separados"""
    data = request.get_json()
    filename = data.get("filename")
    if not filename:
        return jsonify({"erro": "Nome do arquivo não informado."}), 400
    path_in = os.path.join(INPUT_DIR, filename)
    if not os.path.exists(path_in):
        return jsonify({"erro": f"Arquivo {filename} não encontrado."}), 404

    try:
        generated = process_file(path_in, OUTPUT_DIR)
        msg = f"{len(generated)} arquivos gerados em {OUTPUT_DIR}."
        print(f"✅ {msg}")

        # Registrar log
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as log:
            writer = csv.writer(log, delimiter=";")
            writer.writerow([filename, "OK", msg])

        return jsonify({"mensagem": msg, "arquivos": generated}), 200
    except Exception as e:
        print(f"❌ Erro ao processar {filename}: {e}")
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as log:
            writer = csv.writer(log, delimiter=";")
            writer.writerow([filename, "ERRO", str(e)])
        return jsonify({"erro": str(e)}), 500

# ==============================
# API: Logs / Status
# ==============================
@app.route("/api/status", methods=["GET"])
def status():
    """Retorna os últimos registros do log"""
    if not os.path.exists(LOG_FILE):
        return jsonify({"ultimos_logs": ["Nenhum log disponível ainda."]})

    with open(LOG_FILE, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()[-10:]

    return jsonify({"ultimos_logs": [line.strip() for line in lines]})

# ==============================
# API: Download de arquivo gerado
# ==============================
@app.route("/api/download/<filename>", methods=["GET"])
def download_file(filename):
    """Permite baixar um arquivo gerado individualmente"""
    safe_path = os.path.join(OUTPUT_DIR, filename)
    if not os.path.exists(safe_path):
        return jsonify({"erro": f"Arquivo {filename} não encontrado."}), 404
    return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)

# ==============================
# API: Download múltiplo (ZIP)
# ==============================
@app.route("/api/download-all", methods=["GET"])
def download_all():
    """Compacta todos os arquivos do output em um ZIP"""
    zip_stream = BytesIO()
    with zipfile.ZipFile(zip_stream, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in os.listdir(OUTPUT_DIR):
            fpath = os.path.join(OUTPUT_DIR, fname)
            if os.path.isfile(fpath):
                zf.write(fpath, arcname=fname)
    zip_stream.seek(0)
    return send_file(zip_stream, as_attachment=True, download_name="rede_splitter_output.zip")

# ==============================
# Inicialização
# ==============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
