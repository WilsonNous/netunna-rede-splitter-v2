from flask import Flask, jsonify, request, render_template
from splitter_core import process_file
from validator import validate_file
from mover import move_processed_file
from logger import log_operation
from notifier import send_alert
import os

app = Flask(__name__, template_folder="templates")

INPUT_DIR = "input"
OUTPUT_DIR = "output"
ERROR_DIR = "erro"
LOG_DIR = "logs"

@app.route("/")
def index():
    """Painel Web"""
    return render_template("dashboard.html")

@app.route("/api/scan", methods=["GET"])
def scan_files():
    files = os.listdir(INPUT_DIR)
    return jsonify({"arquivos": files})

@app.route("/api/process", methods=["POST"])
def process_endpoint():
    data = request.get_json()
    filename = data.get("filename")
    if not filename:
        return jsonify({"erro": "Informe o nome do arquivo"}), 400

    input_path = os.path.join(INPUT_DIR, filename)
    if not os.path.exists(input_path):
        return jsonify({"erro": "Arquivo não encontrado"}), 404

    try:
        generated_files = process_file(input_path, OUTPUT_DIR)
        is_valid, summary = validate_file(input_path, generated_files)
        log_operation(filename, is_valid, summary)

        if not is_valid:
            send_alert(
                assunto=f"[ALERTA] Divergência detectada em {filename}",
                corpo=f"Os totais do arquivo {filename} não conferem.\n\n{summary}",
                destino="edi@cliente.com.br"
            )
            move_processed_file(input_path, ERROR_DIR)
        else:
            move_processed_file(input_path, OUTPUT_DIR)

        return jsonify({"status": "ok", "gerados": len(generated_files), "valido": is_valid, "resumo": summary})
    except Exception as e:
        send_alert(
            assunto=f"[ERRO] Falha no processamento de {filename}",
            corpo=str(e),
            destino="edi@cliente.com.br"
        )
        move_processed_file(input_path, ERROR_DIR)
        return jsonify({"erro": str(e)}), 500

@app.route("/api/status", methods=["GET"])
def get_status():
    path = os.path.join(LOG_DIR, "operacoes.csv")
    if not os.path.exists(path):
        return jsonify({"mensagem": "Nenhum log encontrado"}), 200
    with open(path, "r", encoding="utf-8") as f:
        logs = f.read().splitlines()[-10:]
    return jsonify({"ultimos_logs": logs})

if __name__ == "__main__":
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(ERROR_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    app.run(host="0.0.0.0", port=5000)
