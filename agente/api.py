"""
Agente Netunna API - controle remoto via HTTP
Expondo endpoints: /api/agente/run /api/agente/status /api/agente/download
"""

from flask import Flask, jsonify
from threading import Thread
from main import main as executar_ciclo
from utils import log, ler_ultimos_logs
from downloader import baixar_output

app = Flask(__name__)

@app.route("/api/agente/run", methods=["POST"])
def run_agente():
    log("▶️ Execução remota solicitada via painel.")
    Thread(target=executar_ciclo).start()
    return jsonify({"status": "started", "msg": "Agente executando em background."})

@app.route("/api/agente/status", methods=["GET"])
def status():
    logs = ler_ultimos_logs(40)
    return jsonify({"status": "ok", "logs": logs})

@app.route("/api/agente/download", methods=["GET"])
def download():
    log("⬇️ Download remoto solicitado via painel.")
    Thread(target=baixar_output).start()
    return jsonify({"status": "started", "msg": "Download iniciado."})

if __name__ == "__main__":
    import os
    port = int(os.getenv("AGENTE_PORT", 10000))
    log(f"🌐 API do Agente Netunna iniciando na porta {port}")
    app.run(host="0.0.0.0", port=port)
