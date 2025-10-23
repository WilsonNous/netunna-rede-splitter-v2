# modules/agente_routes.py
"""
Rotas do Agente Netunna (v4)
Integradas ao Flask principal
"""

from flask import Blueprint, jsonify
from threading import Thread
from agente.main_agente import main as executar_ciclo  # ✅ importa do pacote /agente
from agente.utils import log, ler_ultimos_logs
from agente.downloader import baixar_output

agente_bp = Blueprint("agente", __name__)

@agente_bp.route("/run", methods=["POST"])
def run_agente():
    log("▶️ Execução remota solicitada via painel Splitter.")
    Thread(target=executar_ciclo).start()
    return jsonify({"status": "started", "msg": "Agente executando em background."})

@agente_bp.route("/status", methods=["GET"])
def status():
    logs = ler_ultimos_logs(40)
    return jsonify({"status": "ok", "logs": logs})

@agente_bp.route("/download", methods=["GET"])
def download():
    log("⬇️ Download remoto solicitado via painel Splitter.")
    Thread(target=baixar_output).start()
    return jsonify({"status": "started", "msg": "Download iniciado."})
