"""
Agente Netunna API - Controle Remoto via HTTP
Expondo endpoints:
 - /api/agente/run
 - /api/agente/status
 - /api/agente/download
"""

from flask import Flask, jsonify
from threading import Thread
import traceback
import os

from main import main as executar_ciclo
from agente.utils import log, ler_ultimos_logs
from agente.downloader import baixar_output

# ---------------------------------------------------------
# Configuração básica
# ---------------------------------------------------------
app = Flask(__name__)

# Evita logs duplicados do Flask em produção
import logging
log_flask = logging.getLogger('werkzeug')
log_flask.setLevel(logging.ERROR)

# ---------------------------------------------------------
# Handlers de erro globais
# ---------------------------------------------------------
@app.errorhandler(Exception)
def handle_exception(e):
    """Garante retorno JSON mesmo em erro inesperado."""
    msg = f"❌ Erro interno no Agente: {str(e)}"
    traceback.print_exc()
    log(msg)
    return jsonify({"status": "error", "msg": msg}), 500

# ---------------------------------------------------------
# Endpoint: Executar agente
# ---------------------------------------------------------
@app.route("/api/agente/run", methods=["POST"])
def run_agente():
    try:
        log("▶️ Execução remota solicitada via painel.")
        Thread(target=executar_ciclo, daemon=True).start()
        return jsonify({"status": "started", "msg": "Agente executando em background."})
    except Exception as e:
        log(f"❌ Falha ao iniciar agente: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

# ---------------------------------------------------------
# Endpoint: Status do agente
# ---------------------------------------------------------
@app.route("/api/agente/status", methods=["GET"])
def status():
    try:
        logs = ler_ultimos_logs(40)
        return jsonify({"status": "ok", "logs": logs})
    except Exception as e:
        log(f"⚠️ Falha ao ler logs: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

# ---------------------------------------------------------
# Endpoint: Download de resultados
# ---------------------------------------------------------
@app.route("/api/agente/download", methods=["GET"])
def download():
    try:
        log("⬇️ Download remoto solicitado via painel.")
        Thread(target=baixar_output, daemon=True).start()
        return jsonify({"status": "started", "msg": "Download iniciado."})
    except Exception as e:
        log(f"❌ Erro ao iniciar download: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

# ---------------------------------------------------------
# Inicialização
# ---------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("AGENTE_PORT", 10000))
    log(f"🌐 API do Agente Netunna iniciando na porta {port}")
    try:
        app.run(host="0.0.0.0", port=port)
    except Exception as e:
        log(f"💥 Falha ao subir servidor Flask: {e}")
