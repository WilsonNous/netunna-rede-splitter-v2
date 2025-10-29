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

from agente.main_agente import main as executar_ciclo
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
# Endpoint: Health (sanidade)
# ---------------------------------------------------------
@app.route("/api/agente/health", methods=["GET"])
def health():
    try:
        return jsonify({"status": "ok", "service": "Agente Netunna", "version": "v4.1"}), 200
    except Exception as e:
        log(f"⚠️ Health falhou: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

# ---------------------------------------------------------
# Endpoint: Pull síncrono (lease/direct/zip)
# - GET  /api/agente/pull?limit=200&mode=lease&lotes=NSA_037,NSA_045
# - POST /api/agente/pull  { "limit": 200, "mode": "lease", "lotes": ["NSA_037"] }
# ---------------------------------------------------------
@app.route("/api/agente/pull", methods=["GET", "POST"])
def pull_sync():
    try:
        # Defaults
        limit = 200
        mode = os.getenv("DOWNLOAD_MODE", "lease").lower()
        lotes = []

        # Querystring
        if request.method == "GET":
            if request.args.get("limit"):
                limit = int(request.args.get("limit"))
            if request.args.get("mode"):
                mode = request.args.get("mode").lower()
            if request.args.get("lotes"):
                lotes = [s for s in request.args.get("lotes").split(",") if s.strip()]
        else:
            # JSON body
            payload = request.get_json(silent=True) or {}
            limit = int(payload.get("limit", limit))
            mode = str(payload.get("mode", mode)).lower()
            lotes = payload.get("lotes") or lotes

        # Chama o core (síncrono)
        # Observação: o 'mode' é lido de env dentro do downloader. Se o seu
        # baixar_output já respeita DOWNLOAD_MODE de env, você pode ignorar 'mode' aqui.
        # Caso deseje forçar por chamada, defina DOWNLOAD_MODE no os.environ antes:
        if mode in ("zip", "lease", "direct"):
            os.environ["DOWNLOAD_MODE"] = mode

        log(f"⏬ Pull solicitado: mode={mode} limit={limit} lotes={lotes or 'todos'}")
        result = baixar_output(nsa_hint="000", lotes=lotes, limit=limit)

        # Normaliza status HTTP
        http_status = 200
        status_txt = "success"
        if not result or result.get("ok") is False or str(result.get("notes", "")).startswith("fatal"):
            http_status = 500
            status_txt = "error"

        return jsonify({"status": status_txt, **(result or {})}), http_status

    except Exception as e:
        log(f"❌ Erro no pull_sync: {e}")
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
