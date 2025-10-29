"""
=========================================================
Rotas do Agente Netunna Splitter - v4.1
---------------------------------------------------------
Integração do Agente com o Painel Splitter principal.
Fornece endpoints REST para:
  • /api/agente/run       → Executar ciclo completo
  • /api/agente/status    → Consultar logs
  • /api/agente/download  → Baixar outputs
  • /api/agente/upload    → Enviar arquivos via painel
---------------------------------------------------------
Autor: Nous / Netunna Software © 2025
=========================================================
"""

import os
from threading import Thread
from flask import Blueprint, jsonify, request, current_app

# =========================================================
# 🔧 Imports internos (do pacote /agente)
# =========================================================
from agente.main_agente import main as executar_ciclo
from agente.utils import log, ler_ultimos_logs, ensure_dirs
from agente.downloader import baixar_output
from agente.uploader import upload_file

# =========================================================
# 🔹 Blueprint de integração
# =========================================================
agente_bp = Blueprint("agente", __name__)

# =========================================================
# 🧩 Funções auxiliares
# =========================================================
def _resolve_input_dir() -> str:
    """Resolve o diretório de INPUT do agente, com fallback seguro."""
    base_dir = os.getenv("BASE_DIR") or current_app.root_path
    input_dir = os.getenv("AGENTE_INPUT_DIR") or os.path.join(base_dir, "input")
    os.makedirs(input_dir, exist_ok=True)
    return input_dir

# =========================================================
# ▶️ Executar ciclo completo (Upload → Process → Download)
# =========================================================
@agente_bp.route("/run", methods=["POST"])
def run_agente():
    log("▶️ Execução remota solicitada via painel Splitter.")
    ensure_dirs()
    Thread(target=executar_ciclo, daemon=True).start()
    return jsonify({"status": "started", "msg": "Agente executando em background."})

# =========================================================
# 📊 Status / Logs do agente
# =========================================================
@agente_bp.route("/status", methods=["GET"])
def status():
    logs = ler_ultimos_logs(40)
    return jsonify({
        "status": "ok",
        "logs": logs or ["📭 Nenhum log recente encontrado."]
    })

# =========================================================
# ⬇️ Download remoto dos arquivos processados
# =========================================================
@agente_bp.route("/download", methods=["GET"])
def download():
    log("ℹ️ Download remoto não é mais executado pelo agente.")
    log("📦 Os arquivos processados estão disponíveis para download direto via painel Splitter.")
    return jsonify({
        "status": "ok",
        "msg": "Arquivos disponíveis no painel. Download automático desativado no agente."
    })

# =========================================================
# 📤 Upload de arquivos via painel (API)
# =========================================================
@agente_bp.route("/upload", methods=["POST"])
def upload_via_agente():
    """
    Recebe arquivos do painel e executa upload automático para o Splitter.
    Aceita multipart com:
      - 'file' (um arquivo)
      - 'files' ou 'files[]' (múltiplos)
    Fluxo:
      1) Salva localmente no AGENTE_INPUT_DIR
      2) Executa agente.uploader.upload_file() para envio remoto
    Retorna JSON com status por arquivo.
    """
    input_dir = _resolve_input_dir()

    # --- coleta arquivos enviados
    files = []
    if "file" in request.files:
        files.append(request.files["file"])
    if "files" in request.files:
        files.extend(request.files.getlist("files"))
    if "files[]" in request.files:
        files.extend(request.files.getlist("files[]"))

    if not files:
        return jsonify({
            "ok": False,
            "mensagem": "Nenhum arquivo enviado (use 'file' ou 'files[]' no multipart)."
        }), 400

    # --- processamento de uploads
    resultados = []
    for f in files:
        filename = f.filename or "sem_nome"
        save_path = os.path.join(input_dir, filename)
        try:
            f.save(save_path)
            log(f"📥 Recebido via painel: {filename} → {save_path}")
            ok = upload_file(save_path)
            resultados.append({
                "arquivo": filename,
                "salvo_em": save_path,
                "enviado": bool(ok)
            })
        except Exception as e:
            log(f"❌ Falha ao tratar upload '{filename}': {e}")
            resultados.append({
                "arquivo": filename,
                "erro": str(e),
                "enviado": False
            })

    ok_geral = any(r.get("enviado") for r in resultados)
    msg = "Arquivos processados com sucesso." if ok_geral else "Falha no envio de todos os arquivos."

    return jsonify({
        "ok": ok_geral,
        "mensagem": msg,
        "resultado": resultados
    })

# =========================================================
# 🩺 Healthcheck do agente
# =========================================================
@agente_bp.route("/health", methods=["GET"])
def health():
    """Verifica se o agente está online."""
    try:
        return jsonify({
            "status": "ok",
            "service": "Agente Netunna Splitter",
            "version": "v4.1"
        })
    except Exception as e:
        log(f"⚠️ Falha no healthcheck: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500


# =========================================================
# ⬇️ Pull síncrono (download com lease)
# =========================================================
@agente_bp.route("/pull", methods=["POST", "GET"])
def pull():
    """
    Executa um ciclo de download dos outputs no modo lease.
    Pode ser chamado por GET (com querystring) ou POST (JSON).
    Exemplo:
      GET  /api/agente/pull?limit=200&mode=lease&lotes=NSA_037,NSA_045
      POST /api/agente/pull { "limit":200, "mode":"lease", "lotes":["NSA_037"] }
    """
    try:
        limit = 200
        mode = "lease"
        lotes = []

        if request.method == "GET":
            limit = int(request.args.get("limit", limit))
            mode = request.args.get("mode", mode)
            if request.args.get("lotes"):
                lotes = [x.strip() for x in request.args.get("lotes").split(",") if x.strip()]
        else:
            data = request.get_json(silent=True) or {}
            limit = int(data.get("limit", limit))
            mode = data.get("mode", mode)
            lotes = data.get("lotes", lotes)

        # Aplica modo e chama downloader
        os.environ["DOWNLOAD_MODE"] = mode
        log(f"⏬ Pull solicitado → mode={mode}, limit={limit}, lotes={lotes or 'todos'}")

        from agente.downloader import baixar_output
        result = baixar_output(nsa_hint="000")

        if not result:
            return jsonify({"status": "error", "msg": "Nenhum retorno do downloader"}), 500

        return jsonify({
            "status": "success",
            "mode": mode,
            "limit": limit,
            "lotes": lotes,
            "resultado": result
        })

    except Exception as e:
        log(f"❌ Erro no pull: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500
