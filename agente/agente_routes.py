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
# ⬇️ Pull assíncrono com filtros de data
# =========================================================
@agente_bp.route("/pull", methods=["POST", "GET"])
def pull():
    """
    Executa ou dispara um ciclo de download dos outputs no modo lease ou zip.
    Pode ser chamado por GET (querystring) ou POST (JSON).
    Suporta filtros de data para reduzir volume e tráfego:
      - date_from=YYYY-MM-DD
      - date_to=YYYY-MM-DD
      - since_days=N  (ex.: últimos N dias)
    Exemplo:
      GET  /api/agente/pull?limit=200&mode=lease&since_days=2
      POST /api/agente/pull { "limit":100, "mode":"lease", "date_from":"2025-10-25" }
    """
    from datetime import datetime, timedelta
    from agente.downloader import baixar_output

    try:
        # --- Parâmetros padrão
        limit = 200
        mode = "lease"
        lotes = []
        date_filter = {}

        if request.method == "GET":
            limit = int(request.args.get("limit", limit))
            mode = request.args.get("mode", mode)
            if request.args.get("lotes"):
                lotes = [x.strip() for x in request.args.get("lotes").split(",") if x.strip()]

            # Filtros de data (GET)
            if request.args.get("since_days"):
                days = int(request.args.get("since_days"))
                date_filter["date_from"] = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            if request.args.get("date_from"):
                date_filter["date_from"] = request.args.get("date_from")
            if request.args.get("date_to"):
                date_filter["date_to"] = request.args.get("date_to")

        else:
            data = request.get_json(silent=True) or {}
            limit = int(data.get("limit", limit))
            mode = data.get("mode", mode)
            lotes = data.get("lotes", lotes)

            # Filtros de data (POST)
            if "since_days" in data:
                days = int(data["since_days"])
                date_filter["date_from"] = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            if "date_from" in data:
                date_filter["date_from"] = data["date_from"]
            if "date_to" in data:
                date_filter["date_to"] = data["date_to"]

        os.environ["DOWNLOAD_MODE"] = mode

        log(f"⏬ Pull solicitado → mode={mode}, limit={limit}, lotes={lotes or 'todos'}, filtros={date_filter or 'nenhum'}")

        # --- Executa em background para evitar timeout
        Thread(
            target=lambda: baixar_output(
                nsa_hint="000",
                lotes=lotes,
                limit=limit,
                mode=mode,
                date_filter=date_filter
            ),
            daemon=True
        ).start()

        return jsonify({
            "status": "started",
            "msg": f"Pull disparado em background (mode={mode}, limit={limit}, filtros={date_filter or 'nenhum'})"
        }), 202

    except Exception as e:
        log(f"❌ Erro no pull: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500

# =========================================================
# 📦 Download completo de um lote NSA (ZIP)
# =========================================================
@agente_bp.route("/download-nsa/<nsa_id>", methods=["GET"])
def download_nsa(nsa_id):
    """
    Gera e retorna um ZIP com todos os arquivos do lote NSA informado.
    Exemplo: GET /api/agente/download-nsa/066
    """
    from flask import send_file
    import zipfile
    import io
    from datetime import datetime

    try:
        base_output = os.getenv("AGENTE_OUTPUT_DIR") or "/home/site/azurefiles/output"
        lote_dir = os.path.join(base_output, f"NSA_{nsa_id}")

        if not os.path.exists(lote_dir):
            log(f"⚠️ Lote NSA_{nsa_id} não encontrado em {base_output}")
            return jsonify({"ok": False, "msg": f"Lote NSA_{nsa_id} não encontrado."}), 404

        log(f"📦 Gerando ZIP completo para o lote NSA_{nsa_id}...")

        # Cria ZIP em memória
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(lote_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, lote_dir)
                    zipf.write(file_path, arcname)
        zip_buffer.seek(0)

        zip_name = f"NSA_{nsa_id}_{datetime.now().strftime('%d%m%Y_%H%M%S')}.zip"
        log(f"✅ ZIP gerado com sucesso: {zip_name}")

        return send_file(
            zip_buffer,
            as_attachment=True,
            download_name=zip_name,
            mimetype="application/zip"
        )

    except Exception as e:
        log(f"❌ Erro ao gerar ZIP para NSA_{nsa_id}: {e}")
        return jsonify({"ok": False, "msg": str(e)}), 500
