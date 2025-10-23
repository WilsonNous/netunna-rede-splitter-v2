# modules/agente_routes.py
"""
Rotas do Agente Netunna (v4)
Integradas ao Flask principal
"""

import os
from threading import Thread
from flask import Blueprint, jsonify, request, current_app

# 🔧 Importa componentes do pacote /agente
from agente.main_agente import main as executar_ciclo
from agente.utils import log, ler_ultimos_logs, ensure_dirs
from agente.downloader import baixar_output
from agente.uploader import upload_file  # ✅ faltava este import

agente_bp = Blueprint("agente", __name__)

def _resolve_input_dir() -> str:
    """Resolve o diretório de INPUT do agente, com fallback seguro."""
    base_dir = os.getenv("BASE_DIR") or current_app.root_path
    input_dir = os.getenv("AGENTE_INPUT_DIR") or os.path.join(base_dir, "input")
    os.makedirs(input_dir, exist_ok=True)
    return input_dir

@agente_bp.route("/run", methods=["POST"])
def run_agente():
    log("▶️ Execução remota solicitada via painel Splitter.")
    # garante estrutura básica antes de executar
    ensure_dirs()
    Thread(target=executar_ciclo, daemon=True).start()
    return jsonify({"status": "started", "msg": "Agente executando em background."})

@agente_bp.route("/status", methods=["GET"])
def status():
    logs = ler_ultimos_logs(40)
    return jsonify({"status": "ok", "logs": logs})

@agente_bp.route("/download", methods=["GET"])
def download():
    log("⬇️ Download remoto solicitado via painel Splitter.")
    Thread(target=baixar_output, daemon=True).start()
    return jsonify({"status": "started", "msg": "Download iniciado."})

# 🌟 NOVO: Upload via painel → salva no input do agente e envia ao Splitter
@agente_bp.route("/upload", methods=["POST"])
def upload_via_agente():
    """
    Aceita:
      - multipart com 'file' (um arquivo) ou 'files' / 'files[]' (múltiplos)
    Fluxo:
      1) Salva no AGENTE_INPUT_DIR
      2) Chama agente.uploader.upload_file(path) para cada arquivo
    Retorna JSON com sucesso/falha por arquivo.
    """
    input_dir = _resolve_input_dir()

    # coleta arquivos (aceita 'file' único ou lista 'files'/'files[]')
    files = []
    if "file" in request.files:
        files.append(request.files["file"])
    if "files" in request.files:
        files.extend(request.files.getlist("files"))
    if "files[]" in request.files:
        files.extend(request.files.getlist("files[]"))

    if not files:
        return jsonify({"ok": False, "mensagem": "Nenhum arquivo enviado (use 'file' ou 'files[]' no multipart)."}), 400

    resultados = []
    for f in files:
        filename = f.filename or "sem_nome"
        save_path = os.path.join(input_dir, filename)
        try:
            f.save(save_path)
            log(f"📥 Recebido via painel: {filename} → {save_path}")
            ok = upload_file(save_path)  # faz o POST para SPLITTER_API_UPLOAD e move p/ enviados
            resultados.append({"arquivo": filename, "salvo_em": save_path, "enviado": bool(ok)})
        except Exception as e:
            log(f"❌ Falha ao tratar upload '{filename}': {e}")
            resultados.append({"arquivo": filename, "erro": str(e), "enviado": False})

    # sinaliza sucesso geral se houver pelo menos um enviado com sucesso
    ok_geral = any(r.get("enviado") for r in resultados)
    return jsonify({"ok": ok_geral, "resultado": resultados})
