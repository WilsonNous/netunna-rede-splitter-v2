import os, sys
from flask import Flask, request, jsonify, render_template, send_from_directory, send_file
import csv, io, zipfile
from datetime import datetime
import pytz

# --- Ajuste de path para o Azure ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR)

# --- Imports locais ---
try:
    from splitter_core_v3 import process_file, LOG_PATH
except ModuleNotFoundError:
    from modules.splitter_core_v3 import process_file, LOG_PATH

from modules.processador_integridade import processar_integridade

# --- Inicialização do Flask ---
app = Flask(__name__, template_folder=os.path.join(BASE_DIR, 'templates'))

# ✅ Registrar Blueprint do Agente
from agente.agente_routes import agente_bp
app.register_blueprint(agente_bp, url_prefix="/api/agente")

# --- Diretórios persistentes (Azure Files) ---
BASE_DIR = os.getenv("BASE_DIR", "/home/site/azurefiles")
INPUT_DIR  = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
ERROR_DIR  = os.path.join(BASE_DIR, "erro")
LOG_DIR    = os.path.join(BASE_DIR, "logs")
for d in [INPUT_DIR, OUTPUT_DIR, ERROR_DIR, LOG_DIR]:
    os.makedirs(d, exist_ok=True)

print("📂 Diretórios configurados (persistentes):")
for name, path in {
    "INPUT_DIR": INPUT_DIR,
    "OUTPUT_DIR": OUTPUT_DIR,
    "ERROR_DIR": ERROR_DIR,
    "LOG_DIR": LOG_DIR,
}.items():
    print(f"   {name} = {path}")

# ✅ Timezone Brasil
TZ_BR = pytz.timezone("America/Sao_Paulo")

# ==============================
# Página principal
# ==============================
@app.route("/")
def home():
    files_input = os.listdir(INPUT_DIR)
    files_output = os.listdir(OUTPUT_DIR)
    logs = []
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            logs = list(reader)[-50:]
    return render_template("index.html", files_input=files_input, files_output=files_output, logs=logs)

# ==============================
# API: Upload e processamento automático
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

    try:
        resultado = process_file(save_path, OUTPUT_DIR, ERROR_DIR)
        print(f"✅ Processado automaticamente: {file.filename}")

        tipo = resultado.get("tipo")
        nsa = resultado.get("nsa") or "000"
        arquivo_mae = save_path
        if tipo in ("EEVC", "EEVD", "EEFI"):
            try:
                valid = processar_integridade(tipo, arquivo_mae, OUTPUT_DIR)
                print(f"✅ Validação automática concluída: {valid.get('mensagem')}")
            except Exception as ve:
                print(f"⚠️ Erro na validação automática: {ve}")

        return jsonify({
            "mensagem": f"Arquivo {file.filename} recebido e processado automaticamente.",
            "resultado": resultado
        }), 200

    except Exception as e:
        print(f"❌ Erro ao processar {file.filename}: {e}")
        return jsonify({"erro": str(e)}), 500

# ==============================
# API: Processar manualmente
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
        print(f"✅ Processado manualmente: {filename}")

        tipo = resultado.get("tipo")
        nsa = resultado.get("nsa") or "000"
        arquivo_mae = path_in
        if tipo in ("EEVC", "EEVD", "EEFI"):
            try:
                valid = processar_integridade(tipo, arquivo_mae, OUTPUT_DIR)
                print(f"✅ Validação automática concluída: {valid.get('mensagem')}")
            except Exception as ve:
                print(f"⚠️ Erro na validação automática: {ve}")

        return jsonify({"mensagem": "Processado", "resultado": resultado}), 200
    except Exception as e:
        return jsonify({"erro": str(e)}), 500

# ==============================
# API: Validação de Integridade
# ==============================
@app.route("/api/validate", methods=["POST"])
def api_validate():
    data = request.get_json()
    tipo = data.get("tipo")
    arquivo_mae = data.get("arquivo_mae")
    nsa = data.get("nsa")

    if not all([tipo, arquivo_mae, nsa]):
        return jsonify({"ok": False, "mensagem": "Campos obrigatórios: tipo, arquivo_mae, nsa"}), 400

    arquivo_path = os.path.join(INPUT_DIR, arquivo_mae)
    pasta_filhos = os.path.join(OUTPUT_DIR, f"NSA_{nsa}")

    if not os.path.exists(arquivo_path):
        return jsonify({"ok": False, "mensagem": f"Arquivo mãe não encontrado: {arquivo_mae}"}), 404

    if not os.path.exists(pasta_filhos):
        return jsonify({"ok": False, "mensagem": f"Pasta de filhos não encontrada: {pasta_filhos}"}), 404

    try:
        resultado = processar_integridade(tipo, arquivo_path, pasta_filhos)
        return jsonify(resultado), 200
    except Exception as e:
        print(f"❌ Erro na validação de integridade: {e}")
        return jsonify({"ok": False, "mensagem": str(e)}), 500

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
# ✅ API: Download individual (corrigida)
# ==============================
@app.route("/api/download/<filename>", methods=["GET"])
def download_file(filename):
    """
    Permite baixar qualquer arquivo gerado, mesmo dentro das subpastas (NSA_xxx).
    """
    try:
        # 1️⃣ Verifica se está diretamente na raiz do output
        direct_path = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(direct_path):
            return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)

        # 2️⃣ Busca recursivamente nas subpastas
        for root, _, files in os.walk(OUTPUT_DIR):
            if filename in files:
                print(f"⬇️ Download localizado: {filename} em {root}")
                return send_from_directory(root, filename, as_attachment=True)

        # 3️⃣ Se não encontrar
        print(f"⚠️ Download falhou — arquivo não encontrado: {filename}")
        return jsonify({
            "erro": f"Arquivo '{filename}' não encontrado em {OUTPUT_DIR} ou subpastas."
        }), 404

    except Exception as e:
        print(f"❌ Erro durante download de {filename}: {e}")
        return jsonify({"erro": str(e)}), 500


# ==============================
# ✅ API: Download ZIP completo (corrigida)
# ==============================
@app.route("/api/download-all", methods=["GET"])
def api_download_all():
    """
    Compacta todos os arquivos gerados (inclusive os que estão dentro das pastas NSA_xxx)
    em um único ZIP para download.
    """
    try:
        memory_file = io.BytesIO()
        total_arquivos = 0

        with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(OUTPUT_DIR):
                for f in files:
                    file_path = os.path.join(root, f)
                    arcname = os.path.relpath(file_path, OUTPUT_DIR)
                    zipf.write(file_path, arcname)
                    total_arquivos += 1

        if total_arquivos == 0:
            print("⚠️ Nenhum arquivo encontrado para compactar.")
            return jsonify({"mensagem": "Nenhum arquivo encontrado no diretório de saída."}), 404

        memory_file.seek(0)
        zip_name = f"NetunnaSplitter_{datetime.now(TZ_BR).strftime('%Y%m%d_%H%M%S')}.zip"
        print(f"📦 ZIP gerado com {total_arquivos} arquivos → {zip_name}")

        return send_file(
            memory_file,
            mimetype="application/zip",
            as_attachment=True,
            download_name=zip_name
        )

    except Exception as e:
        print(f"❌ Erro ao gerar ZIP: {e}")
        return jsonify({"erro": str(e)}), 500


# ==============================
# ✅ API: Scan diretórios (corrigido)
# ==============================
@app.route("/api/scan", methods=["GET"])
def api_scan():
    """Lista arquivos de entrada e saída agrupados por subpasta NSA_xxx."""
    result = {"input": [], "output": []}

    # INPUT
    if os.path.exists(INPUT_DIR):
        for f in sorted(os.listdir(INPUT_DIR)):
            fpath = os.path.join(INPUT_DIR, f)
            if os.path.isfile(fpath):
                dt_brasil = datetime.fromtimestamp(os.path.getmtime(fpath), TZ_BR)
                result["input"].append({
                    "nome": f,
                    "data_hora": dt_brasil.strftime("%d/%m/%Y %H:%M:%S")
                })

    # OUTPUT
    if os.path.exists(OUTPUT_DIR):
        for root, dirs, files in os.walk(OUTPUT_DIR):
            if not files:
                continue
            lote = os.path.basename(root)
            if not lote.startswith("NSA_"):
                continue
            for f in sorted(files):
                fpath = os.path.join(root, f)
                dt_brasil = datetime.fromtimestamp(os.path.getmtime(fpath), TZ_BR)
                result["output"].append({
                    "nome": f,
                    "lote": lote,
                    "data_hora": dt_brasil.strftime("%d/%m/%Y %H:%M:%S")
                })

    return jsonify(result)

# ==============================
# Execução
# ==============================
if __name__ == "__main__":
    print("\n🔍 Rotas registradas no Flask:")
    for rule in app.url_map.iter_rules():
        print(f"  {rule.endpoint:30s} -> {rule}")
        
    app.run(host="0.0.0.0", port=10000)

