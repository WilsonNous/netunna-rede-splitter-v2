import os, sys
import hmac
import json
import hashlib
import secrets
import shutil
from flask import Flask, request, jsonify, render_template, send_from_directory, send_file
from werkzeug.utils import secure_filename
import csv, io, zipfile
from datetime import datetime
import pytz
from urllib.parse import quote

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
CLIENTS_DIR = os.path.join(BASE_DIR, "clientes")
DEFAULT_CLIENT_ID = os.getenv("DEFAULT_CLIENT_ID", "ventuno").strip().lower() or "ventuno"
LEGACY_CLIENT_ID = os.getenv("LEGACY_CLIENT_ID", DEFAULT_CLIENT_ID).strip().lower() or DEFAULT_CLIENT_ID
for d in [INPUT_DIR, OUTPUT_DIR, ERROR_DIR, LOG_DIR, CLIENTS_DIR]:
    os.makedirs(d, exist_ok=True)

def _client_paths(cliente):
    cliente = _safe_cliente(cliente)
    root = os.path.join(CLIENTS_DIR, cliente)
    paths = {
        "root": root,
        "input": os.path.join(root, "input"),
        "output": os.path.join(root, "output"),
        "erro": os.path.join(root, "erro"),
    }
    for path in paths.values():
        os.makedirs(path, exist_ok=True)
    return paths

print("📂 Diretórios configurados (persistentes):")
for name, path in {
    "INPUT_DIR": INPUT_DIR,
    "OUTPUT_DIR": OUTPUT_DIR,
    "ERROR_DIR": ERROR_DIR,
    "LOG_DIR": LOG_DIR,
    "CLIENTS_DIR": CLIENTS_DIR,
}.items():
    print(f"   {name} = {path}")

# ✅ Timezone Brasil
TZ_BR = pytz.timezone("America/Sao_Paulo")

# ==============================
# API v1: autenticação / automação
# ==============================
def _api_v1_authorized():
    """Valida Authorization: Bearer ou X-API-Key."""
    expected = (os.getenv("SPLITTER_API_KEY") or "").strip()
    if not expected:
        return False

    bearer = request.headers.get("Authorization", "")
    supplied = ""
    if bearer.lower().startswith("bearer "):
        supplied = bearer[7:].strip()
    if not supplied:
        supplied = request.headers.get("X-API-Key", "").strip()

    return bool(supplied) and hmac.compare_digest(supplied, expected)


def _require_api_v1_key():
    if not _api_v1_authorized():
        return jsonify({"ok": False, "erro": "Não autorizado."}), 401
    return None


def _safe_cliente(value):
    """Identificador interno do cliente, sem amarrar o Splitter a um cliente específico."""
    raw = (value or "default").strip().lower()
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "-" for ch in raw)
    safe = safe.strip("-_")
    return safe[:60] or "default"


def _sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _new_batch_id():
    stamp = datetime.now(TZ_BR).strftime("%Y%m%d_%H%M%S")
    return f"{stamp}_{secrets.token_hex(4)}"


def _batch_root(cliente, batch_id):
    return os.path.join(OUTPUT_DIR, "_api_batches", cliente, batch_id)


def _batch_input_root(cliente, batch_id):
    return os.path.join(INPUT_DIR, "_api_batches", cliente, batch_id)


def _batch_error_root(cliente, batch_id):
    return os.path.join(ERROR_DIR, "_api_batches", cliente, batch_id)


def _manifest_path(cliente, batch_id):
    return os.path.join(_batch_root(cliente, batch_id), "manifest.json")


def _load_manifest(cliente, batch_id):
    path = _manifest_path(cliente, batch_id)
    if not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _files_metadata(paths, batch_root):
    items = []
    for path in paths:
        path = os.path.abspath(str(path))
        if not os.path.isfile(path):
            continue
        # Garante que o arquivo pertença ao batch atual.
        if os.path.commonpath([path, os.path.abspath(batch_root)]) != os.path.abspath(batch_root):
            continue
        items.append({
            "nome": os.path.basename(path),
            "relativo": os.path.relpath(path, batch_root).replace(os.sep, "/"),
            "tamanho": os.path.getsize(path),
            "sha256": _sha256_file(path),
        })
    return items


def _copy_atomic_if_safe(src, dst):
    """
    Publica um arquivo sem sobrescrever silenciosamente conteúdo diferente.

    Retornos:
      "copied"   -> arquivo novo publicado
      "existing" -> destino já existia com o mesmo SHA-256
    """
    src = os.path.abspath(src)
    dst = os.path.abspath(dst)
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    if os.path.exists(dst):
        if not os.path.isfile(dst):
            raise RuntimeError(f"Destino existe e não é arquivo: {dst}")
        if _sha256_file(src) == _sha256_file(dst):
            return "existing"
        raise RuntimeError(
            f"Conflito de publicação: já existe arquivo com mesmo nome e conteúdo diferente: {dst}"
        )

    tmp = f"{dst}.tmp_{secrets.token_hex(4)}"
    try:
        shutil.copy2(src, tmp)
        if _sha256_file(src) != _sha256_file(tmp):
            raise RuntimeError(f"Falha de integridade ao publicar: {os.path.basename(src)}")
        os.replace(tmp, dst)
        return "copied"
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _publish_batch_to_client(cliente, arquivo_mae, gerados, lote, batch_root):
    """
    Publica o resultado aprovado do batch na árvore oficial lida pelo painel:

      clientes/<cliente>/input/<arquivo_mae>
      clientes/<cliente>/output/NSA_xxx/<filhos>

    O batch técnico permanece intacto em output/_api_batches.
    Antes de copiar qualquer item, faz preflight de conflitos para evitar
    publicação parcial por colisão de nomes.
    """
    cpaths = _client_paths(cliente)
    if not lote:
        raise RuntimeError("Não foi possível publicar: lote/NSA não identificado.")

    destino_lote = os.path.join(cpaths["output"], lote)
    os.makedirs(destino_lote, exist_ok=True)

    itens = []

    # Arquivo mãe
    mae_dst = os.path.join(cpaths["input"], os.path.basename(arquivo_mae))
    itens.append((os.path.abspath(arquivo_mae), os.path.abspath(mae_dst), "mae"))

    # Filhos: apenas arquivos pertencentes ao batch atual.
    batch_abs = os.path.abspath(batch_root)
    for src in gerados:
        src_abs = os.path.abspath(str(src))
        if not os.path.isfile(src_abs):
            continue
        if os.path.commonpath([src_abs, batch_abs]) != batch_abs:
            raise RuntimeError(f"Arquivo filho fora do batch atual: {src_abs}")
        dst = os.path.join(destino_lote, os.path.basename(src_abs))
        itens.append((src_abs, os.path.abspath(dst), "filho"))

    # Preflight: verifica todos os conflitos antes de copiar qualquer arquivo.
    for src, dst, _tipo in itens:
        if os.path.exists(dst):
            if not os.path.isfile(dst):
                raise RuntimeError(f"Destino existe e não é arquivo: {dst}")
            if _sha256_file(src) != _sha256_file(dst):
                raise RuntimeError(
                    f"Conflito de publicação: já existe arquivo com mesmo nome e conteúdo diferente: {dst}"
                )

    copiados = 0
    existentes = 0
    for src, dst, _tipo in itens:
        status = _copy_atomic_if_safe(src, dst)
        if status == "copied":
            copiados += 1
        else:
            existentes += 1

    return {
        "ok": True,
        "cliente": cliente,
        "input_dir": cpaths["input"],
        "output_dir": destino_lote,
        "itens_total": len(itens),
        "copiados": copiados,
        "ja_existentes_mesmo_hash": existentes,
        "arquivo_mae_publicado": os.path.basename(arquivo_mae),
        "filhos_publicados": max(0, len(itens) - 1),
    }

def _nsa_summary(cliente, nsa):
    """
    Retorna a visão consolidada de um NSA publicado na árvore oficial do cliente.

    Exemplo:
      clientes/ventuno/output/NSA_367/

    Classifica os arquivos por tipo:
      EEVC
      EEFI
      EEVD
    """
    cliente = _safe_cliente(cliente)

    nsa = str(nsa or "").strip()
    if not nsa.isdigit():
        raise ValueError("NSA inválido.")

    # Normaliza para os últimos 3 dígitos.
    nsa = nsa[-3:].zfill(3)

    cpaths = _client_paths(cliente)
    lote = f"NSA_{nsa}"
    pasta_nsa = os.path.join(cpaths["output"], lote)

    if not os.path.isdir(pasta_nsa):
        return None

    arquivos = []

    tipos = {
        "EEVC": 0,
        "EEFI": 0,
        "EEVD": 0,
        "OUTROS": 0,
    }

    for nome in sorted(os.listdir(pasta_nsa)):
        path = os.path.join(pasta_nsa, nome)

        if not os.path.isfile(path):
            continue

        nome_upper = nome.upper()

        if "_EEVC" in nome_upper:
            tipo = "EEVC"
        elif "_EEFI" in nome_upper:
            tipo = "EEFI"
        elif "_EEVD" in nome_upper:
            tipo = "EEVD"
        else:
            tipo = "OUTROS"

        tipos[tipo] += 1

        arquivos.append({
            "nome": nome,
            "tipo": tipo,
            "tamanho": os.path.getsize(path),
            "sha256": _sha256_file(path),
        })

    tipos_presentes = [
        tipo
        for tipo in ("EEVC", "EEFI", "EEVD")
        if tipos[tipo] > 0
    ]

    # Para a regra atual da VENTUNO consideramos completo quando
    # os três tipos foram publicados.
    #
    # Depois podemos transformar isso em configuração por cliente.
    tipos_esperados = {"EEVC", "EEFI", "EEVD"}

    pronto_para_smedi = (
        set(tipos_presentes) == tipos_esperados
        and len(arquivos) > 0
    )

    return {
        "cliente": cliente,
        "nsa": nsa,
        "lote": lote,
        "pasta": pasta_nsa,

        "tipos": {
            "EEVC": tipos["EEVC"],
            "EEFI": tipos["EEFI"],
            "EEVD": tipos["EEVD"],
            "OUTROS": tipos["OUTROS"],
        },

        "tipos_presentes": tipos_presentes,
        "quantidade": len(arquivos),

        "pronto_para_smedi": pronto_para_smedi,

        "arquivos": arquivos,
    }

@app.route("/api/v1/health", methods=["GET"])
def api_v1_health():
    return jsonify({
        "ok": True,
        "service": "Netunna REDE Splitter",
        "api": "v1",
        "status": "online"
    }), 200


@app.route("/api/v1/upload", methods=["POST"])
def api_v1_upload():
    """
    API máquina-a-máquina.

    Campos multipart:
      file     -> arquivo mãe da REDE (obrigatório)
      cliente  -> identificador lógico, ex.: ventuno (opcional)

    Cada upload recebe um batch_id exclusivo. Assim, NSA repetido e clientes
    diferentes nunca compartilham a mesma pasta de processamento.
    """
    denied = _require_api_v1_key()
    if denied:
        return denied

    if "file" not in request.files:
        return jsonify({"ok": False, "erro": "Nenhum arquivo enviado no campo 'file'."}), 400

    uploaded = request.files["file"]
    filename = secure_filename(uploaded.filename or "")
    if not filename:
        return jsonify({"ok": False, "erro": "Nome de arquivo vazio ou inválido."}), 400

    cliente = _safe_cliente(request.form.get("cliente"))
    batch_id = _new_batch_id()
    input_root = _batch_input_root(cliente, batch_id)
    batch_root = _batch_root(cliente, batch_id)
    error_root = _batch_error_root(cliente, batch_id)
    os.makedirs(input_root, exist_ok=True)
    os.makedirs(batch_root, exist_ok=True)
    os.makedirs(error_root, exist_ok=True)

    save_path = os.path.join(input_root, filename)
    uploaded.save(save_path)
    print(f"📤 [API v1] cliente={cliente} batch={batch_id} arquivo={filename}")

    try:
        resultado = process_file(save_path, batch_root, error_root)
        if not isinstance(resultado, dict):
            return jsonify({"ok": False, "erro": "Resposta inválida do processador."}), 500

        if resultado.get("status") == "ERRO" or resultado.get("erro"):
            return jsonify({
                "ok": False,
                "cliente": cliente,
                "batch_id": batch_id,
                "arquivo_mae": filename,
                "erro": resultado.get("erro") or resultado.get("detalhe") or "Falha no processamento.",
                "resultado": resultado
            }), 422

        tipo = resultado.get("tipo")
        nsa = str(resultado.get("nsa") or "").strip()
        lote = f"NSA_{nsa}" if nsa else None
        gerados = [str(x) for x in (resultado.get("gerados") or [])]

        # Se o processador não retornar caminhos, inspeciona apenas o batch atual.
        if not gerados and lote:
            pasta = os.path.join(batch_root, lote)
            if os.path.isdir(pasta):
                gerados = [
                    os.path.join(pasta, f) for f in sorted(os.listdir(pasta))
                    if os.path.isfile(os.path.join(pasta, f))
                ]

        integridade = None
        if tipo in ("EEVC", "EEVD", "EEFI") and nsa:
            pasta_filhos = os.path.join(batch_root, lote)
            try:
                integridade = processar_integridade(tipo, save_path, pasta_filhos)
                print(f"✅ [API v1] Integridade batch={batch_id}: {integridade.get('mensagem')}")
            except Exception as ve:
                integridade = {"ok": False, "mensagem": str(ve)}
                print(f"⚠️ [API v1] Erro na integridade batch={batch_id}: {ve}")

        arquivos = _files_metadata(gerados, batch_root)

        # Só publica na árvore oficial do cliente depois que o processamento e
        # a integridade estiverem aprovados. Essa é a árvore lida pelo painel.
        if integridade is not None and integridade.get("ok") is False:
            return jsonify({
                "ok": False,
                "cliente": cliente,
                "batch_id": batch_id,
                "arquivo_mae": filename,
                "erro": integridade.get("mensagem") or "Falha na validação de integridade.",
                "resultado": resultado,
                "integridade": integridade
            }), 422

        publicacao = _publish_batch_to_client(
            cliente=cliente,
            arquivo_mae=save_path,
            gerados=gerados,
            lote=lote,
            batch_root=batch_root,
        )
        print(
            f"📚 [API v1] Publicado no painel cliente={cliente} lote={lote} "
            f"filhos={publicacao.get('filhos_publicados')} "
            f"copiados={publicacao.get('copiados')}"
        )

        manifest = {
            "api": "v1",
            "cliente": cliente,
            "batch_id": batch_id,
            "arquivo_mae": filename,
            "arquivo_mae_sha256": _sha256_file(save_path),
            "tipo": tipo,
            "nsa": nsa,
            "lote": lote,
            "criado_em": datetime.now(TZ_BR).isoformat(),
            "quantidade_gerados": len(arquivos),
            "arquivos": arquivos,
            "integridade": integridade,
            "publicacao": publicacao,
            "status": "OK",
        }
        with open(_manifest_path(cliente, batch_id), "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

        return jsonify({
            "ok": True,
            "mensagem": "Arquivo recebido e processado.",
            **manifest,
            "files_url": f"/api/v1/batches/{batch_id}/files?cliente={cliente}",
            "download_url": f"/api/v1/batches/{batch_id}/download?cliente={cliente}",
            "resultado": resultado
        }), 200

    except Exception as e:
        print(f"❌ [API v1] cliente={cliente} batch={batch_id}: {e}")
        return jsonify({
            "ok": False,
            "cliente": cliente,
            "batch_id": batch_id,
            "arquivo_mae": filename,
            "erro": str(e)
        }), 500


@app.route("/api/v1/batches/<batch_id>/files", methods=["GET"])
def api_v1_batch_files(batch_id):
    denied = _require_api_v1_key()
    if denied:
        return denied
    cliente = _safe_cliente(request.args.get("cliente"))
    manifest = _load_manifest(cliente, batch_id)
    if not manifest:
        return jsonify({"ok": False, "erro": "Batch não encontrado."}), 404
    return jsonify({"ok": True, **manifest}), 200


@app.route("/api/v1/batches/<batch_id>/download", methods=["GET"])
def api_v1_batch_download(batch_id):
    """Baixa somente os filhos daquele batch, nunca todo o OUTPUT_DIR."""
    denied = _require_api_v1_key()
    if denied:
        return denied
    cliente = _safe_cliente(request.args.get("cliente"))
    manifest = _load_manifest(cliente, batch_id)
    if not manifest:
        return jsonify({"ok": False, "erro": "Batch não encontrado."}), 404

    batch_root = _batch_root(cliente, batch_id)
    memory_file = io.BytesIO()
    count = 0
    with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zipf:
        for item in manifest.get("arquivos", []):
            rel = item.get("relativo") or ""
            file_path = os.path.abspath(os.path.join(batch_root, rel))
            if os.path.commonpath([file_path, os.path.abspath(batch_root)]) != os.path.abspath(batch_root):
                continue
            if os.path.isfile(file_path):
                zipf.write(file_path, os.path.basename(file_path))
                count += 1

    if count == 0:
        return jsonify({"ok": False, "erro": "Nenhum arquivo filho disponível neste batch."}), 404

    memory_file.seek(0)
    zip_name = f"{cliente}_{manifest.get('lote') or 'lote'}_{batch_id}.zip"
    return send_file(
        memory_file,
        mimetype="application/zip",
        as_attachment=True,
        download_name=zip_name
    )

@app.route(
    "/api/v1/clientes/<cliente>/nsas/<nsa>",
    methods=["GET"]
)
def api_v1_nsa_status(cliente, nsa):
    """
    Consulta o NSA consolidado publicado para um cliente.

    Exemplo:
      GET /api/v1/clientes/ventuno/nsas/367
    """
    denied = _require_api_v1_key()
    if denied:
        return denied

    cliente = _safe_cliente(cliente)

    try:
        resumo = _nsa_summary(cliente, nsa)

    except ValueError as e:
        return jsonify({
            "ok": False,
            "erro": str(e)
        }), 400

    if resumo is None:
        return jsonify({
            "ok": False,
            "cliente": cliente,
            "nsa": nsa,
            "erro": "NSA não encontrado."
        }), 404

    resumo["download_url"] = (
        f"/api/v1/clientes/{cliente}/nsas/"
        f"{resumo['nsa']}/download"
    )

    return jsonify({
        "ok": True,
        **resumo
    }), 200

@app.route(
    "/api/v1/clientes/<cliente>/nsas/<nsa>/download",
    methods=["GET"]
)
def api_v1_nsa_download(cliente, nsa):
    """
    Baixa todos os arquivos filhos publicados de um NSA.

    Diferentemente do endpoint /batches/<batch_id>/download,
    este endpoint consolida EEVC + EEFI + EEVD.

    Exemplo:
      GET /api/v1/clientes/ventuno/nsas/367/download
    """
    denied = _require_api_v1_key()
    if denied:
        return denied

    cliente = _safe_cliente(cliente)

    try:
        resumo = _nsa_summary(cliente, nsa)

    except ValueError as e:
        return jsonify({
            "ok": False,
            "erro": str(e)
        }), 400

    if resumo is None:
        return jsonify({
            "ok": False,
            "cliente": cliente,
            "nsa": nsa,
            "erro": "NSA não encontrado."
        }), 404

    if resumo["quantidade"] == 0:
        return jsonify({
            "ok": False,
            "cliente": cliente,
            "nsa": resumo["nsa"],
            "erro": "Nenhum arquivo disponível neste NSA."
        }), 404

    # Segurança operacional:
    # por padrão só libera quando VC + FI + VD estiverem presentes.
    #
    # Para diagnóstico manual:
    # ?permitir_incompleto=1
    permitir_incompleto = (
        request.args.get("permitir_incompleto", "0") == "1"
    )

    if (
        not resumo["pronto_para_smedi"]
        and not permitir_incompleto
    ):
        return jsonify({
            "ok": False,
            "cliente": cliente,
            "nsa": resumo["nsa"],
            "erro": "NSA ainda não está completo para envio ao SMEDI.",
            "tipos": resumo["tipos"],
            "quantidade": resumo["quantidade"],
            "pronto_para_smedi": False,
        }), 409

    pasta_nsa = resumo["pasta"]

    memory_file = io.BytesIO()
    total_zip = 0

    with zipfile.ZipFile(
        memory_file,
        "w",
        zipfile.ZIP_DEFLATED
    ) as zipf:

        for item in resumo["arquivos"]:
            nome = item["nome"]

            file_path = os.path.abspath(
                os.path.join(pasta_nsa, nome)
            )

            # Segurança contra path traversal.
            if os.path.commonpath([
                file_path,
                os.path.abspath(pasta_nsa)
            ]) != os.path.abspath(pasta_nsa):
                continue

            if not os.path.isfile(file_path):
                continue

            # Confere novamente o hash antes de entregar.
            sha_atual = _sha256_file(file_path)

            if sha_atual != item["sha256"]:
                return jsonify({
                    "ok": False,
                    "erro": (
                        "Arquivo alterado durante a geração "
                        f"do ZIP: {nome}"
                    )
                }), 500

            # ZIP sem subpastas:
            # exatamente os arquivos que serão extraídos para SMEDI.
            zipf.write(
                file_path,
                arcname=nome
            )

            total_zip += 1

    if total_zip == 0:
        return jsonify({
            "ok": False,
            "erro": "Nenhum arquivo disponível para compactação."
        }), 404

    memory_file.seek(0)

    zip_name = (
        f"{cliente}_NSA_{resumo['nsa']}_"
        f"{datetime.now(TZ_BR).strftime('%Y%m%d_%H%M%S')}.zip"
    )

    print(
        f"📦 [API v1] Download NSA consolidado "
        f"cliente={cliente} "
        f"nsa={resumo['nsa']} "
        f"EEVC={resumo['tipos']['EEVC']} "
        f"EEFI={resumo['tipos']['EEFI']} "
        f"EEVD={resumo['tipos']['EEVD']} "
        f"total={total_zip}"
    )

    response = send_file(
        memory_file,
        mimetype="application/zip",
        as_attachment=True,
        download_name=zip_name
    )

    # Metadados úteis para o futuro agente Windows.
    response.headers["X-Netunna-Cliente"] = cliente
    response.headers["X-Netunna-NSA"] = resumo["nsa"]
    response.headers["X-Netunna-EEVC"] = str(
        resumo["tipos"]["EEVC"]
    )
    response.headers["X-Netunna-EEFI"] = str(
        resumo["tipos"]["EEFI"]
    )
    response.headers["X-Netunna-EEVD"] = str(
        resumo["tipos"]["EEVD"]
    )
    response.headers["X-Netunna-Total-Arquivos"] = str(
        total_zip
    )

    return response

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

    cliente = _safe_cliente(data.get("cliente") or DEFAULT_CLIENT_ID)
    cpaths = _client_paths(cliente)
    arquivo_path = os.path.join(cpaths["input"], arquivo_mae)
    pasta_filhos = os.path.join(cpaths["output"], f"NSA_{nsa}")

    # Compatibilidade temporária com o legado anterior à separação por cliente.
    if not os.path.exists(arquivo_path) and cliente == LEGACY_CLIENT_ID:
        arquivo_path = os.path.join(INPUT_DIR, arquivo_mae)
    if not os.path.exists(pasta_filhos) and cliente == LEGACY_CLIENT_ID:
        pasta_filhos = os.path.join(OUTPUT_DIR, f"NSA_{nsa}")

    if not os.path.exists(arquivo_path):
        return jsonify({"ok": False, "mensagem": f"Arquivo mãe não encontrado: {arquivo_mae}", "cliente": cliente}), 404

    if not os.path.exists(pasta_filhos):
        return jsonify({"ok": False, "mensagem": f"Pasta de filhos não encontrada: {pasta_filhos}", "cliente": cliente}), 404

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
    """Baixa arquivo individual com suporte ao legado e à estrutura multi-cliente."""
    try:
        cliente_raw = request.args.get("cliente")
        search_roots = []

        if cliente_raw:
            cliente = _safe_cliente(cliente_raw)
            search_roots.append(_client_paths(cliente)["output"])
            if cliente == LEGACY_CLIENT_ID:
                search_roots.append(OUTPUT_DIR)
        else:
            # Compatibilidade com links antigos: clientes primeiro, legado por último.
            if os.path.isdir(CLIENTS_DIR):
                for nome in sorted(os.listdir(CLIENTS_DIR)):
                    croot = os.path.join(CLIENTS_DIR, nome, "output")
                    if os.path.isdir(croot):
                        search_roots.append(croot)
            search_roots.append(OUTPUT_DIR)

        for base in search_roots:
            direct_path = os.path.join(base, filename)
            if os.path.isfile(direct_path):
                return send_from_directory(base, filename, as_attachment=True)

            for root, dirs, files in os.walk(base):
                # Nunca expõe batches da API por esta rota legada.
                dirs[:] = [d for d in dirs if d != "_api_batches"]
                if filename in files:
                    print(f"⬇️ Download localizado: {filename} em {root}")
                    return send_from_directory(root, filename, as_attachment=True)

        print(f"⚠️ Download falhou — arquivo não encontrado: {filename}")
        return jsonify({"erro": f"Arquivo '{filename}' não encontrado."}), 404

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
    """Lista legado + estrutura multi-cliente sem quebrar o painel durante a migração."""
    result = {"input": [], "output": [], "clientes": []}
    seen_input = set()
    seen_output = set()

    def add_input(base, cliente, origem):
        if not os.path.isdir(base):
            return
        for f in sorted(os.listdir(base)):
            fpath = os.path.join(base, f)
            if not os.path.isfile(fpath):
                continue
            key = (cliente, f, os.path.getsize(fpath))
            if key in seen_input:
                continue
            seen_input.add(key)
            dt_brasil = datetime.fromtimestamp(os.path.getmtime(fpath), TZ_BR)
            result["input"].append({
                "nome": f,
                "cliente": cliente,
                "origem": origem,
                "data_hora": dt_brasil.strftime("%d/%m/%Y %H:%M:%S")
            })

    def add_output(base, cliente, origem):
        if not os.path.isdir(base):
            return
        for root, dirs, files in os.walk(base):
            dirs[:] = [d for d in dirs if d != "_api_batches"]
            if not files:
                continue
            lote = os.path.basename(root)
            if not lote.startswith("NSA_"):
                continue
            for f in sorted(files):
                fpath = os.path.join(root, f)
                if not os.path.isfile(fpath):
                    continue
                key = (cliente, lote, f, os.path.getsize(fpath))
                if key in seen_output:
                    continue
                seen_output.add(key)
                dt_brasil = datetime.fromtimestamp(os.path.getmtime(fpath), TZ_BR)
                result["output"].append({
                    "nome": f,
                    "cliente": cliente,
                    "origem": origem,
                    "lote": lote,
                    "data_hora": dt_brasil.strftime("%d/%m/%Y %H:%M:%S"),
                    "download_url": f"/api/download/{quote(f, safe='')}?cliente={quote(cliente, safe='')}"
                })

    # 1) Estrutura nova por cliente.
    if os.path.isdir(CLIENTS_DIR):
        for cliente in sorted(os.listdir(CLIENTS_DIR)):
            croot = os.path.join(CLIENTS_DIR, cliente)
            if not os.path.isdir(croot):
                continue
            result["clientes"].append(cliente)
            add_input(os.path.join(croot, "input"), cliente, "cliente")
            add_output(os.path.join(croot, "output"), cliente, "cliente")

    # 2) Legado: durante a transição ele pertence ao cliente definido em LEGACY_CLIENT_ID.
    if LEGACY_CLIENT_ID not in result["clientes"]:
        result["clientes"].append(LEGACY_CLIENT_ID)
    add_input(INPUT_DIR, LEGACY_CLIENT_ID, "legado")
    add_output(OUTPUT_DIR, LEGACY_CLIENT_ID, "legado")

    result["input"].sort(key=lambda x: (x.get("cliente", ""), x.get("data_hora", ""), x.get("nome", "")))
    result["output"].sort(key=lambda x: (x.get("cliente", ""), x.get("lote", ""), x.get("nome", "")))
    return jsonify(result)

# ==============================
# Execução
# ==============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

