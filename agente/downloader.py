# agente/downloader.py
import os
import io
import zipfile
import hashlib
import shutil
import tempfile
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from agente.utils import log

# =========================================================
# 🔧 Configurações via .env
# =========================================================
DOWNLOAD_MODE         = (os.getenv("DOWNLOAD_MODE") or "zip").lower()   # zip | lease | direct
BASE_DIR              = Path(os.getenv("BASE_DIR") or ".")
LOCAL_RECEIVED        = BASE_DIR / (os.getenv("AGENTE_OUTPUT_DIR") or "recebidos")

# --- Modo ZIP (legado)
DOWNLOAD_URL_ZIP      = os.getenv("SPLITTER_API_DOWNLOAD")  # ex: /api/download-all (retorna application/zip)

# --- Modos pull (lease/direct) – endpoints do Splitter
SPLITTER_BASE_URL     = os.getenv("SPLITTER_BASE_URL")      # ex: https://splitter.yourdomain
SPLITTER_API_KEY      = os.getenv("SPLITTER_API_KEY", "")
HEADERS               = {"Authorization": f"Bearer {SPLITTER_API_KEY}"} if SPLITTER_API_KEY else {}
LEASE_TTL_SECONDS     = int(os.getenv("LEASE_TTL_SECONDS", "900"))
PULL_LIMIT            = int(os.getenv("PULL_LIMIT", "200"))
VERIFY_SHA256         = (os.getenv("VERIFY_SHA256", "true").lower() == "true")

# =========================================================
# 🧩 Utilitários
# =========================================================
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _download_stream_to(dest: Path, url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 300) -> None:
    _ensure_dir(dest.parent)
    with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            for chunk in r.iter_content(1024 * 1024):
                if chunk:
                    tmp.write(chunk)
            tmp_path = Path(tmp.name)
    shutil.move(str(tmp_path), str(dest))

# =========================================================
# 📦 MODO ZIP (LEGADO)
# =========================================================
def _baixar_zip_consolidado(nsa_hint: str = "000") -> Dict[str, Any]:
    """
    Baixa um ZIP consolidado do Splitter e extrai para uma pasta local.
    Compatível com a sua implementação atual.
    """
    _ensure_dir(LOCAL_RECEIVED)
    if not DOWNLOAD_URL_ZIP:
        raise RuntimeError("SPLITTER_API_DOWNLOAD não está definido no .env para o modo ZIP.")

    log("⬇️ (ZIP) Iniciando download do ZIP consolidado...")
    res = requests.get(DOWNLOAD_URL_ZIP, timeout=180)
    if res.status_code == 200 and "application/zip" in res.headers.get("Content-Type", ""):
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        zip_name = f"output_NSA_{nsa_hint}_{now}.zip"
        zip_path = LOCAL_RECEIVED / zip_name

        with open(zip_path, "wb") as f:
            f.write(res.content)

        log(f"📦 (ZIP) Download concluído → {zip_path}")
        extract_dir = LOCAL_RECEIVED / f"NSA_{nsa_hint}_{now}"
        _ensure_dir(extract_dir)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        log(f"📂 (ZIP) Arquivos extraídos em: {extract_dir}")
        return {"mode": "zip", "ok": True, "zip_path": str(zip_path), "extract_dir": str(extract_dir)}
    else:
        msg = f"⚠️ (ZIP) Falha no download ({res.status_code}) → {res.text[:200]}"
        log(msg)
        return {"mode": "zip", "ok": False, "error": msg}

# =========================================================
# 🔒 MODO LEASE (RECOMENDADO) — lease + confirm
# =========================================================
def _pull_lease(lotes: Optional[List[str]] = None, limit: Optional[int] = None) -> Dict[str, Any]:
    """
    1) POST /api/splitter/lease-files -> pega arquivos 'pending', marca 'leased' e retorna lista + SAS/URL
    2) Baixa cada arquivo para LOCAL_RECEIVED/<lote>/<nome> (valida tamanho/sha quando vier)
    3) POST /api/splitter/confirm-download com ok_ids / fail_ids
    """
    lotes = lotes or []
    limit = limit or PULL_LIMIT
    if not SPLITTER_BASE_URL:
        raise RuntimeError("SPLITTER_BASE_URL não definido no .env para modos pull.")

    lease_ep = f"{SPLITTER_BASE_URL}/api/splitter/lease-files"
    confirm_ep = f"{SPLITTER_BASE_URL}/api/splitter/confirm-download"

    log(f"🔒 (LEASE) Solicitando lease de até {limit} arquivos... lotes={lotes or 'todos'}")
    r = requests.post(lease_ep, json={"limit": limit, "lotes": lotes, "ttl_seconds": LEASE_TTL_SECONDS},
                      headers=HEADERS, timeout=60)
    r.raise_for_status()
    data = r.json()
    files = data.get("files", [])
    lease_id = data.get("lease_id")

    ok_ids, fail_ids, saved = [], [], []
    for f in files:
        file_id = f["id"]
        lote    = f.get("lote") or f.get("dir") or ""
        nome    = f["nome"]
        url     = f.get("sas_url") or f.get("url")
        sha     = f.get("sha256")
        size    = f.get("tamanho") or f.get("size")

        dest = LOCAL_RECEIVED / lote / nome
        try:
            _download_stream_to(dest, url, headers=HEADERS)

            if size and dest.stat().st_size != int(size):
                raise ValueError(f"tam divergente: esperado {size}, real {dest.stat().st_size}")

            if VERIFY_SHA256 and sha:
                got = _sha256_file(dest).lower()
                if got != sha.lower():
                    raise ValueError(f"sha256 divergente para {nome} ({got[:12]} != {sha[:12]})")

            ok_ids.append(file_id)
            saved.append(str(dest))
            log(f"✅ (LEASE) Baixado: {dest}")
        except Exception as e:
            if dest.exists():
                dest.unlink(missing_ok=True)
            fail_ids.append(file_id)
            log(f"❌ (LEASE) Falha em {lote}/{nome}: {e}")

    log(f"🔁 (LEASE) Confirmando download: ok={len(ok_ids)} fail={len(fail_ids)} (lease_id={lease_id})")
    rc = requests.post(confirm_ep, json={"lease_id": lease_id, "ok_ids": ok_ids, "fail_ids": fail_ids},
                       headers=HEADERS, timeout=60)
    rc.raise_for_status()

    return {
        "mode": "lease",
        "ok": len(fail_ids) == 0,
        "downloaded": len(ok_ids),
        "failed": len(fail_ids),
        "saved": saved,
        "lease_id": lease_id
    }

# =========================================================
# ⚡ MODO DIRECT (SIMPLES) — marca baixado ao responder
# =========================================================
def _pull_direct(lotes: Optional[List[str]] = None, limit: Optional[int] = None) -> Dict[str, Any]:
    """
    POST /api/splitter/pull-batch -> seleciona 'pending', já marca 'downloaded' e retorna lista com SAS/URL
    Risco: se cair durante o download, já ficou como baixado no Splitter.
    """
    lotes = lotes or []
    limit = limit or PULL_LIMIT
    if not SPLITTER_BASE_URL:
        raise RuntimeError("SPLITTER_BASE_URL não definido no .env para modos pull.")

    pull_ep = f"{SPLITTER_BASE_URL}/api/splitter/pull-batch"
    log(f"⚡ (DIRECT) Solicitando {limit} arquivos... lotes={lotes or 'todos'}")
    r = requests.post(pull_ep, json={"limit": limit, "lotes": lotes}, headers=HEADERS, timeout=60)
    r.raise_for_status()
    data = r.json()
    files = data.get("files", [])

    saved, failed = [], 0
    for f in files:
        lote = f.get("lote") or f.get("dir") or ""
        nome = f["nome"]
        url  = f.get("sas_url") or f.get("url")
        sha  = f.get("sha256")
        size = f.get("tamanho") or f.get("size")

        dest = LOCAL_RECEIVED / lote / nome
        try:
            _download_stream_to(dest, url, headers=HEADERS)
            if size and dest.stat().st_size != int(size):
                raise ValueError(f"tam divergente: esperado {size}, real {dest.stat().st_size}")
            if VERIFY_SHA256 and sha:
                got = _sha256_file(dest).lower()
                if got != sha.lower():
                    raise ValueError(f"sha256 divergente para {nome} ({got[:12]} != {sha[:12]})")
            saved.append(str(dest))
            log(f"✅ (DIRECT) Baixado: {dest}")
        except Exception as e:
            failed += 1
            if dest.exists():
                dest.unlink(missing_ok=True)
            log(f"❌ (DIRECT) Falha em {lote}/{nome}: {e}")

    return {
        "mode": "direct",
        "ok": failed == 0,
        "downloaded": len(saved),
        "failed": failed,
        "saved": saved
    }

# =========================================================
# 🎯 Função chamada pelo painel/cron
# =========================================================
def baixar_output(nsa_hint: str = "000", lotes: Optional[List[str]] = None, limit: Optional[int] = None) -> Dict[str, Any]:
    """
    Entrada única usada pelo painel (/api/agente/download) e pelo seu cron local:
      - DOWNLOAD_MODE=zip     → baixa ZIP consolidado (legado)
      - DOWNLOAD_MODE=lease   → leasing seguro (recomendado)
      - DOWNLOAD_MODE=direct  → simples, marca baixado na resposta
    Parâmetros opcionais para modos pull: lotes, limit
    """
    _ensure_dir(LOCAL_RECEIVED)

    try:
        if DOWNLOAD_MODE == "zip":
            return _baixar_zip_consolidado(nsa_hint=nsa_hint)
        elif DOWNLOAD_MODE == "lease":
            return _pull_lease(lotes=lotes, limit=limit)
        elif DOWNLOAD_MODE == "direct":
            return _pull_direct(lotes=lotes, limit=limit)
        else:
            msg = f"DOWNLOAD_MODE inválido: {DOWNLOAD_MODE}"
            log(f"❌ {msg}")
            return {"ok": False, "error": msg}
    except Exception as e:
        log(f"❌ Erro durante o download: {e}")
        return {"ok": False, "error": str(e)}
