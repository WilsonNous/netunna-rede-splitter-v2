import os
import io
import zipfile
import hashlib
import tempfile
import shutil
from pathlib import Path
import requests
from agente.utils import log

SPLITTER_BASE_URL = (os.getenv("SPLITTER_BASE_URL") or "").rstrip("/")
SPLITTER_API_KEY = (os.getenv("SPLITTER_API_KEY") or "").strip()
REQUEST_TIMEOUT = int(os.getenv("SPLITTER_DOWNLOAD_TIMEOUT", "300"))


def _headers():
    if not SPLITTER_API_KEY:
        raise RuntimeError("SPLITTER_API_KEY não configurada no agente.")
    return {"Authorization": f"Bearer {SPLITTER_API_KEY}"}


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _get_manifest(cliente: str, batch_id: str) -> dict:
    url = f"{SPLITTER_BASE_URL}/api/v1/batches/{batch_id}/files"
    r = requests.get(url, headers=_headers(), params={"cliente": cliente}, timeout=90)
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(data.get("erro") or "Manifest inválido.")
    return data


def baixar_batch_para_smedi(cliente: str, batch_id: str, smedi_dir: str) -> dict:
    """Baixa somente um batch e libera os filhos na pasta do SMEDI.

    O ZIP é extraído em diretório temporário; tamanho e SHA-256 são validados
    contra o manifest antes de qualquer arquivo aparecer no diretório SMEDI.
    """
    if not SPLITTER_BASE_URL:
        raise RuntimeError("SPLITTER_BASE_URL não configurada no agente.")

    destino = Path(smedi_dir)
    destino.mkdir(parents=True, exist_ok=True)
    manifest = _get_manifest(cliente, batch_id)
    esperados = {item["nome"]: item for item in manifest.get("arquivos", [])}
    if not esperados:
        raise RuntimeError(f"Batch {batch_id} não possui arquivos filhos.")

    url = f"{SPLITTER_BASE_URL}/api/v1/batches/{batch_id}/download"
    log(f"⬇️ [{cliente}] Baixando batch {batch_id} ({len(esperados)} arquivos)...")
    r = requests.get(url, headers=_headers(), params={"cliente": cliente}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    if "application/zip" not in (r.headers.get("Content-Type") or ""):
        raise RuntimeError("A API não retornou um arquivo ZIP.")

    saved, already = [], []
    with tempfile.TemporaryDirectory(prefix=f"splitter_{cliente}_{batch_id}_") as td:
        temp_dir = Path(td)
        with zipfile.ZipFile(io.BytesIO(r.content), "r") as zf:
            # Evita path traversal e aceita apenas nomes simples gerados pelo backend.
            for member in zf.infolist():
                name = Path(member.filename).name
                if not name or name != member.filename.replace("\\", "/").split("/")[-1]:
                    continue
                if name not in esperados:
                    continue
                with zf.open(member, "r") as src, (temp_dir / name).open("wb") as dst:
                    shutil.copyfileobj(src, dst)

        encontrados = {p.name: p for p in temp_dir.iterdir() if p.is_file()}
        faltantes = sorted(set(esperados) - set(encontrados))
        if faltantes:
            raise RuntimeError(f"Arquivos ausentes no ZIP: {faltantes}")

        # Validação completa ANTES da entrega ao SMEDI.
        for nome, item in esperados.items():
            p = encontrados[nome]
            size = int(item.get("tamanho") or 0)
            sha = (item.get("sha256") or "").lower()
            if size and p.stat().st_size != size:
                raise RuntimeError(f"Tamanho divergente em {nome}: {p.stat().st_size} != {size}")
            if sha and _sha256(p).lower() != sha:
                raise RuntimeError(f"SHA-256 divergente em {nome}")

        # Só agora os arquivos são liberados para a pasta observada pelo SMEDI.
        for nome, p in encontrados.items():
            final = destino / nome
            if final.exists():
                if _sha256(final) == _sha256(p):
                    already.append(str(final))
                    log(f"↪️ [{cliente}] Já existente e idêntico no SMEDI: {nome}")
                    continue
                raise RuntimeError(f"Já existe arquivo diferente com o mesmo nome no SMEDI: {nome}")

            # os.replace é atômico quando temporário/destino estão no mesmo volume;
            # como TemporaryDirectory pode estar em outro volume, copiamos para .part
            # dentro do destino e então renomeamos.
            part = destino / f".{nome}.part"
            shutil.copy2(p, part)
            os.replace(part, final)
            saved.append(str(final))
            log(f"✅ [{cliente}] Entregue ao SMEDI: {final}")

    return {
        "ok": True,
        "cliente": cliente,
        "batch_id": batch_id,
        "nsa": manifest.get("nsa"),
        "quantidade": len(esperados),
        "saved": saved,
        "already_present": already,
    }


# Compatibilidade com chamadas antigas do painel/agente.
def baixar_output(*args, **kwargs):
    raise RuntimeError(
        "baixar_output legado foi desativado para automação. "
        "Use baixar_batch_para_smedi(cliente, batch_id, smedi_dir)."
    )
