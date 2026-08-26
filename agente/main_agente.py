"""Netunna REDE Splitter Agent - automação multi-cliente.

Fluxo por cliente:
  pasta REDE -> API v1 -> batch isolado -> validação -> download -> pasta SMEDI
  -> arquivamento do arquivo mãe.
"""
import json
import os
import shutil
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

from agente.uploader import upload_file
from agente.downloader import baixar_batch_para_smedi
from agente.state import AgentState, sha256_file
from agente.utils import log

CONFIG_PATH = os.getenv("AGENTE_CLIENTES_CONFIG", str(Path(__file__).with_name("clientes.json")))
POLL_INTERVAL = int(os.getenv("AGENTE_POLL_INTERVAL", "30"))
RUN_MODE = (os.getenv("AGENTE_RUN_MODE") or "watch").lower()  # once | watch
STATE_DB = os.getenv("AGENTE_STATE_DB", str(Path(__file__).with_name("state") / "agent.db"))


def carregar_clientes():
    path = Path(CONFIG_PATH)
    if not path.exists():
        raise FileNotFoundError(
            f"Configuração de clientes não encontrada: {path}. "
            "Copie agente/clientes.json.example para agente/clientes.json e ajuste os caminhos."
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    clientes = data.get("clientes", []) if isinstance(data, dict) else []
    ativos = [c for c in clientes if c.get("enabled", True)]
    if not ativos:
        raise RuntimeError("Nenhum cliente ativo em clientes.json.")
    return ativos


def _ensure_dirs(cliente):
    for key in ("input_dir", "processed_dir", "error_dir", "smedi_dir"):
        p = cliente.get(key)
        if not p:
            raise ValueError(f"Cliente {cliente.get('id')}: campo obrigatório ausente: {key}")
        Path(p).mkdir(parents=True, exist_ok=True)


def _matches(filename, cliente):
    exts = [x.lower() for x in cliente.get("extensions", [])]
    if exts and Path(filename).suffix.lower() not in exts:
        return False
    contains = cliente.get("filename_contains", [])
    if contains and not any(token.upper() in filename.upper() for token in contains):
        return False
    return True


def _archive(src: Path, dest_dir: str):
    dest = Path(dest_dir) / src.name
    if dest.exists():
        # Evita sobrescrever histórico; adiciona timestamp.
        dest = Path(dest_dir) / f"{src.stem}_{int(time.time())}{src.suffix}"
    shutil.move(str(src), str(dest))
    return dest


def processar_cliente(cliente, state: AgentState):
    cid = cliente["id"]
    _ensure_dirs(cliente)
    input_dir = Path(cliente["input_dir"])
    arquivos = [p for p in sorted(input_dir.iterdir()) if p.is_file() and _matches(p.name, cliente)]

    for path in arquivos:
        sha = sha256_file(str(path))
        anterior = state.status(cid, sha)
        if anterior and anterior.get("status") == "ENTREGUE":
            log(f"↪️ [{cid}] Arquivo já entregue anteriormente (SHA-256): {path.name}")
            _archive(path, cliente["processed_dir"])
            continue

        try:
            state.save(cid, path.name, sha, "ENVIANDO")
            retorno = upload_file(str(path), cliente=cid)
            batch_id = retorno["batch_id"]
            nsa = retorno.get("nsa")
            quantidade = int(retorno.get("quantidade_gerados") or 0)
            integridade = retorno.get("integridade") or {}

            if integridade and integridade.get("ok") is False:
                raise RuntimeError(f"Integridade recusada: {integridade.get('mensagem')}")
            if quantidade <= 0:
                raise RuntimeError("Processamento retornou zero arquivos filhos.")

            state.save(cid, path.name, sha, "PROCESSADO", batch_id=batch_id, nsa=nsa)
            entrega = baixar_batch_para_smedi(cid, batch_id, cliente["smedi_dir"])
            if not entrega.get("ok"):
                raise RuntimeError(str(entrega))

            archived = _archive(path, cliente["processed_dir"])
            state.save(
                cid, path.name, sha, "ENTREGUE", batch_id=batch_id, nsa=nsa,
                detalhe=f"{quantidade} arquivo(s) entregues; mãe arquivado em {archived}",
            )
            log(f"🏁 [{cid}] Concluído: {path.name} | NSA={nsa} | batch={batch_id} | filhos={quantidade}")
        except Exception as e:
            state.save(cid, path.name, sha, "ERRO", detalhe=str(e))
            log(f"❌ [{cid}] Falha em {path.name}: {e}")
            # Mantemos na entrada para permitir correção/retry sem perda de arquivo.


def executar_ciclo():
    state = AgentState(STATE_DB)
    for cliente in carregar_clientes():
        processar_cliente(cliente, state)


def main():
    log(f"🚀 Netunna REDE Agent multi-cliente iniciado | modo={RUN_MODE}")
    if RUN_MODE == "once":
        executar_ciclo()
        return

    while True:
        try:
            executar_ciclo()
        except Exception as e:
            log(f"❌ Erro no ciclo do agente: {e}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
