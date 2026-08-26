import os
import sqlite3
import hashlib
from pathlib import Path


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


class AgentState:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init(self):
        with self._connect() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS processamentos (
                    cliente TEXT NOT NULL,
                    arquivo TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    batch_id TEXT,
                    nsa TEXT,
                    status TEXT NOT NULL,
                    detalhe TEXT,
                    atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (cliente, sha256)
                )
            """)

    def status(self, cliente: str, sha256: str):
        with self._connect() as con:
            row = con.execute(
                "SELECT status, batch_id, nsa, detalhe FROM processamentos WHERE cliente=? AND sha256=?",
                (cliente, sha256),
            ).fetchone()
        if not row:
            return None
        return {"status": row[0], "batch_id": row[1], "nsa": row[2], "detalhe": row[3]}

    def save(self, cliente, arquivo, sha256, status, batch_id=None, nsa=None, detalhe=None):
        with self._connect() as con:
            con.execute("""
                INSERT INTO processamentos(cliente, arquivo, sha256, batch_id, nsa, status, detalhe, atualizado_em)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(cliente, sha256) DO UPDATE SET
                    arquivo=excluded.arquivo,
                    batch_id=COALESCE(excluded.batch_id, processamentos.batch_id),
                    nsa=COALESCE(excluded.nsa, processamentos.nsa),
                    status=excluded.status,
                    detalhe=excluded.detalhe,
                    atualizado_em=CURRENT_TIMESTAMP
            """, (cliente, arquivo, sha256, batch_id, nsa, status, detalhe))
