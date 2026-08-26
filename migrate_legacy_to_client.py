#!/usr/bin/env python3
"""
Migração segura da estrutura legada do Netunna REDE Splitter para clientes/<cliente>/.

Por padrão roda em DRY-RUN: não move nem apaga nada.
Use --apply somente depois de revisar o relatório.

Migra:
  <BASE_DIR>/input/*             -> <BASE_DIR>/clientes/<cliente>/input/
  <BASE_DIR>/output/NSA_*/*      -> <BASE_DIR>/clientes/<cliente>/output/NSA_*/
  <BASE_DIR>/erro/*              -> <BASE_DIR>/clientes/<cliente>/erro/

Não toca em:
  <BASE_DIR>/output/_api_batches/
  <BASE_DIR>/logs/

Conflitos:
- destino ausente: move no --apply;
- destino existente com mesmo SHA-256: remove a cópia legada no --apply;
- destino existente com conteúdo diferente: NÃO altera nada e registra CONFLICT.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def collect_plan(base: Path, cliente: str):
    target_root = base / "clientes" / cliente
    plan = []

    # input legado: somente arquivos da raiz; ignora _api_batches.
    input_root = base / "input"
    if input_root.exists():
        for src in sorted(input_root.iterdir()):
            if src.name == "_api_batches" or not src.is_file():
                continue
            plan.append((src, target_root / "input" / src.name, "input"))

    # output legado: somente lotes NSA_*; ignora _api_batches e qualquer outro conteúdo.
    output_root = base / "output"
    if output_root.exists():
        for lote in sorted(output_root.iterdir()):
            if not lote.is_dir() or not lote.name.startswith("NSA_"):
                continue
            for src in sorted(lote.rglob("*")):
                if not src.is_file():
                    continue
                rel = src.relative_to(lote)
                plan.append((src, target_root / "output" / lote.name / rel, "output"))

    # erro legado: arquivos e subpastas, exceto _api_batches.
    error_root = base / "erro"
    if error_root.exists():
        for src in sorted(error_root.rglob("*")):
            if not src.is_file() or "_api_batches" in src.parts:
                continue
            rel = src.relative_to(error_root)
            plan.append((src, target_root / "erro" / rel, "erro"))

    return plan


def migrate(base: Path, cliente: str, apply: bool):
    plan = collect_plan(base, cliente)
    results = []
    counts = {"MOVE": 0, "DUPLICATE": 0, "CONFLICT": 0, "ERROR": 0}

    for src, dst, area in plan:
        item = {
            "area": area,
            "source": str(src),
            "destination": str(dst),
            "size": src.stat().st_size,
        }
        try:
            src_hash = sha256(src)
            item["sha256"] = src_hash

            if dst.exists():
                if not dst.is_file():
                    item["action"] = "CONFLICT"
                    item["detail"] = "Destino existe e não é arquivo."
                else:
                    dst_hash = sha256(dst)
                    item["destination_sha256"] = dst_hash
                    if dst_hash == src_hash:
                        item["action"] = "DUPLICATE"
                        item["detail"] = "Mesmo conteúdo já existe no destino."
                        if apply:
                            src.unlink()
                            item["applied"] = "origem_legada_removida"
                    else:
                        item["action"] = "CONFLICT"
                        item["detail"] = "Mesmo nome com conteúdo diferente; nada foi alterado."
            else:
                item["action"] = "MOVE"
                item["detail"] = "Arquivo pode ser migrado."
                if apply:
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(src), str(dst))
                    item["applied"] = True

            counts[item["action"]] += 1
        except Exception as exc:
            item["action"] = "ERROR"
            item["detail"] = repr(exc)
            counts["ERROR"] += 1
        results.append(item)

    # Remove somente diretórios NSA_* vazios após apply. Nunca toca em _api_batches.
    if apply:
        output_root = base / "output"
        if output_root.exists():
            for d in sorted(output_root.glob("NSA_*"), reverse=True):
                try:
                    for child in sorted(d.rglob("*"), reverse=True):
                        if child.is_dir() and not any(child.iterdir()):
                            child.rmdir()
                    if d.is_dir() and not any(d.iterdir()):
                        d.rmdir()
                except OSError:
                    pass

    log_dir = base / "migration_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")
    mode = "APPLY" if apply else "DRYRUN"
    report = {
        "mode": mode,
        "base_dir": str(base),
        "cliente": cliente,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "counts": counts,
        "items": results,
    }
    report_path = log_dir / f"migration_{cliente}_{stamp}_{mode}.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Modo: {mode}")
    print(f"Cliente: {cliente}")
    print(f"BASE_DIR: {base}")
    print(f"MOVE={counts['MOVE']} DUPLICATE={counts['DUPLICATE']} CONFLICT={counts['CONFLICT']} ERROR={counts['ERROR']}")
    print(f"Relatório: {report_path}")
    if not apply:
        print("Nenhum arquivo foi alterado. Para executar: use --apply")
    return 1 if counts["CONFLICT"] or counts["ERROR"] else 0


def main():
    parser = argparse.ArgumentParser(description="Migra legado do REDE Splitter para estrutura multi-cliente.")
    parser.add_argument("--cliente", default="ventuno", help="ID do cliente (default: ventuno)")
    parser.add_argument("--base-dir", default=os.getenv("BASE_DIR", "/home/site/azurefiles"))
    parser.add_argument("--apply", action="store_true", help="Executa a migração; sem esta flag é apenas simulação.")
    args = parser.parse_args()

    cliente = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in args.cliente.strip().lower()).strip("-_")
    if not cliente:
        raise SystemExit("Cliente inválido.")
    raise SystemExit(migrate(Path(args.base_dir), cliente, args.apply))


if __name__ == "__main__":
    main()
