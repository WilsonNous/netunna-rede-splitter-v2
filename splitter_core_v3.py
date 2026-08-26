import os
from pathlib import Path
from modules.eevd_processor import process_eevd
from modules.eevc_processor import process_eevc
from modules.eefi_processor import process_eefi
from utils.log_utils import log_result

LOG_PATH = os.path.join("logs", "operacoes.csv")


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def limpar_output(output_dir):
    """Remove apenas arquivos soltos da raiz indicada.

    A API v1 usa uma raiz exclusiva por batch; por isso esta limpeza não
    interfere em outros processamentos nem em outros clientes.
    """
    ensure_dir(output_dir)
    for fname in os.listdir(output_dir):
        fpath = os.path.join(output_dir, fname)
        if os.path.isfile(fpath):
            os.remove(fpath)
    print(f"🧹 Limpeza realizada em {output_dir}")


def _as_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_generated(resultado):
    """Normaliza as três implementações existentes dos processadores.

    EEVC -> gerados
    EEVD -> filhos
    EEFI -> files_generated
    """
    if not isinstance(resultado, dict):
        return []

    raw = (
        resultado.get("gerados")
        or resultado.get("filhos")
        or resultado.get("files_generated")
        or []
    )
    gerados = [str(p) for p in raw if p]

    # Alguns cenários, como EEVD sem movimento, criam o arquivo mas não o
    # retornam na lista. Se houver output_dir, recuperamos os arquivos reais.
    if not gerados:
        output_dir = resultado.get("output_dir")
        if output_dir and os.path.isdir(output_dir):
            gerados = [
                os.path.join(output_dir, name)
                for name in sorted(os.listdir(output_dir))
                if os.path.isfile(os.path.join(output_dir, name))
            ]

    return gerados


def _normalize_totals(tipo, resultado):
    """Converte os formatos de totais de EEVC, EEVD e EEFI para uma interface comum."""
    if not isinstance(resultado, dict):
        return 0, 0

    if "total_trailer" in resultado or "total_processado" in resultado:
        return _as_int(resultado.get("total_trailer")), _as_int(resultado.get("total_processado"))

    if tipo == "EEVD":
        trailer = resultado.get("totais_trailer_mae") or {}
        processados = resultado.get("totais_processados") or {}
        # Mantemos o líquido como total sintético para compatibilidade com o
        # log legado; bruto/desconto/líquido permanecem no resultado original.
        return _as_int(trailer.get("liquido")), _as_int(processados.get("liquido"))

    if tipo == "EEFI":
        return _as_int(resultado.get("total_052")), _as_int(resultado.get("sum_pvs"))

    return 0, 0


def process_file(input_path, output_dir, error_dir):
    """Detecta o layout e chama o processador correspondente.

    O retorno é normalizado para que todos os layouts exponham as mesmas chaves:
    status, detalhe, tipo, nsa, gerados, total_trailer e total_processado.
    """
    ensure_dir(output_dir)
    ensure_dir(error_dir)

    filename = os.path.basename(input_path).upper()
    print(f"\n📥 Iniciando processamento de: {filename}")

    tipo = None
    resultado = None

    try:
        if "EEVD" in filename or "_VD_" in filename:
            tipo = "EEVD"
            limpar_output(output_dir)
            resultado = process_eevd(input_path, output_dir)
        elif "EEVC" in filename or "_VC_" in filename:
            tipo = "EEVC"
            limpar_output(output_dir)
            resultado = process_eevc(input_path, output_dir)
        elif "EEFI" in filename or "_FI_" in filename:
            tipo = "EEFI"
            limpar_output(output_dir)
            resultado = process_eefi(input_path, output_dir)
        else:
            raise ValueError("Tipo de arquivo não reconhecido (esperado EEVD, EEVC ou EEFI).")

        if not isinstance(resultado, dict):
            raise ValueError("O processador retornou uma resposta inválida.")

        gerados = _normalize_generated(resultado)
        total_trailer, total_processado = _normalize_totals(tipo, resultado)

        if "status" in resultado:
            status = str(resultado.get("status") or "ERRO").upper()
        elif "ok" in resultado:
            status = "OK" if bool(resultado.get("ok")) else "ERRO"
        else:
            status = "OK" if total_trailer == total_processado else "ERRO"

        detalhe = resultado.get("detalhe") or resultado.get("message") or ""
        data_ref = resultado.get("data_ref", "")
        nsa = str(resultado.get("nsa", "") or "").strip()

        log_result(filename, tipo, total_trailer, total_processado, status, detalhe)

        print("------------------------------------------------")
        print(f"✅ {filename} ({tipo})")
        print(f"   ▸ Status..........: {status}")
        print(f"   ▸ Total trailer...: {total_trailer}")
        print(f"   ▸ Total processado: {total_processado}")
        print(f"   ▸ Arquivos gerados: {len(gerados)}")
        print("------------------------------------------------")

        return {
            "arquivo": filename,
            "tipo": tipo,
            "status": status,
            "detalhe": detalhe,
            "total_trailer": total_trailer,
            "total_processado": total_processado,
            "data_ref": data_ref,
            "nsa": nsa,
            "gerados": gerados,
            "lotes_count": len(gerados),
            # Mantém o retorno bruto para auditoria sem perder dados específicos.
            "processador": resultado,
        }

    except Exception as e:
        log_result(filename, tipo or "DESCONHECIDO", 0, 0, "ERRO", str(e))
        print(f"❌ Falha ao processar {filename}: {e}")
        return {
            "erro": str(e),
            "arquivo": filename,
            "tipo": tipo or "DESCONHECIDO",
            "status": "ERRO",
            "detalhe": str(e),
            "gerados": [],
            "lotes_count": 0,
        }
