import os
import re
from collections import defaultdict
from utils.file_utils import ensure_outfile
from utils.validation_utils import to_centavos, validar_totais


# ======================================================
#  🔹 Funções auxiliares
# ======================================================

def _extract_data_nsa(header_line: str, filename: str) -> tuple[str, str]:
    """Extrai data (DDMMAA) e NSA a partir do header ou nome do arquivo."""
    data_ref = "000000"
    nsa = "000"

    # Data no header (posições 3–11)
    if header_line.startswith("002") and len(header_line) >= 11:
        raw = header_line[3:11]
        if raw.isdigit():
            data_ref = f"{raw[:2]}{raw[2:4]}{raw[6:8]}"

    # NSA tentativa por header (sequência de 6 dígitos ex: 000041)
    m = re.search(r"(\d{6})(\d{9})", header_line)
    if m:
        nsa_candidate = m.group(1)
        if nsa_candidate.isdigit():
            nsa = nsa_candidate[-3:]

    # Fallback via nome
    if nsa == "000":
        m2 = re.search(r"\.(\d{3})\D*$", filename)
        if m2:
            nsa = m2.group(1)

    return data_ref, nsa


def _rewrite_header_with_pv(header_line: str, pv: str) -> str:
    """Substitui no header 002 o código do estabelecimento pelo PV filho."""
    pv9 = str(pv).zfill(9)

    def repl(m):
        nsa6 = m.group(1)
        return f"{nsa6}{pv9}"

    new_header, count = re.subn(r"(\d{6})\d{9}", repl, header_line, count=1)
    return new_header if count == 1 else header_line


def _get_liquido_valor(line: str) -> int:
    """Extrai valor líquido do registro conforme o tipo."""
    tipo = line[:3]
    try:
        if tipo in ("006", "010", "016", "022"):  # RVs
            val_str = line[114:129]
        elif tipo in ("008", "012", "018"):       # CV/NSU
            val_str = line[206:221]
        elif tipo == "014":                       # Parcela
            val_str = line[70:85]
        elif tipo == "024":                       # Dólar
            val_str = line[38:53]
        else:
            return 0
        return to_centavos(val_str)
    except Exception:
        return 0


def _get_status(line: str) -> str:
    """Extrai o campo de status conforme o tipo de registro."""
    tipo = line[:3]
    try:
        if tipo in ("006", "010", "016", "022"):
            # Status RV: posições 117–119
            return line[117:120].strip()
        elif tipo in ("008", "012", "018"):
            # Status CV/NSU: posições 84–86
            return line[84:87].strip()
        elif tipo == "014":
            # Parcelas — podem não ter status (assume válido)
            return "0"
        elif tipo == "024":
            # Transações em dólar — idem
            return "0"
        else:
            return "999"  # não somável
    except Exception:
        return "999"


# ======================================================
#  🔹 Processador EEVC
# ======================================================

def process_eevc(input_path: str, output_dir: str, error_dir: str = "erro"):
    """
    Processa arquivo EEVC (Vendas Crédito) v4.
    - Divide por PV (registro 004)
    - Soma valores líquidos de 006–024
    - Ignora cancelamentos (status ≠ 0)
    - Recalcula trailer 026 (total líquido)
    - Valida com 028 (arquivo mãe)
    """
    print("🟢 Processando EEVC (Vendas Crédito)")
    filename = os.path.basename(input_path)

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.rstrip("\n") for l in f if l.strip()]

    if not lines:
        raise ValueError("Arquivo EEVC vazio.")

    header_line = None
    trailer_line = None
    grupos = defaultdict(list)
    totais_pv = defaultdict(lambda: {"liquido": 0})
    current_pv = None

    for line in lines:
        tipo = line[:3]

        if tipo == "002":
            header_line = line

        elif tipo == "004":  # Início de bloco PV
            pv = line[3:12].strip()
            current_pv = pv
            grupos[pv].append(line)

        elif tipo in ("006", "008", "010", "012", "014", "016", "018", "022", "024", "026", "028"):
            if current_pv:
                status = _get_status(line)
                if status in ("", "0", "000"):  # válidos
                    valor = _get_liquido_valor(line)
        
                    # Regras finais conforme manual EEVC
                    if tipo in ("012", "018"):      # débitos e cancelamentos
                        valor = -valor
                    elif tipo in ("008", "026", "028"):  # ajustes e trailers
                        valor = 0
        
                    totais_pv[current_pv]["liquido"] += valor
        
                grupos[current_pv].append(line)


        elif tipo == "026":
            if current_pv:
                grupos[current_pv].append(line)
                current_pv = None

        elif tipo == "028":
            trailer_line = line

        else:
            if current_pv:
                grupos[current_pv].append(line)

    # Extração data e NSA
    data_ref, nsa = _extract_data_nsa(header_line, filename)

    # --- Geração dos filhos ---
    gerados = []
    soma_total_processado = 0

    for pv, blocos in grupos.items():
        total_liquido = totais_pv[pv]["liquido"]
        soma_total_processado += total_liquido

        header_pv = _rewrite_header_with_pv(header_line, pv)

        # Trailer 026 customizado (15 dígitos com zeros à esquerda)
        trailer_026 = f"026{pv.zfill(9)}{' ' * 102}{str(total_liquido).zfill(15)}"

        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVC.txt"
        out_path = ensure_outfile(output_dir, nome_arquivo)

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(header_pv + "\n")
            for l in blocos:
                if not l.startswith("026"):  # substitui trailer 026 por recalculado
                    f.write(l + "\n")
            f.write(trailer_026 + "\n")
            if trailer_line:
                f.write(trailer_line + "\n")

        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)} — Total líquido: {total_liquido}")

    # --- Validação total com trailer 028 (arquivo mãe) ---
    total_trailer_str = trailer_line[134:149].strip() if trailer_line else "0"
    total_trailer = int(total_trailer_str) if total_trailer_str.isdigit() else 0
    detalhe = validar_totais(total_trailer, soma_total_processado)
    status = "OK" if total_trailer == soma_total_processado else "ERRO"

    print(f"✅ EEVC — Total trailer: {total_trailer} | Processado: {soma_total_processado} | {status}")

    return {
        "arquivo": filename,
        "data_ref": data_ref,
        "nsa": nsa,
        "total_trailer": total_trailer,
        "total_processado": soma_total_processado,
        "status": status,
        "detalhe": detalhe,
        "gerados": gerados,
    }
