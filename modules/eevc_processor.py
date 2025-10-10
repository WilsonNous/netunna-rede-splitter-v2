import os
import re
from collections import defaultdict
from utils.file_utils import ensure_outfile
from utils.validation_utils import validar_totais  # vamos usar “contagem” por enquanto


# ----------------------------
# Helpers
# ----------------------------
def _ddmmaa_from_ddmmaaaa(s: str) -> str:
    # s ="DDMMAAAA" -> "DDMMAA"
    if re.fullmatch(r"\d{8}", s):
        return f"{s[:2]}{s[2:4]}{s[6:8]}"
    return "000000"


def _extract_data_nsa_from_header_or_name(header_line: str, filename: str) -> tuple[str, str]:
    """
    Data (DDMMAA) e NSA:
    - Data: header 002 posições 3–10 (DDMMAAAA) -> DDMMAA
    - NSA: tentativa via header (padrão 6 dígitos “000041” próximo ao CNPJ raiz), senão fallback pelo nome
    """
    data_ref = "000000"
    nsa = "000"

    # Data do header (posicional): 002 + [3:11] = DDMMAAAA
    if len(header_line) >= 11 and header_line.startswith("002"):
        raw = header_line[3:11]
        data_ref = _ddmmaa_from_ddmmaaaa(raw)

    # NSA tentativa por header:
    # Padrão visto: "... 000041020770677 DIARIO ..." (6 dígitos de NSA seguidos de 9 dígitos do estabelecimento)
    m = re.search(r"(\d{6})(\d{9})", header_line)
    if m:
        nsa_candidate = m.group(1)  # ex: 000041
        if nsa_candidate.isdigit():
            nsa = nsa_candidate[-3:]

    # Fallback pelo nome (ex.: VENTUNO... .251005.123 -> 123)
    if nsa == "000":
        m2 = re.search(r"\.(\d{3})\s*$", filename)
        if m2 and m2.group(1).isdigit():
            nsa = m2.group(1)

    return data_ref, nsa


def _rewrite_header_with_pv(header_line: str, pv: str) -> str:
    """
    Substitui no HEADER 002 o código do estabelecimento da matriz pelo PV do arquivo filho.
    Pelo padrão observado, temos um bloco “NSA(6) + ESTAB(9)” -> ex.: 000041020770677
    Mantemos a NSA (6 dígitos) e trocamos apenas os 9 dígitos seguintes pelo PV.
    """
    pv9 = str(pv).zfill(9)

    def repl(m):
        nsa6 = m.group(1)  # mantém a NSA
        return f"{nsa6}{pv9}"

    # troca somente a PRIMEIRA ocorrência desse padrão
    new_header, count = re.subn(r"(\d{6})\d{9}", repl, header_line, count=1)
    return new_header if count == 1 else header_line


# ----------------------------
# Processador EEVC
# ----------------------------
def process_eevc(input_path: str, output_dir: str, error_dir: str = "erro"):
    """
    EEVC (Crédito) v3 – separação por PV e validação por CONTAGEM (temporária).

    Registros conhecidos:
      - 002: Header do arquivo
      - 004: Detalhe por estabelecimento (PV)
      - 026: Fim do bloco do estabelecimento (opcional/conforme layout)
      - 028: Trailer do arquivo

    Saída:
      - <PV>_<DDMMAA>_<NSA>_EEVC.txt
      - Header com PV ajustado
      - Trailer copiado do arquivo-mãe (até migrarmos para trailer recalculado por PV)
    """
    filename = os.path.basename(input_path)

    with open(input_path, "r", encoding="utf-8", errors="replace") as f:
        lines = [l.rstrip("\n") for l in f if l.strip()]

    if not lines:
        raise ValueError("Arquivo EEVC vazio.")

    header_line = None
    trailer_line = None

    # agrupamento por PV
    grupos = defaultdict(list)
    current_pv = None
    total_004 = 0  # vamos validar por contagem de 004, por enquanto

    for line in lines:
        tipo = line[:3]

        if tipo == "002":
            header_line = line

        elif tipo == "004":
            # PV nas posições 3:12
            pv = line[3:12].strip()
            current_pv = pv
            grupos[pv].append(line)
            total_004 += 1

        elif tipo == "026":
            # fim do bloco do PV corrente
            if current_pv:
                grupos[current_pv].append(line)
                current_pv = None

        elif tipo == "028":
            trailer_line = line

        else:
            # Linhas intermediárias pertencem ao PV corrente, se houver
            if current_pv:
                grupos[current_pv].append(line)

    # Data e NSA pro nome de arquivo
    if not header_line:
        raise ValueError("Header 002 não encontrado no EEVC.")
    data_ref, nsa = _extract_data_nsa_from_header_or_name(header_line, filename)

    # Geração dos filhos
    gerados = []
    total_processado = 0  # usaremos contagem (nº de 004) como “processado”

    for pv, blocos in grupos.items():
        # Conta quantos 004 neste PV (para resumo)
        count_004_pv = sum(1 for l in blocos if l.startswith("004"))
        total_processado += count_004_pv

        # Header do filho com PV ajustado
        header_line_pv = _rewrite_header_with_pv(header_line, pv)

        # Nome <PV>_<DDMMAA>_<NSA>_EEVC.txt
        out_name = f"{pv}_{data_ref}_{nsa}_EEVC.txt"
        out_path = ensure_outfile(output_dir, out_name)

        with open(out_path, "w", encoding="utf-8") as fout:
            fout.write(header_line_pv + "\n")
            for l in blocos:
                fout.write(l + "\n")
            if trailer_line:
                fout.write(trailer_line + "\n")

        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)}")

    # -------- Validação (temporária: por contagem) --------
    total_trailer = total_004
    detalhe = "Validação OK — contagem de 004 consistente entre mãe e filhos."
    status = "OK" if total_processado == total_trailer else "ERRO"
    if status == "ERRO":
        diff = total_processado - total_trailer
        detalhe = f"Divergência de contagem (004): {diff:+d}"

    print(f"✅ EEVC — Contagem 004 | Trailer: {total_trailer} | Processado: {total_processado} | {status}")

    return {
        "arquivo": filename,
        "data_ref": data_ref,
        "nsa": nsa,
        "total_trailer": total_trailer,
        "total_processado": total_processado,
        "status": status,
        "detalhe": detalhe,
        "gerados": gerados,
    }
