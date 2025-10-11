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

    # Data no header 002: pos. 004–011 (DDMMAAAA) → armazeno DDMMAA
    # Manual: Registro 002 – Header do Arquivo (Data de emissão) 
    # (DDMMAAAA). 
    if header_line.startswith("002") and len(header_line) >= 11:
        raw = header_line[3:11]
        if raw.isdigit():
            data_ref = f"{raw[:2]}{raw[2:4]}{raw[6:8]}"

    # NSA tentativa (sequência 006 dígitos + PV 009) no 002: pos. 072–077 = seq.,
    # mas como o layout varia em VB, mantemos o teu fallback por regex e nome.
    m = re.search(r"(\d{6})(\d{9})", header_line)
    if m:
        nsa_candidate = m.group(1)
        if nsa_candidate.isdigit():
            nsa = nsa_candidate[-3:]

    # Fallback via nome (final .XYZ)
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


def _liquido_rv(line: str) -> int:
    """
    Extrai 'Valor líquido' dos RVs 006/010/016/022.
    Manual: posições 114–128 (9(13)V99) para cada um desses registros.
    """
    return to_centavos(line[114:129]) if len(line) >= 129 else 0


def _valor_liquido_cv(line: str) -> int:
    """
    Extrai 'Valor líquido' dos CVs que possuem esse campo:
    - 012/018: pos. 206–220 = Valor líquido do CV/NSU.
    (Obs.: 024 não tem 'valor líquido', é 'Valor do CV/NSU' em 038–052.)
    """
    return to_centavos(line[206:221]) if len(line) >= 221 else 0


def _status_cv(line: str) -> str:
    """Status do CV/NSU nas posições 84–86 para 008/012/018/024."""
    return line[84:87].strip() if len(line) >= 87 else ""


def _build_trailer_026(pv: str, total_liquido_cent: int) -> str:
    """
    Monta o 026 respeitando posições:
    001-003 '026'
    004-012 PV (9)
    013-027 Valor total bruto (15) -> zeros
    028-033 Qtde rejeitados (6) -> zeros
    034-048 Valor total rejeitado (15) -> zeros
    049-063 Total rotativo (15) -> zeros
    064-078 Total parcelado s/ juros (15) -> zeros
    079-093 Total IATA (15) -> zeros
    094-108 Total dólar (15) -> zeros
    109-123 Total desconto (15) -> zeros
    124-138 Total líquido (15) -> **preenche**
    139-153 Total gorjeta (15) -> zeros
    154-168 Total taxa embarque (15) -> zeros
    169-174 Qtde CV/NSU acatados (6) -> zeros
    """
    def num15(n): return str(max(0, n)).zfill(15)
    parts = [
        "026",
        str(pv).zfill(9),
        "0".zfill(15),  # bruto
        "0".zfill(6),   # qtd rejeitados
        "0".zfill(15),  # rejeitado
        "0".zfill(15),  # rotativo
        "0".zfill(15),  # parcelado s/ juros
        "0".zfill(15),  # IATA
        "0".zfill(15),  # dólar
        "0".zfill(15),  # desconto
        num15(total_liquido_cent),  # líquido
        "0".zfill(15),  # gorjeta
        "0".zfill(15),  # taxa embarque
        "0".zfill(6),   # qtd acatados
    ]
    # Concatena respeitando larguras (sem separador)
    s = (
        parts[0] +                      # 3
        parts[1] +                      # +9
        parts[2] +                      # +15
        parts[3] +                      # +6
        parts[4] +                      # +15
        parts[5] +                      # +15
        parts[6] +                      # +15
        parts[7] +                      # +15
        parts[8] +                      # +15
        parts[9] +                      # +15
        parts[10] +                     # +15
        parts[11] +                     # +15
        parts[12] +                     # +15
        parts[13]                       # +6
    )
    return s


# ======================================================
#  🔹 Processador EEVC
# ======================================================

def process_eevc(input_path: str, output_dir: str, error_dir: str = "erro"):
    """
    Processa arquivo EEVC (Vendas Crédito) v4.4.
    - Divide por PV (registro 004…026)
    - Soma SOMENTE RVs (006/010/016/022) para validar com 028 (Valor total líquido)
    - Ignora 008/014/024 na validação do 028 para evitar dupla contagem
    - Recalcula trailer 026 por PV (posicionando 'valor total líquido' em 124–138)
    - Valida com 028 (arquivo mãe, pos. 134–148)
    """
    print("🟢 Processando EEVC (Vendas Crédito)")
    filename = os.path.basename(input_path)

    # REDE: arquivos VB com Latin-1 é o mais resiliente
    with open(input_path, "r", encoding="latin-1", errors="replace") as f:
        lines = [l.rstrip("\n") for l in f if l.strip()]

    if not lines:
        raise ValueError("Arquivo EEVC vazio.")

    header_line = None
    trailer_line = None
    grupos = defaultdict(list)            # PV -> linhas do bloco (sem 026 original)
    totais_pv = defaultdict(lambda: {"liquido_rv": 0})
    current_pv = None

    for line in lines:
        tipo = line[:3]

        if tipo == "002":
            header_line = line

        elif tipo == "004":  # Início de bloco PV
            pv = line[3:12].strip()
            current_pv = pv
            grupos[pv].append(line)

        elif tipo in ("006", "010", "016", "022"):
            # RVs: somar líquido (114–128)
            if current_pv:
                tot = _liquido_rv(line)
                totais_pv[current_pv]["liquido_rv"] += tot
                grupos[current_pv].append(line)

        elif tipo in ("008", "012", "018", "014", "024"):
            # CVs/parcelas não entram na soma do 028.
            # Mantemos as linhas no bloco do PV para o filho, se dentro de PV.
            if current_pv:
                grupos[current_pv].append(line)

        elif tipo == "026":
            # Fecha o bloco do PV (não guardar o 026 original; será recalculado)
            current_pv = None

        elif tipo == "028":
            # Trailer do arquivo (global, fora de PV)
            trailer_line = line

        else:
            # Qualquer outro registro dentro do PV vai para o filho
            if current_pv:
                grupos[current_pv].append(line)

    if not header_line or not trailer_line:
        raise ValueError("Header (002) ou Trailer (028) ausentes no arquivo EEVC.")

    # Extração data e NSA para nome do filho
    data_ref, nsa = _extract_data_nsa(header_line, filename)

    # --- Geração dos filhos ---
    gerados = []
    soma_total_processado = 0

    for pv, blocos in grupos.items():
        total_liquido_rv = totais_pv[pv]["liquido_rv"]
        soma_total_processado += total_liquido_rv

        header_pv = _rewrite_header_with_pv(header_line, pv)

        # 026 recalculado (preenche apenas Total Líquido; demais campos zerados)
        trailer_026 = _build_trailer_026(pv, total_liquido_rv)

        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVC.txt"
        out_path = ensure_outfile(output_dir, nome_arquivo)

        with open(out_path, "w", encoding="latin-1", errors="ignore") as f:
            f.write(header_pv + "\n")
            for l in blocos:
                if not l.startswith("026"):  # substitui trailer 026 original por recalculado
                    f.write(l + "\n")
            f.write(trailer_026 + "\n")
            f.write(trailer_line + "\n")  # mantém 028 ao final do filho para referência

        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)} — Total líquido (RVs): {total_liquido_rv}")

    # --- Validação total com trailer 028 (arquivo mãe) ---
    # Manual 028: Valor total líquido pos. 134–148
    total_trailer_str = trailer_line[133:148].strip() if len(trailer_line) >= 148 else "0"
    total_trailer = int(total_trailer_str) if total_trailer_str.isdigit() else 0
    detalhe = validar_totais(total_trailer, soma_total_processado)
    status = "OK" if total_trailer == soma_total_processado else "ERRO"

    print(f"✅ EEVC — Total trailer(028): {total_trailer} | Processado(RVs): {soma_total_processado} | {status}")

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
