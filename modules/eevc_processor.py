# =============================================================================
#  📦 process_eevc.py - Processador de Arquivos EEVC (Vendas Crédito) v4.5
#  Autor: Wilson Martins | NETUNNA Software
#  Última atualização: 2026-04-17 (v3.0 - Stateless & Manual-Compliant)
#  Descrição: Divide arquivo EEVC por PV, recalcula totais e gera arquivos filhos
# =============================================================================

import os
import re
from collections import defaultdict
from utils.file_utils import ensure_outfile
from utils.validation_utils import to_centavos, validar_totais


# =============================================================================
#  🔹 Funções auxiliares
# =============================================================================

def _extract_data_nsa(header_line: str, filename: str) -> tuple[str, str]:
    """
    Extrai data de referência (DDMMAA) e NSA a partir do header 002 ou nome do arquivo.
    """
    data_ref = "000000"
    nsa = "000"

    if header_line.startswith("002") and len(header_line) >= 11:
        raw = header_line[3:11]
        if raw.isdigit() and len(raw) == 8:
            data_ref = f"{raw[:2]}{raw[2:4]}{raw[6:8]}"

    m = re.search(r"(\d{6})(\d{9})", header_line)
    if m:
        nsa_candidate = m.group(1)
        if nsa_candidate.isdigit():
            nsa = nsa_candidate[-3:]

    if nsa == "000":
        m2 = re.search(r"\.(\d{3})\D*$", filename)
        if m2 and m2.group(1).isdigit():
            nsa = m2.group(1)

    return data_ref, nsa


def _rewrite_header_with_pv(header_line: str, pv: str) -> str:
    """
    Substitui no header 002 o código do estabelecimento pelo PV filho.
    """
    pv9 = str(pv).zfill(9)

    def repl(m):
        return f"{m.group(1)}{pv9}"

    new_header, count = re.subn(r"(\d{6})\d{9}", repl, header_line, count=1)
    return new_header if count == 1 else header_line


def _liquido_rv(line: str) -> int:
    """
    Extrai o 'Valor líquido' de registros RV (006/010/016/022).
    Posições: 114–128 (15 caracteres, 2 casas decimais implícitas).
    """
    return to_centavos(line[114:129]) if len(line) >= 129 else 0


def _build_trailer_026(pv: str, total_liquido_cent: int) -> str:
    """
    Monta o registro 026 (Totalizador por PV) conforme layout EEVC v4.5.
    """
    def num15(n: int) -> str:
        return str(max(0, n)).zfill(15)

    parts = [
        "026", str(pv).zfill(9), "0".zfill(15), "0".zfill(6), "0".zfill(15),
        "0".zfill(15), "0".zfill(15), "0".zfill(15), "0".zfill(15), "0".zfill(15),
        num15(total_liquido_cent), "0".zfill(15), "0".zfill(15), "0".zfill(6),
    ]
    return "".join(parts)


# =============================================================================
#  🔹 Processador EEVC - Função principal
# =============================================================================

def process_eevc(input_path: str, output_dir: str, error_dir: str = "erro") -> dict:
    """
    Processa arquivo EEVC (Vendas Crédito) v4.5 compatível com NSA.
    Estratégia Stateless: extrai PV diretamente de cada registro (pos. 004-012).
    """
    print("🟢 Processando EEVC (Vendas Crédito) v4.5")
    filename = os.path.basename(input_path)

    with open(input_path, "r", encoding="latin-1", errors="replace") as f:
        lines = [l.rstrip("\n") for l in f if l.strip()]

    if not lines:
        raise ValueError("Arquivo EEVC vazio.")

    header_line = None
    trailer_line = None
    grupos = defaultdict(list)
    totais_pv = defaultdict(int)
    audit = {"012_fonte": 0, "012_gerados": 0}

    # Tipos válidos conforme manual EEVC v4.5 (Seção 3 & 4)
    TIPOS_RV = {"006", "010", "016", "022"}
    TIPOS_VALIDOS = {
        "002", "004", "005", "033", "006", "008", "034", "040",
        "010", "011", "012", "035", "014", "016", "017", "018", "036",
        "019", "020", "021", "022", "024", "029", "026", "028"
    }

    # =====================================================================
    #  🔁 Loop principal de processamento (Stateless)
    # =====================================================================
    for line in lines:
        if len(line) < 3:
            continue
        tipo = line[:3]

        if tipo == "002":
            header_line = line
            continue
        if tipo == "028":
            trailer_line = line
            continue

        if tipo not in TIPOS_VALIDOS:
            continue  # Ignora linhas fora do layout oficial

        # ✅ EXTRAÇÃO DIRETA DO PV (Manual garante pos. 004-012 para TODOS os registros)
        if len(line) < 12:
            continue
        pv = line[3:12].strip()
        if not pv.isdigit() or len(pv) != 9:
            continue

        grupos[pv].append(line)
        audit["012_fonte"] += (1 if tipo == "012" else 0)

        # Soma valores líquidos apenas dos RVs para validação com trailer 028
        if tipo in TIPOS_RV:
            totais_pv[pv] += _liquido_rv(line)

    # Validação de estrutura mínima
    if not header_line or not trailer_line:
        raise ValueError("Header (002) ou Trailer (028) ausentes no arquivo EEVC.")

    data_ref, nsa = _extract_data_nsa(header_line, filename)

    # =====================================================================
    #  📤 Geração dos arquivos filhos
    # =====================================================================
    gerados = []
    soma_total_processado = 0

    subdir = os.path.join(output_dir, f"NSA_{nsa}")
    os.makedirs(subdir, exist_ok=True)

    for pv, blocos in grupos.items():
        # ✅ CORREÇÃO DE ESCALA: round() para evitar perda acumulada de centavos
        total_liquido_rv = round(totais_pv[pv] / 10)
        soma_total_processado += total_liquido_rv

        header_pv = _rewrite_header_with_pv(header_line, pv)
        trailer_026 = _build_trailer_026(pv, total_liquido_rv)

        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVC.txt"
        out_path = os.path.join(subdir, nome_arquivo)

        with open(out_path, "w", encoding="latin-1", errors="ignore") as f:
            f.write(header_pv + "\n")
            for l in blocos:
                # Substitui o 026 original pelo recalculado
                if not l.startswith("026"):
                    f.write(l + "\n")
            f.write(trailer_026 + "\n")
            f.write(trailer_line + "\n")

        gerados.append(out_path)
        audit["012_gerados"] += sum(1 for l in blocos if l.startswith("012"))
        print(f"🧾 Gerado: {os.path.basename(out_path)} | Líquido: {total_liquido_rv} | 012: {sum(1 for l in blocos if l.startswith('012'))}")

    # =====================================================================
    #  ✅ Validação final + Auditoria de integridade
    # =====================================================================
    total_trailer_str = trailer_line[133:148].strip() if len(trailer_line) >= 148 else "0"
    total_trailer = int(total_trailer_str) if total_trailer_str.isdigit() else 0

    detalhe = validar_totais(total_trailer, soma_total_processado)
    status_totais = "OK" if total_trailer == soma_total_processado else "ERRO"
    status_012 = "OK" if audit["012_fonte"] == audit["012_gerados"] else "ERRO"
    status_final = "OK" if (status_totais == "OK" and status_012 == "OK") else "ERRO"

    print(f"✅ EEVC — Trailer(028): {total_trailer} | Processado: {soma_total_processado} | {status_totais}")
    print(f"🔍 Auditoria 012 — Fonte: {audit['012_fonte']} | Gerados: {audit['012_gerados']} | {status_012}")

    return {
        "arquivo": filename,
        "data_ref": data_ref,
        "nsa": nsa,
        "total_trailer": total_trailer,
        "total_processado": soma_total_processado,
        "status": status_final,
        "detalhe": f"{detalhe} | 012:{status_012}",
        "gerados": gerados,
        "audit": audit,
    }
