# =============================================================================
#  📦 process_eevc.py - Processador de Arquivos EEVC (Vendas Crédito) v4.5
#  Autor: Wilson Martins | NETUNNA Software
#  Última atualização: 2026-04-17
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
    Retorna tupla (data_ref, nsa).
    """
    data_ref = "000000"
    nsa = "000"

    # Data no header 002: pos. 004–011 (DDMMAAAA) → armazena apenas DDMMAA
    if header_line.startswith("002") and len(header_line) >= 11:
        raw = header_line[3:11]
        if raw.isdigit() and len(raw) == 8:
            data_ref = f"{raw[:2]}{raw[2:4]}{raw[6:8]}"

    # NSA: tenta extrair dos 6 dígitos de sequência + PV de 9 dígitos no header
    m = re.search(r"(\d{6})(\d{9})", header_line)
    if m:
        nsa_candidate = m.group(1)
        if nsa_candidate.isdigit():
            nsa = nsa_candidate[-3:]

    # Fallback: extrai NSA do nome do arquivo (ex.: arquivo.041.txt → NSA=041)
    if nsa == "000":
        m2 = re.search(r"\.(\d{3})\D*$", filename)
        if m2 and m2.group(1).isdigit():
            nsa = m2.group(1)

    return data_ref, nsa


def _rewrite_header_with_pv(header_line: str, pv: str) -> str:
    """
    Substitui no header 002 o código do estabelecimento (pos. 010-018) pelo PV filho.
    Mantém a sequência de 6 dígitos e substitui apenas os 9 dígitos do PV.
    """
    pv9 = str(pv).zfill(9)

    def repl(m):
        nsa6 = m.group(1)
        return f"{nsa6}{pv9}"

    # Substitui padrão: 6 dígitos (sequência) + 9 dígitos (PV original)
    new_header, count = re.subn(r"(\d{6})\d{9}", repl, header_line, count=1)
    return new_header if count == 1 else header_line


def _liquido_rv(line: str) -> int:
    """
    Extrai o 'Valor líquido' de registros RV (006/010/016/022).
    Posições: 114–128 (15 caracteres numéricos, 2 casas decimais implícitas).
    Retorna valor em centavos (int).
    """
    if len(line) >= 129:
        return to_centavos(line[114:129])
    return 0


def _build_trailer_026(pv: str, total_liquido_cent: int) -> str:
    """
    Monta o registro 026 (Totalizador por PV) conforme layout EEVC v4.5.
    
    Layout:
    001-003: Tipo '026'
    004-012: PV (9)
    013-027: Valor total bruto (15) → zeros
    028-033: Qtde rejeitados (6) → zeros
    034-048: Valor total rejeitado (15) → zeros
    049-063: Total rotativo (15) → zeros
    064-078: Total parcelado s/ juros (15) → zeros
    079-093: Total IATA (15) → zeros
    094-108: Total dólar (15) → zeros
    109-123: Total desconto (15) → zeros
    124-138: Total líquido (15) → **preenchido**
    139-153: Total gorjeta (15) → zeros
    154-168: Total taxa embarque (15) → zeros
    169-174: Qtde CV/NSU acatados (6) → zeros
    """
    def num15(n: int) -> str:
        return str(max(0, n)).zfill(15)

    parts = [
        "026",                           # 001-003
        str(pv).zfill(9),                # 004-012
        "0".zfill(15),                   # 013-027: Total bruto
        "0".zfill(6),                    # 028-033: Qtde rejeitados
        "0".zfill(15),                   # 034-048: Total rejeitado
        "0".zfill(15),                   # 049-063: Total rotativo
        "0".zfill(15),                   # 064-078: Total parcelado s/ juros
        "0".zfill(15),                   # 079-093: Total IATA
        "0".zfill(15),                   # 094-108: Total dólar
        "0".zfill(15),                   # 109-123: Total desconto
        num15(total_liquido_cent),       # 124-138: Total líquido ★
        "0".zfill(15),                   # 139-153: Total gorjeta
        "0".zfill(15),                   # 154-168: Total taxa embarque
        "0".zfill(6),                    # 169-174: Qtde CV/NSU
    ]
    return "".join(parts)


# =============================================================================
#  🔹 Processador EEVC - Função principal
# =============================================================================

def process_eevc(input_path: str, output_dir: str, error_dir: str = "erro") -> dict:
    """
    Processa arquivo EEVC (Vendas Crédito) v4.5 compatível com NSA.
    
    Funcionalidades:
    - Divide arquivo mestre por PV (registro 004 até 026)
    - Soma SOMENTE valores de RVs (006/010/016/022) para validar com trailer 028
    - Inclui todos os tipos de registro definidos no manual EEVC v4.5
    - Recalcula trailer 026 por PV com base nos RVs processados
    - Gera arquivos filhos em output/NSA_<nsa>/<PV>_<data>_<nsa>_EEVC.txt
    
    Args:
        input_path: Caminho completo do arquivo EEVC de entrada
        output_dir: Diretório base para saída dos arquivos filhos
        error_dir: Diretório para logs de erro (reservado para uso futuro)
    
    Returns:
        dict com metadados do processamento:
        {
            "arquivo": str,
            "data_ref": str,
            "nsa": str,
            "total_trailer": int,
            "total_processado": int,
            "status": "OK" | "ERRO",
            "detalhe": str,
            "gerados": list[str]
        }
    
    Raises:
        ValueError: Se arquivo vazio, ou sem header/trailer obrigatórios
    """
    print("🟢 Processando EEVC (Vendas Crédito)")
    filename = os.path.basename(input_path)

    # Leitura do arquivo com encoding latin-1 (padrão EEVC)
    with open(input_path, "r", encoding="latin-1", errors="replace") as f:
        lines = [l.rstrip("\n") for l in f if l.strip()]

    if not lines:
        raise ValueError("Arquivo EEVC vazio.")

    # Variáveis de estado
    header_line = None
    trailer_line = None
    grupos = defaultdict(list)  # pv -> lista de linhas
    totais_pv = defaultdict(lambda: {"liquido_rv": 0})
    current_pv = None

    # Mapeamento de tipos conforme manual EEVC v4.5
    TIPOS_RV = {"006", "010", "016", "022"}  # RVs com cálculo de valor líquido
    TIPOS_DETALHE = {
        # Request / Avisos
        "005", "033",
        # RVs (já em TIPOS_RV, mas incluídos para completude)
        "006", "010", "016", "022",
        # CV/NSU e Parcelas
        "008", "012", "018", "014", "024",
        # E-Commerce / Recarga / Ajustes
        "034", "040", "011", "035", "036",
        # Serviços adicionais
        "017", "019", "020", "021", "029",
    }

    # =====================================================================
    #  🔁 Loop principal de processamento
    # =====================================================================
    for line in lines:
        if len(line) < 3:
            continue  # Ignora linhas malformadas
        tipo = line[:3]

        if tipo == "002":  # Header do Arquivo
            header_line = line

        elif tipo == "004":  # Header Matriz (início de bloco PV)
            pv = line[3:12].strip()
            if pv and pv.isdigit() and len(pv) == 9:
                current_pv = pv
                grupos[pv].append(line)
            else:
                # PV inválido: ignora registro mas mantém estado anterior
                continue

        elif tipo in TIPOS_RV:  # RVs: calcula líquido e agrupa
            if current_pv:
                tot = _liquido_rv(line)
                totais_pv[current_pv]["liquido_rv"] += tot
                grupos[current_pv].append(line)

        elif tipo in TIPOS_DETALHE:  # Demais registros detalhe
            # Fallback defensivo: se current_pv=None, tenta extrair PV do próprio registro
            pv_ativo = current_pv
            if pv_ativo is None and len(line) >= 12:
                fallback = line[3:12].strip()
                if fallback.isdigit() and len(fallback) == 9:
                    pv_ativo = fallback
                    current_pv = fallback  # Restaura contexto para próximos registros

            if pv_ativo and pv_ativo.isdigit():
                if tipo in TIPOS_RV:
                    totais_pv[pv_ativo]["liquido_rv"] += _liquido_rv(line)
                grupos[pv_ativo].append(line)

        elif tipo == "026":  # Totalizador por PV
            # ✅ CORREÇÃO CRÍTICA: NÃO resetar current_pv!
            # O 026 é um totalizador intermediário, não delimita fim de PV
            if current_pv:
                grupos[current_pv].append(line)

        elif tipo == "028":  # Trailer do Arquivo
            trailer_line = line

        else:  # Tipos não mapeados ou desconhecidos
            if current_pv:
                grupos[current_pv].append(line)

    # Validação de estrutura mínima
    if not header_line or not trailer_line:
        raise ValueError("Header (002) ou Trailer (028) ausentes no arquivo EEVC.")

    # Extração de metadados
    data_ref, nsa = _extract_data_nsa(header_line, filename)

    # =====================================================================
    #  📤 Geração dos arquivos filhos
    # =====================================================================
    gerados = []
    soma_total_processado = 0

    # Cria subdiretório NSA_<nsa> (padrão NETUNNA para conciliação)
    subdir = os.path.join(output_dir, f"NSA_{nsa}")
    os.makedirs(subdir, exist_ok=True)

    for pv, blocos in grupos.items():
        # Ajuste de escala: EEVC armazena valores com 1 casa decimal extra
        total_liquido_rv = totais_pv[pv]["liquido_rv"] // 10
        soma_total_processado += total_liquido_rv

        # Reescreve header com PV filho
        header_pv = _rewrite_header_with_pv(header_line, pv)
        # Monta trailer 026 específico para este PV
        trailer_026 = _build_trailer_026(pv, total_liquido_rv)

        # Nome do arquivo: <PV>_<DDMMAA>_<NSA>_EEVC.txt
        nome_arquivo = f"{pv}_{data_ref}_{nsa}_EEVC.txt"
        out_path = os.path.join(subdir, nome_arquivo)

        # Escrita do arquivo filho
        with open(out_path, "w", encoding="latin-1", errors="ignore") as f:
            f.write(header_pv + "\n")
            for l in blocos:
                # Evita duplicar 026: o trailer recalculado substitui o original
                if not l.startswith("026"):
                    f.write(l + "\n")
            f.write(trailer_026 + "\n")
            f.write(trailer_line + "\n")  # Trailer global do arquivo

        gerados.append(out_path)
        print(f"🧾 Gerado: {os.path.basename(out_path)} → {subdir} | Líquido RVs: {total_liquido_rv}")

    # =====================================================================
    #  ✅ Validação final com trailer 028
    # =====================================================================
    # Extrai total do trailer 028 (pos. 133-147, 15 dígitos)
    total_trailer_str = trailer_line[133:148].strip() if len(trailer_line) >= 148 else "0"
    total_trailer = int(total_trailer_str) if total_trailer_str.isdigit() else 0

    detalhe = validar_totais(total_trailer, soma_total_processado)
    status = "OK" if total_trailer == soma_total_processado else "ERRO"

    print(f"✅ EEVC — Trailer(028): {total_trailer} | Processado(RVs): {soma_total_processado} | {status}")

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
