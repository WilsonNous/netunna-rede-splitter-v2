# =============================================================================
#  📦 process_eevc.py - Processador de Arquivos EEVC (Vendas Crédito) v4.6
#  Autor: Wilson Martins | NETUNNA Software
#  Última atualização: 2026-08-27
#  Descrição:
#      Divide arquivo EEVC por PV, recalcula totais, valida trailer 028
#      e gera arquivos filhos preservando integridade dos registros 012.
#
#  Correção v4.6:
#      - Corrigida extração do campo "Valor líquido" dos registros RV.
#      - Faixa correta: Python [114:128], equivalente às posições 115-128.
#      - Removida compensação artificial por divisão /10.
#      - Validação agora fecha exatamente com o trailer 028.
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
    Extrai:
      - data de referência no formato DDMMAA
      - NSA com 3 posições

    A data é obtida prioritariamente do header 002.
    O NSA é obtido do header e, como fallback, do nome do arquivo.
    """
    data_ref = "000000"
    nsa = "000"

    # -------------------------------------------------------------------------
    # Data de referência
    # Exemplo header:
    # 00227082026...
    #
    # raw = 27082026
    # resultado = 270826
    # -------------------------------------------------------------------------
    if header_line.startswith("002") and len(header_line) >= 11:
        raw = header_line[3:11]

        if raw.isdigit() and len(raw) == 8:
            data_ref = f"{raw[:2]}{raw[2:4]}{raw[6:8]}"

    # -------------------------------------------------------------------------
    # NSA a partir do header
    #
    # Mantém a lógica compatível com o comportamento anterior.
    # -------------------------------------------------------------------------
    m = re.search(r"(\d{6})(\d{9})", header_line)

    if m:
        nsa_candidate = m.group(1)

        if nsa_candidate.isdigit():
            nsa = nsa_candidate[-3:]

    # -------------------------------------------------------------------------
    # Fallback pelo nome do arquivo
    #
    # Exemplo:
    # VENTUNOFORTE_20770677_VC_27082026367.TXT
    #
    # OBS:
    # mantém a regra anterior para compatibilidade.
    # -------------------------------------------------------------------------
    if nsa == "000":
        m2 = re.search(r"\.(\d{3})\D*$", filename)

        if m2 and m2.group(1).isdigit():
            nsa = m2.group(1)

    return data_ref, nsa


def _rewrite_header_with_pv(header_line: str, pv: str) -> str:
    """
    Substitui no header 002 o código do estabelecimento pelo PV filho.

    O PV é normalizado para 9 posições.
    """
    pv9 = str(pv).zfill(9)

    def repl(m):
        return f"{m.group(1)}{pv9}"

    new_header, count = re.subn(
        r"(\d{6})\d{9}",
        repl,
        header_line,
        count=1,
    )

    return new_header if count == 1 else header_line


def _liquido_rv(line: str) -> int:
    """
    Extrai o Valor Líquido dos registros RV:

        006
        010
        016
        022

    Layout EEVC:
        posições 115-128
        tamanho: 14 caracteres

    Em índice Python:
        line[114:128]

    IMPORTANTE:
        O campo já possui escala de centavos implícita.
        Portanto NÃO deve haver divisão posterior por 10.

    Erro existente até v4.5:
        line[114:129]

    Isso capturava 15 caracteres e incluía o primeiro dígito
    do campo seguinte, provocando diferença acumulada no fechamento.
    """
    if len(line) < 128:
        return 0

    campo = line[114:128]

    return to_centavos(campo)


def _build_trailer_026(pv: str, total_liquido_cent: int) -> str:
    """
    Monta o registro 026 - Totalizador por PV.

    O valor líquido do PV é informado no campo correspondente
    usando 15 posições numéricas.

    O restante permanece zerado conforme regra adotada pelo Splitter.
    """

    def num15(n: int) -> str:
        return str(max(0, n)).zfill(15)

    parts = [
        "026",

        str(pv).zfill(9),

        "0".zfill(15),
        "0".zfill(6),
        "0".zfill(15),
        "0".zfill(15),
        "0".zfill(15),
        "0".zfill(15),
        "0".zfill(15),
        "0".zfill(15),

        num15(total_liquido_cent),

        "0".zfill(15),
        "0".zfill(15),
        "0".zfill(6),
    ]

    return "".join(parts)


# =============================================================================
#  🔹 Processador EEVC - Função principal
# =============================================================================

def process_eevc(
    input_path: str,
    output_dir: str,
    error_dir: str = "erro",
) -> dict:
    """
    Processa arquivo EEVC - Vendas Crédito.

    Estratégia:
        1. lê arquivo mãe;
        2. identifica header 002;
        3. identifica trailer 028;
        4. separa registros por PV;
        5. soma valor líquido dos RVs;
        6. gera arquivo filho por PV;
        7. recalcula 026;
        8. preserva registros 012;
        9. valida soma dos filhos contra trailer 028;
       10. devolve resultado estruturado para API.
    """

    print("🟢 Processando EEVC (Vendas Crédito) v4.6")

    filename = os.path.basename(input_path)

    # =========================================================================
    # Leitura do arquivo
    # =========================================================================

    with open(
        input_path,
        "r",
        encoding="latin-1",
        errors="replace",
    ) as f:

        lines = [
            line.rstrip("\r\n")
            for line in f
            if line.strip()
        ]

    if not lines:
        raise ValueError("Arquivo EEVC vazio.")

    # =========================================================================
    # Estruturas de trabalho
    # =========================================================================

    header_line = None
    trailer_line = None

    grupos = defaultdict(list)
    totais_pv = defaultdict(int)

    audit = {
        "012_fonte": 0,
        "012_gerados": 0,
    }

    # =========================================================================
    # Tipos de registros
    # =========================================================================

    TIPOS_RV = {
        "006",
        "010",
        "016",
        "022",
    }

    TIPOS_VALIDOS = {
        "002",
        "004",
        "005",
        "033",
        "006",
        "008",
        "034",
        "040",
        "010",
        "011",
        "012",
        "035",
        "014",
        "016",
        "017",
        "018",
        "036",
        "019",
        "020",
        "021",
        "022",
        "024",
        "029",
        "026",
        "028",
    }

    # =========================================================================
    # Loop principal
    # =========================================================================

    for line in lines:

        if len(line) < 3:
            continue

        tipo = line[:3]

        # ---------------------------------------------------------------------
        # Header geral
        # ---------------------------------------------------------------------

        if tipo == "002":
            header_line = line
            continue

        # ---------------------------------------------------------------------
        # Trailer geral
        # ---------------------------------------------------------------------

        if tipo == "028":
            trailer_line = line
            continue

        # ---------------------------------------------------------------------
        # Tipo fora do layout processado
        # ---------------------------------------------------------------------

        if tipo not in TIPOS_VALIDOS:
            continue

        # ---------------------------------------------------------------------
        # Extração do PV
        #
        # Posições 004-012
        # Python [3:12]
        # ---------------------------------------------------------------------

        if len(line) < 12:
            continue

        pv = line[3:12].strip()

        if not pv.isdigit() or len(pv) != 9:
            continue

        # ---------------------------------------------------------------------
        # Guarda registro no grupo daquele PV
        # ---------------------------------------------------------------------

        grupos[pv].append(line)

        # ---------------------------------------------------------------------
        # Auditoria dos registros 012
        # ---------------------------------------------------------------------

        if tipo == "012":
            audit["012_fonte"] += 1

        # ---------------------------------------------------------------------
        # Soma líquida dos RVs
        #
        # IMPORTANTE:
        # não existe mais divisão /10.
        # ---------------------------------------------------------------------

        if tipo in TIPOS_RV:
            valor_liquido = _liquido_rv(line)
            totais_pv[pv] += valor_liquido

    # =========================================================================
    # Validação estrutural
    # =========================================================================

    if not header_line:
        raise ValueError("Header 002 ausente no arquivo EEVC.")

    if not trailer_line:
        raise ValueError("Trailer 028 ausente no arquivo EEVC.")

    # =========================================================================
    # Data / NSA
    # =========================================================================

    data_ref, nsa = _extract_data_nsa(
        header_line,
        filename,
    )

    # =========================================================================
    # Diretório NSA
    # =========================================================================

    subdir = os.path.join(
        output_dir,
        f"NSA_{nsa}",
    )

    os.makedirs(
        subdir,
        exist_ok=True,
    )

    # =========================================================================
    # Geração dos filhos
    # =========================================================================

    gerados = []

    soma_total_processado = 0

    for pv, blocos in grupos.items():

        # ---------------------------------------------------------------------
        # Total líquido correto do PV
        #
        # Antes:
        # round(totais_pv[pv] / 10)
        #
        # Agora:
        # o valor já está corretamente em centavos.
        # ---------------------------------------------------------------------

        total_liquido_rv = totais_pv[pv]

        soma_total_processado += total_liquido_rv

        # ---------------------------------------------------------------------
        # Header específico do PV
        # ---------------------------------------------------------------------

        header_pv = _rewrite_header_with_pv(
            header_line,
            pv,
        )

        # ---------------------------------------------------------------------
        # Novo trailer 026 do PV
        # ---------------------------------------------------------------------

        trailer_026 = _build_trailer_026(
            pv,
            total_liquido_rv,
        )

        # ---------------------------------------------------------------------
        # Nome do arquivo filho
        # ---------------------------------------------------------------------

        nome_arquivo = (
            f"{pv}_"
            f"{data_ref}_"
            f"{nsa}_"
            f"EEVC.txt"
        )

        out_path = os.path.join(
            subdir,
            nome_arquivo,
        )

        # ---------------------------------------------------------------------
        # Gravação
        # ---------------------------------------------------------------------

        with open(
            out_path,
            "w",
            encoding="latin-1",
            errors="ignore",
        ) as f:

            f.write(header_pv + "\n")

            for linha_bloco in blocos:

                # O 026 original não é replicado.
                # Geramos um novo 026 ao final.
                if linha_bloco.startswith("026"):
                    continue

                f.write(linha_bloco + "\n")

            f.write(trailer_026 + "\n")

            # Mantém trailer geral 028
            f.write(trailer_line + "\n")

        # ---------------------------------------------------------------------
        # Auditoria
        # ---------------------------------------------------------------------

        qtd_012_pv = sum(
            1
            for linha_bloco in blocos
            if linha_bloco.startswith("012")
        )

        audit["012_gerados"] += qtd_012_pv

        gerados.append(out_path)

        print(
            f"🧾 Gerado: {os.path.basename(out_path)} "
            f"| Líquido: {total_liquido_rv} "
            f"| 012: {qtd_012_pv}"
        )

    # =========================================================================
    # Trailer 028
    # =========================================================================

    total_trailer_str = (
        trailer_line[133:148].strip()
        if len(trailer_line) >= 148
        else "0"
    )

    total_trailer = (
        int(total_trailer_str)
        if total_trailer_str.isdigit()
        else 0
    )

    # =========================================================================
    # Validação financeira
    # =========================================================================

    detalhe = validar_totais(
        total_trailer,
        soma_total_processado,
    )

    status_totais = (
        "OK"
        if total_trailer == soma_total_processado
        else "ERRO"
    )

    # =========================================================================
    # Validação registros 012
    # =========================================================================

    status_012 = (
        "OK"
        if audit["012_fonte"] == audit["012_gerados"]
        else "ERRO"
    )

    # =========================================================================
    # Status final
    # =========================================================================

    status_final = (
        "OK"
        if (
            status_totais == "OK"
            and status_012 == "OK"
        )
        else "ERRO"
    )

    # =========================================================================
    # Logs
    # =========================================================================

    print(
        f"✅ EEVC — Trailer(028): {total_trailer} "
        f"| Processado: {soma_total_processado} "
        f"| {status_totais}"
    )

    print(
        f"🔍 Auditoria 012 — "
        f"Fonte: {audit['012_fonte']} "
        f"| Gerados: {audit['012_gerados']} "
        f"| {status_012}"
    )

    if status_totais == "ERRO":
        diferenca = soma_total_processado - total_trailer

        print(
            f"⚠️ Divergência financeira EEVC: "
            f"{diferenca} centavos"
        )

    # =========================================================================
    # Retorno
    # =========================================================================

    return {
        "arquivo": filename,
        "data_ref": data_ref,
        "nsa": nsa,

        "total_trailer": total_trailer,
        "total_processado": soma_total_processado,

        "status": status_final,

        "detalhe": (
            f"{detalhe} "
            f"| 012:{status_012}"
        ),

        "gerados": gerados,

        "audit": audit,
    }
