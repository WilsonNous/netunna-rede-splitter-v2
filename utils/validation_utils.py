def to_centavos(valor_str: str) -> int:
    """
    Converte string numérica do layout Rede (sem separadores) para inteiro em centavos.
    ATENÇÃO: os campos de valor no EEVD já vêm em CENTAVOS.
    Ex.: '000000000011013' -> 11013
    """
    if not valor_str:
        return 0
    s = valor_str.strip().replace(".", "").replace(",", "")
    return int(s) if s.isdigit() else 0


def format_reais(valor_centavos: int) -> str:
    """Formata inteiro em centavos para 'R$ 0,00' (pt-BR)."""
    return f"R$ {valor_centavos / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def validar_totais(total_trailer: int, total_processado: int) -> str:
    """Valida soma de valores entre filhos e trailer do arquivo-mãe (em centavos)."""
    if total_trailer == total_processado:
        return "Validação OK — valores totais consistentes."
    diff = total_processado - total_trailer
    direcao = "a maior" if diff > 0 else "a menor"
    return f"Divergência de valor: {format_reais(abs(diff))} ({direcao})."
