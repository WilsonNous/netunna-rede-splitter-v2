def format_reais(valor_centavos: int) -> str:
    """Formata valor em centavos para o formato monetário brasileiro (R$ 0,00)."""
    try:
        return f"R$ {valor_centavos / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "R$ 0,00"


def validar_totais(total_trailer: int, total_processado: int, tolerancia_centavos: int = 0) -> str:
    """
    Valida soma de valores entre trailers filhos e trailer do arquivo-mãe.
    Permite definir uma tolerância mínima em centavos (padrão = 0).
    """
    diff = total_processado - total_trailer
    abs_diff = abs(diff)

    # Aplica tolerância opcional
    if abs_diff <= tolerancia_centavos:
        return f"Validação OK — diferença dentro da tolerância ({format_reais(abs_diff)})."

    if diff == 0:
        return "Validação OK — valores totais consistentes."

    direcao = "a maior" if diff > 0 else "a menor"
    return f"Divergência de valor: {format_reais(abs_diff)} ({direcao})."
