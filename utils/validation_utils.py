def format_reais(valor_centavos: int) -> str:
    """Formata valor em centavos para R$ 0,00."""
    return f"R$ {valor_centavos / 100:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def validar_totais(total_trailer: int, total_processado: int) -> str:
    """Valida soma de valores entre filhos e trailer do arquivo-mãe."""
    if total_trailer == total_processado:
        return "Validação OK — valores totais consistentes."
    
    diff = total_processado - total_trailer
    abs_diff = abs(diff)
    direcao = "a maior" if diff > 0 else "a menor"

    return f"Divergência de valor: {format_reais(abs_diff)} ({direcao})."
