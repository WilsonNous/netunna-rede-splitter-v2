def validar_totais(total_trailer, total_processado):
    """Valida soma de valores entre filhos e trailer do arquivo-mãe."""
    if total_trailer == total_processado:
        return "Validação OK — valores totais consistentes."
    diff = total_processado - total_trailer
    return f"Divergência de valor: {diff:+,} centavos."
