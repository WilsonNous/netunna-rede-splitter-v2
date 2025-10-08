import os

def validate_file(original_path, generated_files):
    """Compara o total de registros originais com os separados."""
    try:
        total_registros = sum(1 for _ in open(original_path, encoding='utf-8', errors='ignore'))
        total_separados = sum(sum(1 for _ in open(f, encoding='utf-8', errors='ignore')) for f in generated_files)
        is_valid = abs(total_registros - total_separados) <= len(generated_files)
        resumo = f"Registros originais: {total_registros}, separados: {total_separados}"
        return is_valid, resumo, total_registros, total_separados
    except Exception as e:
        return False, f"Erro na validação: {e}", 0, 0
