import os

def validate_file(original_path, generated_files):
    try:
        total_registros = sum(1 for _ in open(original_path, encoding='utf-8'))
        total_separados = sum(sum(1 for _ in open(f, encoding='utf-8')) for f in generated_files)
        is_valid = abs(total_registros - total_separados) <= len(generated_files)
        resumo = f'Registros originais: {total_registros}, separados: {total_separados}'
        return is_valid, resumo
    except Exception as e:
        return False, f'Erro na validação: {e}'
