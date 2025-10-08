import os
from datetime import datetime

def log_operation(filename, valido, resumo):
    os.makedirs('logs', exist_ok=True)
    path = os.path.join('logs', 'operacoes.csv')
    with open(path, 'a', encoding='utf-8') as f:
        f.write(f'{datetime.now().isoformat()};{filename};{valido};{resumo}\n')
