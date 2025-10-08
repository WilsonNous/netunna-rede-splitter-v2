# 🧩 Netunna REDE Splitter v2

Sistema automatizado para:
- Separar arquivos EEVC, EEVD e EEFI da REDE;
- Validar totais com o trailer do arquivo agrupado;
- Registrar logs;
- Mover arquivos processados para saída ou erro;
- Enviar alertas por e-mail;
- E rodar automaticamente todo dia às 03h.

## 🚀 Estrutura
📦 netunna-rede-splitter-v2/
├── app.py
├── splitter_core.py
├── validator.py
├── mover.py
├── notifier.py
├── logger.py
├── scheduler.py
├── input/
├── output/
├── erro/
└── logs/

## 🌐 Endpoints
| Endpoint | Método | Função |
|-----------|---------|--------|
| `/api/scan` | GET | Lista arquivos aguardando |
| `/api/process` | POST | Processa arquivo específico |
| `/api/status` | GET | Últimos logs de execução |

## 🕓 Execução Automática
A rotina roda todo dia às 03h via APScheduler.
