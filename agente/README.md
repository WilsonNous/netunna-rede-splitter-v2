# 🧠 Agente Netunna Splitter v4

Componente auxiliar do projeto **Netunna Splitter**, responsável por:

- Monitorar diretório local de entrada (`input`)
- Enviar arquivos ao Splitter (upload)
- Baixar e extrair resultados processados (download)
- Expor endpoints REST para controle via painel web

## 🚀 Modos de execução

### Manual
```bash
python agente/main.py

API REST (controle remoto)
python agente/api.py

Monitoramento contínuo
python agente/watcher.py

Configuração

Copie .env.example para .env e ajuste os caminhos e URLs conforme o ambiente.

Netunna Software © 2025 — Flexibilidade | Segurança | Escalabilidade | Performance


---

## ✅ Pronto para commit

No terminal Git:

```bash
cd netunna-rede-splitter-v2
git checkout -b feature/agente
git add agente/
git commit -m "feat(agente): estrutura inicial do Agente Netunna v4"
git push origin feature/agente
