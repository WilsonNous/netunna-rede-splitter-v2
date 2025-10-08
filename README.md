# 🧩 Netunna REDE Splitter v3

Sistema de separação, validação e monitoramento automático de arquivos REDE (EEVC, EEVD e EEFI).  
Versão **v3** — com painel web, logs completos, validação por contagem e envio de e-mails automáticos.

---

## 🚀 Funcionalidades

| Categoria | Descrição |
|------------|------------|
| 🗂️ Separação Automática | Divide arquivos EEVC, EEVD e EEFI por estabelecimento (PV). |
| 🧾 Validação | Compara total de registros processados com trailer. |
| 📧 Notificação Automática | Envia e-mail em caso de divergência de contagem. |
| 🧠 Painel Web | Exibe tabela de logs (OK/ERRO), download e controle manual. |
| 🗄️ Log CSV | Registra todas as operações em `logs/operacoes.csv`. |
| ⚙️ Integração | Suporte a uploads via agente local e endpoints API. |

---

## 📁 Estrutura de Diretórios

```
netunna-rede-splitter-v3/
├── app.py                        # API + Painel Web
├── splitter_core_v3.py           # Lógica principal
├── config_email.json             # Configuração SMTP (Outlook)
├── requirements.txt              # Dependências p/ Render
├── templates/
│   └── index.html                # Painel visual
├── input/                        # Recepção de arquivos
├── output/                       # Arquivos separados
├── erro/                         # Arquivos com divergência
└── logs/
    └── operacoes.csv             # Registro das execuções
```

---

## ⚙️ Configuração de E-mail

Editar `config_email.json` com as credenciais de envio via Outlook 365:

```json
{
  "smtp_server": "smtp.office365.com",
  "smtp_port": 587,
  "username": "automacao.edi@netunna.com.br",
  "password": "@Utomc@o",
  "recipients": ["wilson.martins@netunna.com.br"]
}
```

---

## 🖥️ Deploy no Render

- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `gunicorn app:app`
- **Port:** 10000

Após o deploy, acesse:
👉 [https://nn-rede-splitter-v2.onrender.com](https://nn-rede-splitter-v2.onrender.com)

---

## 🔗 Endpoints Principais

| Método | Rota | Descrição |
|--------|------|------------|
| `GET` | `/` | Painel Web com status e logs |
| `POST` | `/api/upload` | Upload de arquivo (via robô local) |
| `POST` | `/api/process` | Processa arquivo manualmente |
| `GET` | `/api/status` | Retorna logs CSV estruturados |
| `GET` | `/api/download/<arquivo>` | Download individual |
| `GET` | `/api/download-all` | Download ZIP de todos os arquivos |

---

## 🧩 Operação via Agente Local

O agente local (`rede_upload_agent.py`) pode enviar automaticamente os arquivos do diretório local:

```python
LOCAL_INPUT = r"C:\Users\WilsonMartins\Downloads\ventuno\Input"
LOCAL_SENT = r"C:\Users\WilsonMartins\Downloads\ventuno\Enviados"
API_URL = "https://nn-rede-splitter-v2.onrender.com/api/upload"
```

O envio é automático e o arquivo é movido para `/Enviados` após sucesso.

---

## 🧾 Estrutura do Log CSV

```
data_hora,arquivo,tipo,total_trailer,total_processado,status,detalhe
2025-10-09 03:02:15,EEVC_051025,EEVC,62,62,OK,Validação concluída sem divergências.
2025-10-09 03:03:41,EEFI_041025,EEFI,59,57,ERRO,Divergência na contagem de registros (2 registros faltando).
```

---

## 📧 Exemplo de E-mail Automático

**Assunto:** ⚠️ Divergência detectada no arquivo EEFI_041025

```
Olá, equipe EDI Netunna 👋

Durante o processamento automático, foi detectada uma divergência no arquivo EEFI_041025.

📁 Arquivo: EEFI_041025
📊 Tipo: EEFI
🔢 Total no trailer: 59
📈 Total processado: 57

🟠 Detalhe: Divergência na contagem de registros (2 registros faltando).

O arquivo foi movido para a pasta /erro para análise manual.
```

---

## 🔍 Logs Visuais no Painel

O painel exibe os logs com ícones:

| Status | Significado |
|---------|-------------|
| ✅ | Processamento concluído com sucesso |
| ❌ | Divergência detectada ou falha |

---

## 🧰 Créditos

Desenvolvido pela equipe **Netunna Automations**  
Sob direção técnica de **Wilson Martins**  
> "Automação inteligente, confiável e humana." 🤖💡
