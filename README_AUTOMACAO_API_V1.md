# Netunna REDE Splitter - API v1 e Agente Multi-Cliente

## Objetivo

Automatizar o fluxo:

`Pasta REDE no Windows -> API Splitter no Azure -> separação por estabelecimento -> validação -> download do batch -> pasta SMEDI -> arquivamento do arquivo mãe`

A implementação é multi-cliente. O Splitter não possui regra fixa para VENTUNO. O campo `cliente` apenas separa e rastreia batches. Novos clientes são adicionados no `agente/clientes.json`, sem alterar o código do backend.

## Segurança e isolamento

- API v1 protegida por `SPLITTER_API_KEY`.
- Cada upload recebe um `batch_id` único.
- Estrutura no Azure: `_api_batches/<cliente>/<batch_id>/NSA_<nsa>`.
- O mesmo NSA pode ser processado novamente sem misturar arquivos antigos.
- Clientes diferentes nunca compartilham o mesmo diretório de batch.
- O download retorna apenas os filhos do batch solicitado.
- O agente valida tamanho e SHA-256 antes de liberar arquivos na pasta observada pelo SMEDI.
- O arquivo mãe só é arquivado após a entrega completa dos filhos.

## Endpoints

### Health

`GET /api/v1/health`

Sem autenticação.

### Upload

`POST /api/v1/upload`

Header:

`X-API-Key: <chave>`

ou

`Authorization: Bearer <chave>`

Body multipart/form-data:

- `file` - arquivo mãe REDE
- `cliente` - identificador lógico, por exemplo `ventuno`

A resposta contém `batch_id`, `nsa`, `lote`, quantidade de filhos, metadados SHA-256 e URLs do batch.

### Manifest / arquivos do batch

`GET /api/v1/batches/<batch_id>/files?cliente=ventuno`

Protegido pela API Key.

### Download do batch

`GET /api/v1/batches/<batch_id>/download?cliente=ventuno`

Protegido pela API Key. Retorna ZIP contendo somente os arquivos filhos daquele processamento.

## Configuração do agente Windows

1. Copiar `agente/.env.example` para `.env` e preencher `SPLITTER_API_KEY`.
2. Copiar `agente/clientes.json.example` para `agente/clientes.json`.
3. Ajustar os diretórios reais de recebimento REDE, processados e SMEDI para cada cliente.
4. Executar inicialmente em modo único:

`AGENTE_RUN_MODE=once`

5. Depois da homologação, mudar para:

`AGENTE_RUN_MODE=watch`

## Adicionando outro cliente

Adicionar outro objeto em `clientes.json`:

```json
{
  "id": "cliente-x",
  "enabled": true,
  "input_dir": "C:\\EDI\\REDE\\CLIENTE_X\\entrada",
  "processed_dir": "C:\\EDI\\REDE\\CLIENTE_X\\processados",
  "error_dir": "C:\\EDI\\REDE\\CLIENTE_X\\erro",
  "smedi_dir": "C:\\SMEDI\\CLIENTE_X\\entrada",
  "filename_contains": ["CLIENTE_X"],
  "extensions": []
}
```

Nenhuma alteração no Azure é necessária apenas para cadastrar um novo cliente.
