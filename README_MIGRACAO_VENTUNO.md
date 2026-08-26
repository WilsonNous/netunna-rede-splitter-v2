# Migração segura do legado para VENTUNO

Esta versão do painel é compatível ao mesmo tempo com:

- legado: `/home/site/azurefiles/input` e `/home/site/azurefiles/output/NSA_*`;
- multi-cliente: `/home/site/azurefiles/clientes/<cliente>/input` e `/home/site/azurefiles/clientes/<cliente>/output/NSA_*`.

Por isso a ordem segura é:

1. publicar esta versão do código;
2. confirmar `/api/v1/health`;
3. abrir o painel e confirmar que os arquivos antigos continuam visíveis como `VENTUNO`;
4. executar a migração primeiro em DRY-RUN;
5. revisar o relatório e confirmar `CONFLICT=0` e `ERROR=0`;
6. executar com `--apply`;
7. atualizar o painel e conferir as quantidades.

## Variáveis recomendadas no Azure

```text
DEFAULT_CLIENT_ID=ventuno
LEGACY_CLIENT_ID=ventuno
```

## Simulação — não altera arquivos

No SSH/Console do App Service, na pasta da aplicação:

```bash
python migrate_legacy_to_client.py --cliente ventuno
```

O resultado termina com algo como:

```text
Modo: DRYRUN
MOVE=123 DUPLICATE=0 CONFLICT=0 ERROR=0
Nenhum arquivo foi alterado.
```

O relatório detalhado fica em:

```text
/home/site/azurefiles/migration_logs/
```

## Executar a migração

Somente depois de revisar o DRY-RUN:

```bash
python migrate_legacy_to_client.py --cliente ventuno --apply
```

## O que é migrado

```text
/home/site/azurefiles/input/*
  -> /home/site/azurefiles/clientes/ventuno/input/

/home/site/azurefiles/output/NSA_*/*
  -> /home/site/azurefiles/clientes/ventuno/output/NSA_*/

/home/site/azurefiles/erro/*
  -> /home/site/azurefiles/clientes/ventuno/erro/
```

## O que NÃO é alterado

- `/home/site/azurefiles/output/_api_batches/` — batches da API v1 continuam exatamente onde estão;
- `/home/site/azurefiles/logs/` — logs existentes continuam globais nesta fase;
- arquivos com conflito de nome e conteúdo diferente — ficam no legado para análise manual.

## Regra de conflito

- destino não existe: move;
- destino existe e SHA-256 é igual: considera duplicado e remove a cópia legada somente no `--apply`;
- destino existe e SHA-256 é diferente: não sobrescreve e registra `CONFLICT`.
