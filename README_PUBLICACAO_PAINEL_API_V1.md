# Publicação da API v1 no painel multi-cliente

## Objetivo

A API continua processando cada arquivo em um batch isolado:

`/home/site/azurefiles/output/_api_batches/<cliente>/<batch_id>/NSA_xxx`

Após processamento `OK` e integridade aprovada, a aplicação publica uma cópia oficial para a árvore lida pelo painel:

- mãe: `/home/site/azurefiles/clientes/<cliente>/input/`
- filhos: `/home/site/azurefiles/clientes/<cliente>/output/NSA_xxx/`

O batch técnico não é removido nem alterado.

## Segurança contra colisões

Antes de copiar qualquer arquivo, a aplicação faz preflight de todos os destinos.

- se o destino não existe: publica;
- se existe e o SHA-256 é igual: considera já publicado;
- se existe e o SHA-256 é diferente: aborta a publicação e retorna erro, sem sobrescrever.

A cópia usa arquivo temporário no destino e `os.replace` após validar SHA-256.

## Resposta da API

O manifest passa a conter o bloco `publicacao`, por exemplo:

```json
{
  "publicacao": {
    "ok": true,
    "cliente": "ventuno",
    "filhos_publicados": 68,
    "copiados": 69,
    "ja_existentes_mesmo_hash": 0
  }
}
```

## Teste recomendado

Após o deploy, reenvie o FI controlado com:

- `Authorization: Bearer <token>`
- `file`: `VENTUNOFORTE_20770677_FI_27082026368.TXT`
- `cliente`: `ventuno`

Depois atualize o painel e confirme o `NSA_367`.
