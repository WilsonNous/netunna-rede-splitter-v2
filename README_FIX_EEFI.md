# Correção EEFI - 27/08/2026

Problema identificado no arquivo FI real:

- o registro 045 estava sendo lido com faixa de valor incorreta;
- registros 035 e 045 estavam sendo subtraídos do fechamento financeiro;
- no arquivo analisado, o trailer 052 fecha exatamente com 034 + 036;
- 035 e 045 permanecem nos arquivos filhos, mas não compõem o total financeiro do trailer 052.

Teste com:
VENTUNOFORTE_20770677_FI_27082026368.TXT

Resultado esperado:
- NSA: 367
- Filhos: 68
- sum_pvs: 350401152
- total_052: 350401152
- ok: true
