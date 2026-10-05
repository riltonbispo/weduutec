# Weduu technical challenge

## Rodando o simulador

Com o Redis e a API local em execução, inicie a plataforma simulada:

```bash
docker compose up -d redis
npm run dev
npm run mock
```

Em outro terminal, registre o webhook e use o `cid` e o `token` retornados para iniciar um lote:

```bash
curl -X POST http://localhost:5000/register \
  -H 'content-type: application/json' \
  -d '{"name":"Local test","webhook":"http://localhost:4000"}'

curl -X POST http://localhost:5000/burst/<cid> \
  -H 'x-token: <token>'
```

As opções do simulador podem ser alteradas por `MOCK_PORT`, `TOTAL`, `DUP_RATE`, `ERROR_RATE`, `SEED`, `DROP_SEQS`, `INVALID_SKU_SEQS` e `EARLY_DISPATCH`. Relatórios de callback são gravados em `reports/`.

## Rodando o worker

O worker de enrich roda em um processo separado da API. Com `REDIS_URL`, `WEDUU_BASE_URL`, `WEDUU_CID` e `WEDUU_TOKEN` configurados no ambiente, execute:

```bash
npm run dev:worker
```

Para uma execução sem watch, use `npm run worker`. A API continua sendo iniciada separadamente com `npm run dev`.
