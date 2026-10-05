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
