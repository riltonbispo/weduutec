# Integração Weduu

Serviço Node.js/TypeScript para receber SKUs com ACK rápido.
Redis e BullMQ cuidam de deduplicação, estado e processamento assíncrono.
O worker limita a concorrência, aplica retries e finaliza cada run por callback.

```text
Weduu -> API -> Redis/BullMQ -> worker -> /enrich -> finalizer -> /callback
```

## Pré-requisitos

- Node.js 24
- Docker com Docker Compose
- ngrok para expor a porta 4000 durante o teste real

## Execução

1. Crie a configuração e substitua `WEDUU_BASE_URL` pela URL indicada no desafio:

   ```bash
   cp .env.example .env
   docker compose up --build -d
   ```

2. Exponha a API:

   ```bash
   ngrok http 4000
   ```

3. Registre o webhook usando a URL HTTPS do ngrok:

   ```bash
   npm run register -- https://xxxx.ngrok.app "Seu Nome"
   ```

4. Copie `WEDUU_CID` e `WEDUU_TOKEN` exibidos para `.env` e recrie o worker:

   ```bash
   docker compose up -d --force-recreate worker
   ```

5. Solicite um lote e consulte o `run_id` retornado:

   ```bash
   npm run burst
   npm run status -- <run_id>
   ```

As respostas brutas de callbacks bem-sucedidos ficam em `reports/<run_id>__attempt-<n>.json` ou `.txt`. Para desenvolvimento local também é possível subir apenas a dependência: `docker compose up -d redis`.

## Testes e simulador

Os testes de integração precisam do Redis:

```bash
docker compose up -d redis
npm ci
npm test
npm run typecheck
npm run lint
```

O simulador determinístico da plataforma roda com:

```bash
npm run mock
```

As opções do simulador estão em `MOCK_PORT`, `TOTAL`, `DUP_RATE`, `ERROR_RATE`, `SEED`, `DROP_SEQS`, `INVALID_SKU_SEQS` e `EARLY_DISPATCH`.

## Configuração

| Variável                                                | Padrão                   | Uso                               |
| ------------------------------------------------------- | ------------------------ | --------------------------------- |
| `PORT`                                                  | `4000`                   | Porta da API                      |
| `LOG_LEVEL`                                             | `info`                   | Nível dos logs estruturados       |
| `REDIS_URL`                                             | `redis://127.0.0.1:6379` | Redis/BullMQ                      |
| `REDIS_COMMAND_TIMEOUT_MS` / `REDIS_CONNECT_TIMEOUT_MS` | `175` / `75`             | Fail-fast do `/process`           |
| `WEDUU_BASE_URL`                                        | sem padrão               | URL da plataforma                 |
| `WEDUU_CID` / `WEDUU_TOKEN`                             | sem padrão               | Credenciais do worker             |
| `ENRICH_TIMEOUT_MS`                                     | `5000`                   | Timeout do `/enrich`              |
| `ENRICH_MAX_ATTEMPTS`                                   | `5`                      | Limite de tentativas transitórias |
| `ENRICH_BACKOFF_BASE_MS` / `ENRICH_BACKOFF_MAX_MS`      | `500` / `8000`           | Backoff do enrich                 |
| `ENRICH_RATE_LIMIT_MAX_WAITS`                           | `20`                     | Limite de esperas por `429`       |
| `CALLBACK_TIMEOUT_MS` / `CALLBACK_LEASE_MS`             | `10000` / `30000`        | Timeout e lease do callback       |
| `CALLBACK_MAX_ATTEMPTS` / `CALLBACK_BACKOFF_BASE_MS`    | `5` / `1000`             | Retry do callback                 |
| `SWEEP_INTERVAL_MS` / `RUN_STALL_TIMEOUT_MS`            | `2000` / `60000`         | Varredura e detecção de lacunas   |

Detalhes: [decisões arquiteturais](docs/decisions.md) e [roteiro de entrega](docs/entrega.md).
