# AGENTS.md — Regras para agentes de desenvolvimento

Este arquivo define as regras que qualquer agente de código deve seguir ao trabalhar neste repositório.

Leia `SPEC.md` antes de alterar qualquer código.

Leia também `docs/decisions.md` antes de introduzir uma nova dependência, mudar a arquitetura ou alterar um invariante.

---

# 1. Objetivo do repositório

Implementar o desafio técnico de integração da Weduu em Node.js/TypeScript, priorizando:

- ACK rápido;
- processamento assíncrono;
- idempotência;
- concorrência controlada;
- retries previsíveis;
- ausência de perda silenciosa;
- código simples de explicar em entrevista.

Não otimizar para complexidade hipotética antes do fluxo real funcionar.

---

# 2. Stack padrão

Use esta stack salvo decisão registrada em `docs/decisions.md`:

- Node.js LTS;
- TypeScript com `strict: true`;
- Fastify para HTTP;
- Zod para validação;
- BullMQ para fila;
- Redis para fila, idempotência e estado transitório da execução;
- cliente HTTP explícito para integração com Weduu (ex.: `fetch` nativo ou Axios, escolher um só);
- Vitest para testes;
- Docker Compose para dependências locais, especialmente Redis.

Evite adicionar bibliotecas quando a plataforma padrão do Node resolve de forma clara.

---

# 3. Estrutura de pastas esperada

```text
src/
  server.ts
  app.ts

  config/
    env.ts

  routes/
    check.route.ts
    process.route.ts

  schemas/
    check.schema.ts
    process.schema.ts

  clients/
    weduu.client.ts

  queues/
    sku.queue.ts

  workers/
    sku.worker.ts

  services/
    process-message.service.ts
    enrich-sku.service.ts
    run-finalizer.service.ts
    callback.service.ts

  repositories/
    run.repository.ts
    item.repository.ts

  domain/
    run.ts
    item.ts
    errors.ts

  lib/
    logger.ts
    retry.ts
    sleep.ts

tests/
  unit/
  integration/

docs/
  decisions.md
```

A estrutura pode evoluir, mas não criar camadas vazias ou abstrações sem uso real.

---

# 4. Invariantes obrigatórios

Todo agente deve preservar estas regras em qualquer alteração.

## 4.1 `/process` não processa SKU sincronamente

`POST /process` deve apenas:

1. validar;
2. deduplicar;
3. enfileirar;
4. responder `200`.

O request path de `/process` nunca deve chamar:

- `/enrich`;
- `/callback`;
- qualquer operação lenta que dependa da plataforma externa.

Meta: ACK muito abaixo do SLA de 600 ms.

## 4.2 Idempotência

A chave de idempotência é obrigatoriamente:

```text
run_id:seq
```

A mesma mensagem pode chegar mais de uma vez.

Duplicatas não podem causar efeitos duplicados.

## 4.3 Concorrência global

Nunca permitir mais de **3 chamadas simultâneas** ao `/enrich` em todo o processo.

Não implementar limite de 3 por `run_id` se múltiplos runs puderem somar mais de 3 chamadas.

## 4.4 Retry

- `429`: respeitar `retry-after`;
- `500`: retry com backoff + jitter + limite;
- `401`/`404`: não repetir indefinidamente;
- todo retry precisa ser observável em logs/testes.

## 4.5 Finalização de run

Callback apenas quando todos os `seq` esperados entre `0` e `total - 1` estiverem resolvidos.

O payload deve ser ordenado por `seq`.

A operação de "verificar conclusão + marcar callback" deve ser protegida contra corrida.

## 4.6 Nenhum item perdido silenciosamente

Todo item precisa terminar em estado conhecido.

Falha definitiva deve ser registrada e consultável/logada.

---

# 5. Regras de código

## TypeScript

- não usar `any` sem justificativa local;
- preferir `unknown` + validação;
- `strict: true`;
- funções pequenas e com responsabilidade clara;
- separar tipos de transporte HTTP de tipos de domínio quando isso melhorar clareza;
- não esconder erros com `catch {}` vazio;
- não usar `as SomeType` para substituir validação de payload externo.

## Validação

Todo payload de entrada deve ser validado na borda.

Para `/process`, validar no mínimo:

- `run_id`: string não vazia;
- `seq`: inteiro >= 0;
- `sku`: string não vazia.

Para `/check`:

- `token`: string não vazia.

## Logs

Preferir logs estruturados.

Incluir contexto quando aplicável:

```text
run_id
seq
sku
attempt
status_code
duration_ms
```

Nunca logar segredo completo (`token`).

---

# 6. Regras da fila

Ao criar jobs de SKU:

- usar id lógico baseado em `run_id:seq` quando a tecnologia permitir;
- não criar duplicata efetiva para o mesmo item;
- retry precisa ser limitado;
- não remover falhas sem antes persistir/registrar o estado final;
- worker deve usar concorrência compatível com o limite global de 3.

Se a implementação usar mais de um worker/processo, o limitador de 3 deve continuar sendo global. Não assumir que `concurrency: 3` em cada processo satisfaz o requisito.

---

# 7. Regras para integração HTTP

Centralizar chamadas da plataforma em `src/clients/weduu.client.ts` ou equivalente.

Esse cliente deve encapsular:

- base URL;
- headers;
- serialização;
- timeouts;
- interpretação de status;
- erro tipado/classificado.

Não espalhar `fetch()`/Axios diretamente por routes e services.

---

# 8. Estratégia de erros

Classificar erros em pelo menos:

```text
transient
rate_limited
permanent
```

Exemplo:

- `429` -> `rate_limited`;
- `500` -> `transient`;
- `401` -> `permanent`;
- `404` -> `permanent`.

Não criar retry infinito.

Não marcar como sucesso um item cujo enrich não retornou dados válidos.

---

# 9. Testes obrigatórios

Antes de considerar uma feature pronta, cobrir o comportamento relevante.

## `/check`

- ecoa token;
- rejeita payload inválido.

## `/process`

- responde `200` para payload válido;
- não espera enrich;
- mensagens duplicadas não criam efeito duplicado;
- aceita mensagens fora de ordem.

## Concorrência

Teste que nunca existam mais de 3 chamadas simultâneas ao enrich.

O teste deve medir concorrência em voo, não apenas quantidade total de chamadas.

## Retry

- `500` sofre retry;
- retry tem limite;
- `429` respeita `retry-after`;
- `401` e `404` não entram em retry infinito.

## Finalização

- não chama callback antes de todos os `seq`;
- detecta lacuna de `seq`;
- ordena resultados por `seq`;
- duplicata não incrementa conclusão duas vezes;
- callbacks concorrentes não geram duplo envio.

## Falha definitiva

- item termina em estado `failed` ou equivalente;
- causa e tentativas ficam registradas.

---

# 10. Comandos esperados

O projeto deve expor scripts equivalentes a:

```bash
npm install
npm run dev
npm run build
npm run start
npm test
npm run test:watch
npm run lint
npm run typecheck
```

Se algum comando mudar, atualizar este arquivo e o README.

Para dependências locais:

```bash
docker compose up -d redis
```

Ou, se o compose subir toda a aplicação:

```bash
docker compose up --build
```

---

# 11. Definição de pronto para cada alteração

Antes de finalizar uma tarefa:

1. confirmar aderência a `SPEC.md`;
2. rodar testes relevantes;
3. rodar typecheck;
4. rodar lint se configurado;
5. evitar regressão no SLA de `/process`;
6. atualizar `docs/decisions.md` se houve decisão arquitetural nova;
7. atualizar README se mudou forma de execução/configuração.

---

# 12. O que NÃO fazer

Não:

- chamar `/enrich` dentro da route `/process`;
- esperar todos os SKUs dentro de uma request HTTP;
- usar `Promise.all` com todos os SKUs sem limitador;
- assumir ordem de chegada;
- usar apenas `sku` como idempotency key;
- usar contador simples de mensagens como prova de completude sem validar `seq`;
- tratar duplicata como novo item;
- ignorar `retry-after`;
- retry infinito;
- engolir exceções;
- registrar token completo em logs;
- criar callback parcial;
- enviar callback sem ordenar por `seq`;
- criar frontend antes do backend ponta a ponta estar funcionando;
- adicionar Kafka, Kubernetes, microserviços ou banco complexo sem necessidade demonstrável;
- refatorar código não relacionado à tarefa atual;
- mudar contratos definidos em `SPEC.md` para "facilitar" a implementação.

---

# 13. Ordem recomendada de implementação

Agentes devem preferir tarefas pequenas e verificáveis nesta ordem:

1. bootstrap Node/TypeScript/Fastify;
2. config/env;
3. `POST /check`;
4. `POST /process` com validação e ACK;
5. Redis/BullMQ;
6. idempotência `run_id:seq`;
7. cliente Weduu;
8. worker de enrich;
9. limite global de concorrência 3;
10. retry de 500 e 429;
11. persistência de resultado;
12. controle de `run_id` + `total`;
13. agregação/completude;
14. callback;
15. testes de corrida/duplicata;
16. scripts de register/burst ou comandos de apoio;
17. Docker/README;
18. executar lote real e guardar relatório.

Não avançar para otimizações sofisticadas se o fluxo anterior ainda não estiver testado.

---

# 14. Regra para mudanças arquiteturais

Se uma alteração mudar qualquer um destes pontos:

- fila;
- persistência;
- modelo de idempotência;
- concorrência;
- estratégia de retry;
- forma de detectar conclusão;
- semântica do callback;

registrar um ADR curto em `docs/decisions.md` antes ou junto do código.

O ADR deve conter:

```text
Contexto
Decisão
Alternativas consideradas
Trade-offs
Consequências
```

---

# 15. Prompt-base para agentes

Ao pedir implementação a um agente, incluir ou referenciar estes invariantes:

```text
- /process nunca chama serviços externos de negócio: valida, deduplica, enfileira e responde 200.
- Idempotency key = run_id:seq.
- Máximo de 3 chamadas simultâneas ao /enrich globalmente.
- 429 respeita retry-after.
- 500 usa retry limitado com backoff + jitter.
- Callback somente após todos os seq 0..total-1 resolvidos, ordenado por seq.
- Nenhuma falha pode desaparecer silenciosamente; falha definitiva deve ficar registrada.
```

Se uma solução proposta violar qualquer uma dessas regras, rejeitar a solução e propor outra.
