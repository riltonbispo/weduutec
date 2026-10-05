## ADR-001 — Stack: Fastify + BullMQ + Redis

**Contexto.** O serviço precisa responder ACK em menos de 600 ms e processar de forma assíncrona itens que dependem de uma API lenta (400–800 ms), limitada a 3 chamadas simultâneas e instável (~10% de `500`).

**Decisão.** Fastify (HTTP e validação na borda), Redis como armazenamento de estado e idempotência, BullMQ como fila e executor de retries/delays.

**Alternativas consideradas.**

- Fila em memória com `p-limit`: mais simples, zero dependências, mas perde itens se o processo reiniciar e não escala para vários processos.
- Fila em banco relacional (outbox + polling): durável, porém mais latência e mais código para locks e polling.
- SQS/RabbitMQ: adequados em produção, pesados para o escopo e para rodar localmente.

**Trade-offs.**

- O caminho do ACK passa a depender de Redis (escrita de ~1–3 ms em rede local). Se o Redis cair, `/process` não consegue enfileirar.
- Mais uma peça de infraestrutura para subir (Docker Compose resolve).
- Para 20 itens é mais do que o necessário; a escolha é deliberada por durabilidade em restart e por demonstrar o desenho que escala.

**Consequências.**

- Itens sobrevivem a restart do processo.
- Retries com delay e backoff vêm da biblioteca, não de código próprio.
- Jobs concluídos são removidos após uma hora ou ao exceder 1.000 registros; jobs falhos permanecem para diagnóstico. O estado durável continua no hash do item.
- Os hashes de item deverão receber TTL após o encerramento do ciclo de vida. Para lotes de 20.000 SKUs, isso evita crescimento ilimitado do Redis sem apagar estado enquanto o run ainda está ativo.
- **Se o lote tivesse 20.000 SKUs:** enfileirar em bulk (`addBulk`) e pipeline no Redis; manter o `/process` mínimo; limiter global de 3 (a API externa continua sendo o gargalo: 20.000 × ~0,6 s ÷ 3 ≈ 70 min); callback em partes ou com payload paginado se o contrato permitir; Redis com persistência (AOF) e dimensionamento de memória; métricas de fila (lag, taxa, falhas); múltiplos workers só com limiter distribuído.

---

## ADR-002 — Idempotência por `run_id:seq`, independente do `jobId`

**Contexto.** A entrega é at-least-once: a mesma mensagem pode chegar mais de uma vez, em qualquer ordem, inclusive depois de o job original ter terminado e sido removido.

**Decisão.**

- A chave lógica é `run_id:seq`, guardada no estado do item (hash no Redis, campo `status`).
- Ao receber: criar o item com `HSETNX`/script atômico; se já existir, olhar o `status`:
  - `completed`/`failed`/`processing`: ignorar e responder `200`;
  - `received`/`queued`: garantir que existe um job executável; se o job estiver `failed`,
    recolocá-lo na fila com os contadores do BullMQ reiniciados.
- O `jobId` é `<run_id>_<seq>` (BullMQ rejeita `:`), servindo como segunda barreira contra duplicata enquanto o job existir.
- Transição `received -> queued` só depois de o `add` retornar com sucesso.
- O worker aceita `received`, `queued` ou `processing` ao iniciar. A existência do job prova que o
  enqueue foi confirmado; assim, o worker pode avançar antes de `markQueued`, que se torna um no-op
  seguro caso o item já esteja em `processing` ou terminal.

**Alternativas consideradas.**

- `SET NX` simples + `queue.add`: se o processo cair entre os dois passos, a duplicata seguinte é ignorada e o item se perde.
- Depender só do `jobId`: com `removeOnComplete`, uma duplicata tardia recria o job.

**Trade-offs.** Um pouco mais de lógica no `/process` (leitura do item e do job em caso de duplicata)
em troca de recuperar jobs falhos sem criar processamento duplicado.

**Consequências.** Reprocessar a mesma chave não duplica trabalho, não incrementa conclusão duas
vezes e não sobrescreve resultado. Um crash entre `queue.add` e `markQueued` não impede o job já
confirmado de processar o item, e uma duplicata recupera um job `failed` cujo item ainda não seja
terminal.

---

## ADR-003 — Limite global de 3 chamadas ao `/enrich`

**Contexto.** A API aceita no máximo 3 requisições simultâneas e responde `429` acima disso. O limite vale para o cliente como um todo, não por run.

**Decisão.** Um único processo worker com `concurrency: 3` na fila de enrich. A fila também configura `globalConcurrency: 3`, recurso disponível no BullMQ 6 instalado. A instância singleton de `WeduuClient` impõe `maxInFlight: 3` com um semáforo FIFO em memória, como defesa em profundidade contra chamadas que contornem acidentalmente a concorrência do worker.

**Alternativas consideradas.**

- `concurrency: 3` por processo em N processos: viola o limite global.
- `p-limit` dentro de um job único que processa o lote inteiro: perde a granularidade de retry por item.

**Trade-offs.** Throughput limitado a ~3 itens por ~0,6 s (≈ 5 itens/s). É exatamente o teto imposto pela API, então não há ganho em paralelizar mais.

**Consequências.** O teste de concorrência deve medir chamadas **em voo** (contador de pico no simulador), não só o total. O semáforo do cliente protege uma única instância/processo; preservar o cliente como singleton é obrigatório e múltiplos processos continuam exigindo um limitador distribuído.

---

## ADR-004 — Política de retry

**Contexto.** `429` traz `retry-after`; `500` é transitório (~10%); `401` e `404` não se resolvem repetindo.

**Decisão.**

| Resposta                     | Classe         | Ação                                                                                                                                               |
| ---------------------------- | -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `429`                        | `rate_limited` | `worker.rateLimit(retryAfterMs)` seguido de `Worker.RateLimitError()`; não consome tentativas de `500`; teto próprio de esperas persistido no hash |
| `500`, timeout, erro de rede | `transient`    | Até 5 tentativas, backoff exponencial (base 500 ms) + jitter                                                                                       |
| `401`, `404`                 | `permanent`    | Sem retry; item `failed` com causa e tentativas                                                                                                    |
| `200` com corpo inválido     | `permanent`    | Validar o corpo com Zod; nunca marcar `completed` sem dados válidos                                                                                |

**Alternativas consideradas.** `sleep(retry-after)` dentro do job: ocupa um dos 3 slots enquanto espera e reduz a vazão.

**Trade-offs.** Dois contadores de tentativa (retry e rate limit) aumentam um pouco a complexidade, em troca de não sacrificar vazão nem esgotar tentativas por causa de `429`.

**Consequências.** Todo retry é logado com `run_id`, `seq`, `attempt`, `status_code` e `duration_ms`. No BullMQ 6, `RateLimitError` move o job de `active` para `wait` sem passar por `moveToFailed` e, portanto, sem incrementar `job.attemptsMade`; mesmo assim, os contadores do hash do item são a fonte de verdade.

---

## ADR-005 — Ciclo de vida do run e registro tardio de `total`

**Contexto.** A plataforma começa a chamar `/process` imediatamente após responder ao `/burst`; o `total` pode ser persistido depois que os primeiros itens já foram aceitos e até processados.

**Decisão.** Itens são aceitos independentemente de o run existir. O run é criado sob demanda (primeira mensagem ou `registerRun`) com `total = null`. A conclusão é reavaliada em dois eventos: item resolvido e `total` registrado.

**Alternativas consideradas.**

- Rejeitar mensagens de run desconhecido: causa perda e retry desnecessário.
- Fazer o `burst` bloquear até registrar o `total` antes de a plataforma enviar: impossível, a ordem é controlada por eles.

**Trade-offs.** A avaliação de conclusão roda em dois pontos do código; precisa de teste específico para o caso "tudo concluído antes do `total`".

**Consequências.** Nenhum item é perdido por corrida de registro.

---

## ADR-006 — Callback com claim atômico e retry

**Contexto.** O callback não pode ser duplicado por corrida (dois itens finais concluindo ao mesmo tempo) nem se perder por falha de rede.

**Decisão.** Estados `open -> sending -> completed`. O claim `open -> sending` é atômico em Lua e concede um lease; falha no POST devolve o run a `open`, com backoff e limite de tentativas. O sweeper usa o mesmo claim e retoma leases expirados, em vez de manter uma segunda fila de callback. Itens `failed` são omitidos do payload e registrados.

**Alternativas consideradas.**

- Booleano `callbackSent`: marca antes de enviar (perde se falhar) ou depois (permite duplo envio).
- Enviar callback com `null` nos itens falhos: contraria o contrato (`price`/`stock` numéricos).
- Fila dedicada de callback: adiciona um segundo mecanismo de recuperação; lease + sweeper já cobrem crash e retry.

**Trade-offs.** O relatório da plataforma pode apontar itens ausentes quando houver falha definitiva; é o comportamento honesto (sem inventar dados).

**Consequências.** Callbacks concorrentes não geram duplo envio; falha de callback é visível e retentável.

---

## ADR-007 — Run travado (stalled) sem callback parcial

**Contexto.** Se alguma mensagem nunca chegar, o run nunca completa e não há sinal visível.

**Decisão.** O sweeper detecta, sem escrever no caminho de `/process`, que um run ficou `RUN_STALL_TIMEOUT_MS` sem progresso e ainda tem lacunas. O run vira `stalled`, as lacunas são logadas e **não há callback parcial**. Mensagem tardia reabre a avaliação.

**Alternativas consideradas.**

- Callback parcial automático: contraria I5 e produz relatório com itens perdidos.
- Espera infinita sem sinal: viola "nenhuma falha silenciosa".

**Trade-offs.** Em caso raro de perda de mensagem, o run exige uma nova execução (`burst`); como cada execução gera um novo relatório, o custo é baixo.

**Consequências.** Toda condição de não-conclusão é observável.

---

## ADR-008 — Cliente HTTP: `fetch` nativo com timeout

**Contexto.** Todas as chamadas à plataforma devem passar por um único cliente que encapsule base URL, headers, timeout e classificação de erro.

**Decisão.** `fetch` nativo do Node com `AbortSignal.timeout` (padrão 5000 ms para `/enrich`). Erros são convertidos em `transient | rate_limited | permanent`.

**Alternativas consideradas.** Axios: interceptors e retry embutidos, mas é uma dependência extra sem ganho aqui; o retry fica no worker, não no cliente.

**Trade-offs.** Menos conveniências prontas; em troca, zero dependência e comportamento explícito.

**Consequências.** Abortar no cliente não cancela a requisição no servidor, que pode continuar contando em voo; por isso o timeout é folgado em relação aos 400–800 ms normais.

---

## ADR-009 — Simulador da plataforma para testes

**Contexto.** Testar só contra a plataforma real exige ngrok e não permite reproduzir duplicata, desordem e falhas de forma determinística.

**Decisão.** `scripts/mock-platform.ts`: implementa `/register`, `/burst`, `/enrich` (latência 400–800 ms, `429` acima de 3 em voo, ~10% de `500`, `404` para SKU inválido) e `/callback` (validação do payload). Envia as mensagens para `/process` fora de ordem e com duplicatas, mede o tempo de ACK, registra o pico de concorrência e imprime um relatório semelhante ao da plataforma.

**Alternativas consideradas.** Mocks pontuais por teste (`nock`/`msw`): úteis em unitários, mas não reproduzem o comportamento de rajada e concorrência de ponta a ponta.

**Trade-offs.** Custo inicial de escrever o simulador, pago por testes de integração confiáveis e iteração rápida.

**Consequências.** Os critérios de aceite de concorrência, duplicata e callback viram testes automatizados.

---

## ADR-010 — Worker em processo separado da API

**Contexto.** O endpoint `/process` precisa manter o ACK abaixo de 600 ms, enquanto o enrich pode levar segundos, sofrer retry e bloquear durante shutdown.

**Decisão.** A API HTTP e o worker BullMQ usam entrypoints e processos separados. Cada processo mantém suas próprias conexões Redis; o worker usa conexões sem os timeouts curtos do caminho de ACK. O worker cria uma única instância de `WeduuClient`, usa `concurrency: 3` e configura a concorrência global da fila em 3.

**Alternativas consideradas.** Executar o worker no mesmo processo Fastify simplificaria o boot local, mas faria carga, falhas e shutdown do enrich competirem com o caminho de ACK.

**Trade-offs.** Há um processo e conexões Redis adicionais para operar, em troca de isolamento de latência e falhas.

**Consequências.** API e worker devem ser iniciados separadamente (`npm run dev` e `npm run dev:worker`, ou `npm run worker`). O shutdown do worker fecha o consumidor, a fila de controle e todas as conexões Redis de forma graciosa.

---

## ADR-011 — Verificação de completude pela faixa de sequências

**Contexto.** O callback exige que cada `seq` de `0..total-1` esteja resolvido; um contador isolado não prova ausência de lacunas.

**Decisão.** Para os 20 itens do desafio, o script Lua do claim percorre `0..total-1` e consulta o conjunto `run:{runId}:resolved` antes de conceder o lease do callback.

**Alternativas consideradas.** Um contador de itens resolvidos é O(1), mas pode ocultar sequências fora da faixa ou duplicadas sem validações adicionais.

**Trade-offs.** A varredura é O(total) no Redis e bloqueia o servidor durante o script, aceitável para lotes de 20 itens.

**Consequências.** Para 20.000 itens, substituir por contador atômico combinado com validação de faixa no registro e considerar callback em partes, sem perder a checagem explícita de lacunas.
