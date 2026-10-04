# SPEC.md — Desafio Técnico Weduu

## 1. Objetivo

Este repositório implementa um serviço de integração capaz de:

1. registrar um webhook na plataforma Weduu;
2. receber um lote de SKUs;
3. confirmar rapidamente o recebimento de cada mensagem;
4. processar os SKUs de forma assíncrona;
5. consultar preço e estoque no serviço de enriquecimento;
6. consolidar o resultado por execução (`run_id`);
7. enviar o lote finalizado para a plataforma via callback.

A prioridade da solução é demonstrar entendimento do contrato de integração, desacoplamento entre recebimento e processamento, controle de concorrência, idempotência, tolerância a falhas e clareza arquitetural.

---

## 2. Base URL da plataforma externa

```text
https://dev-wdu-ped-test-1014944555984.us-central1.run.app
```

Todos os endpoints utilizam JSON.

O serviço local precisa estar acessível por HTTPS público durante a execução do teste, por exemplo via ngrok.

---

## 3. Credenciais

Após o registro bem-sucedido, a plataforma retorna:

```json
{
  "cid": "clx...",
  "token": "a3f9..."
}
```

Essas credenciais autenticam as chamadas subsequentes.

Regras:

- `cid` identifica o cliente/registro;
- `token` é enviado nos headers exigidos por cada endpoint;
- credenciais nunca devem ser commitadas no repositório;
- usar variáveis de ambiente para valores sensíveis.

---

# 4. Endpoints da integração

Existem 6 endpoints no fluxo completo:

| Endpoint | Responsável | Objetivo |
|---|---|---|
| `POST /check` | Este serviço | Validar o webhook |
| `POST /process` | Este serviço | Receber mensagens do lote |
| `POST /register` | Weduu | Registrar o webhook |
| `POST /burst/:cid` | Weduu | Solicitar um novo lote |
| `GET /enrich/:sku` | Weduu | Consultar preço e estoque |
| `POST /callback` | Weduu | Entregar o resultado consolidado |

---

## 5. Endpoint implementado — `POST /check`

### Objetivo

Handshake de validação do webhook durante o registro.

### Request

```json
{
  "token": "a3f9..."
}
```

### Response — `200 OK`

```json
{
  "token": "a3f9..."
}
```

### Regras

- devolver exatamente o token recebido;
- não realizar processamento adicional;
- falhas de validação do payload devem retornar erro HTTP apropriado.

---

## 6. Endpoint implementado — `POST /process`

### Objetivo

Receber uma mensagem pertencente a um lote.

### Request

```json
{
  "run_id": "clx...",
  "seq": 7,
  "sku": "sku-001"
}
```

### Response

Qualquer status `2xx` é aceito pelo contrato externo.

Neste projeto, o padrão será:

```http
200 OK
```

```json
{
  "ok": true
}
```

### Regras do contrato

- SLA de ACK: **600 ms**;
- somente o status HTTP e o tempo da resposta são relevantes para o ACK;
- timeout da entrega: **3 segundos**;
- a ordem das mensagens **não é garantida**;
- a entrega é **at-least-once**;
- mensagens duplicadas podem ocorrer;
- identificador único de uma mensagem: **`run_id + seq`**;
- a primeira mensagem pode ser desconsiderada pela plataforma para a medição de warm-up.

### Comportamento obrigatório

O endpoint deve executar somente o caminho mínimo necessário:

```text
receber
  -> validar
  -> deduplicar
  -> enfileirar
  -> responder 200
```

O endpoint **não pode esperar o enriquecimento terminar**.

Ele também não deve chamar `/enrich` nem `/callback` diretamente.

---

## 7. Endpoint consumido — `POST /register`

### Objetivo

Registrar o webhook deste serviço.

### Request

```json
{
  "name": "Seu Nome",
  "webhook": "https://xxxx.ngrok.app"
}
```

Durante esta chamada, a plataforma executa:

```text
POST <webhook>/check
```

O registro só é concluído caso o token seja devolvido corretamente.

### Response de sucesso

```json
{
  "cid": "clx...",
  "token": "a3f9..."
}
```

### Exemplo de erro

```http
422 Unprocessable Entity
```

```json
{
  "error": "handshake_failed",
  "reason": "..."
}
```

---

## 8. Endpoint consumido — `POST /burst/:cid`

### Objetivo

Solicitar uma nova execução/lote.

### Headers

```http
x-token: <token>
```

### Response

```json
{
  "run_id": "clx...",
  "total": 20,
  "started_at": "2026-08-03T12:00:00Z"
}
```

### Regras

- cada execução gera um novo `run_id`;
- cada execução gera novos SKUs;
- depois da resposta, a plataforma começa a enviar mensagens para `POST /process`;
- o `total` precisa ser persistido no estado da execução para determinar quando o callback pode ser enviado.

---

## 9. Endpoint consumido — `GET /enrich/:sku`

### Objetivo

Consultar preço e estoque de um SKU.

### Headers

```http
x-cid: <cid>
x-token: <token>
```

### Response de sucesso — `200`

```json
{
  "sku": "sku-001",
  "price": 149.90,
  "stock": 42
}
```

### Comportamento esperado da API

| Status | Significado | Tratamento esperado |
|---|---|---|
| `200` | sucesso | persistir resultado |
| `429` | mais de 3 requisições simultâneas | respeitar `retry-after` |
| `500` | falha transitória (~10%) | retry com backoff + jitter |
| `401` | credencial inválida | falha não transitória; registrar |
| `404` | SKU inválido | falha não transitória; registrar |

A latência esperada de uma chamada bem-sucedida é de aproximadamente **400–800 ms**.

### Limite de concorrência

No máximo **3 requisições simultâneas** podem estar em voo para `/enrich`.

O limite é global para o processo inteiro, e não por `run_id`.

---

## 10. Endpoint consumido — `POST /callback`

### Objetivo

Entregar o resultado consolidado de uma execução.

### Headers

```http
x-token: <token>
content-type: application/json
```

### Request

```json
{
  "cid": "clx...",
  "run_id": "clx...",
  "result": [
    {
      "seq": 0,
      "sku": "sku-001",
      "price": 149.9,
      "stock": 42
    }
  ]
}
```

### Regras

- o resultado deve ser consolidado por `run_id`;
- os itens devem ser enviados ordenados por `seq`;
- o callback só pode ocorrer quando todos os `seq` esperados estiverem resolvidos;
- o mesmo `run_id` pode receber callback mais de uma vez pela API externa, mas esta implementação deve evitar callbacks duplicados acidentais;
- cada callback enviado gera um novo relatório de execução na plataforma.

---

# 11. Invariantes obrigatórios

Estas regras têm precedência sobre decisões de implementação.

## I1. ACK rápido e sem enriquecimento síncrono

`POST /process` nunca executa chamadas externas de negócio.

Ele deve:

1. validar o payload;
2. aplicar idempotência/deduplicação;
3. enfileirar o trabalho;
4. responder `200` rapidamente.

É proibido chamar `/enrich` ou `/callback` no request path de `/process`.

> A persistência/fila usada internamente pode envolver Redis ou outro armazenamento; o que não pode existir é dependência do serviço externo de enriquecimento para responder o ACK.

## I2. Idempotência por `run_id:seq`

A chave lógica de uma mensagem é:

```text
<run_id>:<seq>
```

Reprocessar a mesma chave não pode:

- criar trabalho duplicado efetivo;
- incrementar contadores de conclusão duas vezes;
- sobrescrever resultado final de forma inconsistente;
- gerar callback adicional.

## I3. Concorrência global máxima de 3

No máximo três chamadas ao `/enrich` podem estar simultaneamente em voo no processo inteiro.

Não é permitido aplicar `concurrency = 3` separadamente por execução caso isso permita superar 3 globalmente.

## I4. Retry controlado

### Para `429`

- ler o header `retry-after`;
- aguardar o período indicado antes de nova tentativa;
- não gerar busy loop.

### Para `500`

- usar retry limitado;
- usar backoff exponencial ou equivalente;
- adicionar jitter para evitar novas tentativas sincronizadas;
- após atingir o limite, registrar falha definitiva.

### Para `401` e `404`

- não fazer retry indiscriminado;
- registrar o motivo da falha;
- resolver o item como falha definitiva/visível conforme o modelo adotado.

## I5. Callback somente após resolução completa

Para uma execução cujo `total = N`, o callback só pode ser disparado quando todos os índices:

```text
0 .. N-1
```

estiverem resolvidos.

Antes do callback:

- confirmar que não há lacunas de `seq`;
- ordenar o resultado por `seq`;
- garantir que o callback ainda não foi enviado para essa conclusão.

## I6. Nenhuma falha silenciosa

Todo item deve terminar em um estado observável, por exemplo:

```text
received
queued
processing
completed
failed
```

Falhas definitivas precisam armazenar pelo menos:

- `run_id`;
- `seq`;
- `sku`;
- tipo/status da falha;
- mensagem ou causa;
- número de tentativas;
- data/hora da última tentativa.

Nenhum item pode simplesmente desaparecer da fila sem estado final conhecido.

---

# 12. Modelo de estado esperado

## Run

Campos conceituais mínimos:

```ts
{
  runId: string
  total: number
  callbackSent: boolean
  createdAt: Date
  completedAt?: Date
}
```

## Item

Campos conceituais mínimos:

```ts
{
  runId: string
  seq: number
  sku: string
  status: 'received' | 'queued' | 'processing' | 'completed' | 'failed'
  price?: number
  stock?: number
  attempts: number
  error?: string
}
```

A implementação concreta pode variar, mas os invariantes acima devem ser preservados.

---

# 13. Fluxo de referência

```text
1. iniciar API
2. expor API via HTTPS público
3. POST /register
4. plataforma chama POST /check
5. receber cid + token
6. POST /burst/:cid
7. persistir run_id + total
8. plataforma envia N chamadas POST /process
9. cada /process valida, deduplica, enfileira e responde 200
10. worker consome fila com concorrência global <= 3
11. worker chama GET /enrich/:sku
12. tratar sucesso, 429, 500 e falhas definitivas
13. persistir resultado por run_id + seq
14. detectar quando todos os seq foram resolvidos
15. ordenar por seq
16. POST /callback
17. registrar callback como concluído
18. guardar o relatório da melhor execução no repositório
```

---

# 14. Critérios de aceite internos

A implementação só deve ser considerada pronta quando:

- [ ] `/check` ecoa o token corretamente;
- [ ] `/process` responde `200` dentro do SLA em condições normais;
- [ ] `/process` não chama `/enrich` diretamente;
- [ ] duplicatas de `run_id:seq` não geram processamento duplicado efetivo;
- [ ] mensagens fora de ordem são aceitas corretamente;
- [ ] nunca existem mais de 3 chamadas simultâneas ao `/enrich`;
- [ ] `429` respeita `retry-after`;
- [ ] `500` possui retry com backoff, jitter e limite;
- [ ] falhas definitivas ficam persistidas/visíveis;
- [ ] nenhum `seq` ausente permite callback prematuro;
- [ ] callback é ordenado por `seq`;
- [ ] callback não é disparado duas vezes por condição de corrida;
- [ ] testes cobrem idempotência, concorrência, retry e consolidação;
- [ ] README explica como executar localmente;
- [ ] relatório da melhor execução foi salvo para a entrega.

---

# 15. Fora de escopo inicial

Não implementar antes do fluxo principal funcionar:

- frontend React sem necessidade clara;
- autenticação própria de usuários;
- Kubernetes;
- microserviços adicionais;
- abstrações genéricas sem uso real;
- observabilidade complexa antes dos requisitos principais;
- arquitetura distribuída para 20.000 itens antes de validar o lote de 20.

Primeiro objetivo: concluir corretamente o fluxo ponta a ponta com 20 SKUs.
