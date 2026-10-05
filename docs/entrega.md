# Entrega

## Decisões arquiteturais

Separei a API HTTP do processo worker. O `POST /process` faz apenas validação, deduplicação,
persistência do estado inicial e enqueue no BullMQ antes de responder `200`; enriquecimento e
callback nunca são executados no caminho do ACK. Redis é a fonte de verdade para o estado
transitório dos runs e itens, enquanto o BullMQ fornece fila durável, tentativas e atrasos.

A idempotência usa `run_id:seq`. O `jobId` correspondente (`<run_id>_<seq>`) funciona como uma
segunda barreira, mas não substitui o estado do item. Duplicatas não recriam trabalho concluído e
recuperam jobs ausentes ou falhos para itens `received`, `queued` ou `processing`. O
worker aceita itens em `received`, `queued` ou `processing`, fechando a janela de crash entre
adicionar o job e marcar o item como enfileirado.

O worker roda em um único processo com concorrência 3. Além da concorrência configurada no worker,
a fila possui limite global 3 e o cliente HTTP usa um semáforo com o mesmo limite. Respostas `429`
respeitam `retry-after`; erros `500`, timeout e rede usam até cinco tentativas com backoff
exponencial e jitter; `401`, `404` e respostas inválidas são falhas permanentes e ficam registradas.

A conclusão de um run exige todos os `seq` de `0` até `total - 1`, sem depender apenas de um
contador. Um script Lua faz a verificação e o claim atômico do callback, evitando envio duplicado em
corridas. O callback usa lease e retry limitado, e um sweeper recupera leases expirados e torna runs
com lacunas visíveis como `stalled`. Os detalhes e alternativas estão documentados em
[decisions.md](decisions.md).

## Trade-offs aceitos

- O ACK depende de Redis. Se ele estiver indisponível, `/process` responde rapidamente com `503` em
  vez de confirmar uma mensagem que não foi persistida.
- Redis e BullMQ adicionam infraestrutura para um lote de apenas 20 itens, mas permitem sobreviver a
  reinícios e tornam retries e estados observáveis.
- Há somente um processo worker. Isso simplifica a garantia global de três chamadas ao `/enrich`;
  escalar horizontalmente exigiria um limitador distribuído.
- A verificação de completude percorre toda a faixa `0..total-1` em Lua. O custo é desprezível para
  20 itens e garante que uma contagem correta não esconda lacunas.
- O contrato de callback não define representação para itens definitivamente falhos. Esses itens são
  omitidos do resultado, sem inventar preço ou estoque, e permanecem consultáveis no Redis e nos
  logs.
- Os estados são transitórios em Redis e as respostas do callback são salvas em arquivo. Essa
  persistência é suficiente para o desafio, mas não substitui armazenamento histórico durável de
  longo prazo.

## O que mudaria com 20.000 SKUs

O limite externo de três requisições simultâneas continuaria sendo o principal gargalo: considerando
aproximadamente 600 ms por enriquecimento, 20.000 itens levariam cerca de 70 minutos mesmo com o
cliente operando no limite. Eu manteria o ACK mínimo e o processamento por item, mas adicionaria
métricas de lag, throughput, tentativas, falhas definitivas e idade dos runs.

No Redis, usaria persistência AOF, política explícita de TTL após o encerramento do run,
dimensionamento de memória e pipelines para operações em lote. A verificação de completude deixaria
de percorrer 20.000 posições dentro de um script Lua: usaria validação de faixa no recebimento mais
um contador ou bitmap atualizado atomicamente, preservando a detecção de lacunas.

Se fosse necessário aumentar a quantidade de workers, substituiria as proteções locais por um
limitador distribuído que mantivesse o teto global de três chamadas ao `/enrich`. Por fim, validaria
com a plataforma a possibilidade de callback paginado ou em partes; sem mudança no contrato,
manteria um único callback, mas construiria o payload de forma eficiente e avaliaria seu tamanho
antes do envio.

## Melhor execução

O melhor relatório obtido na plataforma oficial está em
[reports/best-execution.json](../reports/best-execution.json). A execução recebeu **score 100**, com
20 resultados corretos, nenhuma divergência ou perda, recuperação das cinco falhas `500` forçadas,
idempotência aprovada, concorrência dentro do limite e p95 de ACK de 449 ms.
