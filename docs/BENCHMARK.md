# Benchmark de Performance

> ⚠️ **Limitações da máquina de teste — leia antes de interpretar os números abaixo.**
> Todos os testes foram rodados **localmente**, em **Docker Desktop (macOS) com 12
> vCPUs / 36GiB RAM** alocados à VM, competindo no mesmo host físico por CPU entre os
> 10 containers da aplicação, Kafka, MongoDB e Azurite. Isso é **fundamentalmente
> diferente** do ambiente-alvo de produção (Kubernetes via ArgoCD), onde Kafka, MongoDB
> e Azure Blob Storage real rodam em infraestrutura **separada** dos pods de
> processamento — sem disputar CPU com eles. Três artefatos específicos deste
> laboratório local, que **não devem se repetir** (ou devem ser bem menores) em produção:
> - **Azurite** (emulador local do Azure Blob Storage) é um processo Node.js único,
>   com metadados persistidos em arquivo JSON — não escala para múltiplos GB com
>   escritas/leituras concorrentes como o Azure Blob Storage real. Foi o gargalo
>   dominante na fase de **geração** dos arquivos de teste (não afeta o tempo de
>   *processamento*, que é a métrica que importa para a SLA de produção).
> - O broker **Kafka (KRaft, single-node)** ocasionalmente apresenta instabilidade de
>   coordenador logo após subir (`This is not the correct coordinator`) — observado
>   tanto com 10 quanto com 3 containers consumindo; não impediu nenhum teste de
>   completar, mas adiciona alguns segundos de latência de rebalance. Em produção, um
>   cluster Kafka multi-broker gerenciado não deve apresentar esse comportamento.
> - **MongoDB** usa um único documento contador compartilhado (`batch_sequences`) para
>   gerar IDs sequenciais — sob alta concorrência de cold-start (10 containers criando
>   `StepExecution`s simultaneamente), gera conflitos de escrita transitórios
>   (`NoSuchTransaction`), mitigados com retry exponencial (`CustomSequenceIncrementer`).
>
> Os números abaixo são **direcionais** (mostram tendência de escala e viabilidade da
> arquitetura), não uma medição absoluta da capacidade em produção.

## Cenário 1 — 10 containers (topologia real: 1 arquivo por dígito verificador)

Cada container com **1 CPU / 2GiB memória** (limit), `partitions-per-file=8`,
`chunk-size=5000`, Java 25 (virtual threads, sem pinning — JEP 491), Kafka/MongoDB com
CPU dedicada separada dos containers de app. Volume dividido igualmente entre os 10
arquivos (ex.: 30MM = 3MM linhas/arquivo).

| Volume total | chunk-size | Tempo de processamento | Throughput agregado | Resultado |
|---:|---:|---:|---:|:--|
| 1MM | 5000 | 21.1s | ~47.400 registros/s | 10/10, 0 erros |
| 5MM | 5000 | 37.2s | ~134.500 registros/s | 10/10, 0 erros |
| 10MM | 5000 | 61.0s | ~164.000 registros/s | 10/10, 0 erros |
| 20MM | 5000 | 88.7s | ~225.600 registros/s | 10/10, 0 erros |
| 30MM | 5000 | 103.4s | ~290.200 registros/s | 10/10, 0 erros |
| 100MM | 10000 | 331.7s (~5.5min) | ~301.500 registros/s | 10/10, 0 erros |

A linha de 100MM usa `chunk-size=10000` (mudou de 5000 durante a investigação do
timeout de transação do Mongo em volumes maiores — ver seção de 200MM abaixo) e
`RoundRobinAssignor` já aplicado; não é uma comparação direta 1:1 com as linhas
anteriores. Importante: **100MM com a topologia real de 10 containers completou sem
esbarrar no teto do Kafka single-broker** que derrubou o teste de 200MM/10-containers
(ver nota de limitações abaixo) — indicando que o teto fica entre 100MM e 200MM nesta
configuração local.

Throughput cresce com o volume porque custos fixos (restart dos containers,
rebalance do consumer group, warm-up) são amortizados sobre mais dados — o comportamento
esperado de um pipeline com overhead de inicialização fixo.

Reproduzir: `./scripts/benchmark.sh <totalRecords> [recordLength] [timeoutSeconds]`.

## Cenário 2 — Teste de capacidade por container (mais fiel a produção)

Rodar os 10 containers simultaneamente neste laboratório de 12 núcleos mistura dois
efeitos: a capacidade real de processamento de cada container, e a contenção de CPU
local entre app/Kafka/Mongo/Azurite — que **não existirá em produção** (infra separada
dos pods no Kubernetes). Para isolar a métrica que importa (capacidade de
processamento por container/pod), este cenário roda só **3 containers**, cada um com
**3 CPUs dedicadas**, processando **1 arquivo no tamanho real de produção** cada — o
restante da máquina (9 núcleos) fica livre para Kafka/MongoDB/Azurite, eliminando a
disputa de recursos que distorce o Cenário 1.

Volumes usados: 20MM linhas/arquivo (carga inicial de produção, 200MM ÷ 10 arquivos) e
6MM linhas/arquivo (carga de homologação, 60MM ÷ 10 arquivos).

> ⚠️ **Bug de metodologia encontrado e corrigido:** o tópico `saldo-file-processor` tem
> 10 partições fixas (1 por dígito), mas este cenário só ativa 3 containers/dígitos
> (0, 1, 2). O assignor padrão do Kafka (`RangeAssignor`) distribui partições em
> **faixas contíguas** — com 3 consumers para 10 partições, a faixa `[0-3]` (onde caem
> os 3 dígitos com tráfego real) podia inteira parar num **único container**, deixando
> os outros 2 ociosos e o "throughput/container" reportado (agregado ÷ 3) sendo uma
> divisão fictícia, não uma medição real de 3 containers trabalhando em paralelo.
> Corrigido trocando para `partition.assignment.strategy: RoundRobinAssignor`
> (distribui ciclicamente partição a partição, garantindo 1 dígito real por container
> ativo — confirmado via log `partitions assigned` antes de aceitar o resultado). Os
> números do teste de **produção** abaixo já refletem a correção (também rodado com
> `chunk-size=10000`, não 5000); o de **homologação** ainda é o valor antigo
> (`chunk-size=5000`) e não foi revalidado — trate com cautela até re-executar.

| Cenário | Linhas/arquivo | Total (3 containers) | chunk-size | Tempo de processamento | Throughput/container | Resultado |
|---|---:|---:|---:|---:|---:|:--|
| Homologação (não revalidado) | 6MM | 18MM | 5000 | 61.1s | ~98.360 registros/s | 3/3, 0 erros |
| Produção (revalidado, distribuição confirmada) | 20MM | 60MM | 10000 | 135.9s | ~148.148 registros/s | 3/3, 0 erros |

**Extrapolação para os 10 containers de produção** (assumindo throughput/container
estável e infra dedicada, sem a contenção local que documentamos acima):
- Produção (200MM, 20MM/arquivo): ~148.148 × 10 ≈ 1.481.480 registros/s agregado → 200MM em ~135s (**dentro da meta de 5 minutos**).

Essa extrapolação é linear e otimista (assume que Kafka/Mongo/Blob real não viram
gargalo em produção, o que é razoável dado que são serviços gerenciados/dedicados,
mas **não foi validado empiricamente** — só um laboratório com hosts físicos
separados, ou o próprio cluster Kubernetes alvo, pode confirmar).

**Validação com volume real de 200MM (3 containers, ~66.67MM linhas/arquivo cada,
chunk-size=10000, distribuição confirmada via RoundRobinAssignor):**

| Volume total | Linhas/arquivo | Tempo de processamento | Throughput/container | Resultado |
|---:|---:|---:|---:|:--|
| 200MM | 66.67MM | 563.5s (~9.4min) | ~118.413 registros/s | 3/3, 0 erros |

Esse número é **menor** que os 148.148 registros/s/container do teste de produção
(20MM/arquivo) — esperado, já que arquivos ~3.3x maiores rodando por ~9.4min sustentados
têm mais tempo para expor overhead que só aparece em janelas longas (GC, contenção de
I/O acumulada). Ainda assim, **3/3 sem erros**, o que é o resultado mais importante: a
arquitetura sustenta 200MM de ponta a ponta neste laboratório, só que com 3 containers
generosos em CPU, não com a topologia real de 10 containers de 1 CPU (essa última
esbarrou no teto local de Kafka/disco — ver nota abaixo).

> ⚠️ **Por que não validamos 200MM com 10 containers reais:** tentamos diretamente
> (10 containers de 1 CPU cada, topologia de produção) e o teste falhou consistentemente
> — não por causa da aplicação, mas por dois limites físicos deste laboratório:
> 1. **Kafka single-broker (2 CPUs) não sustenta o volume de produção por tempo
>    suficiente**: após ~17min de processamento real (offsets avançando normalmente), o
>    canal interno do broker consigo mesmo caiu (`Node 1 disconnected`) e ~8min depois
>    **todos os 10 consumers foram expulsos do grupo por expiração de heartbeat**
>    (colapso total do consumer group). Um cluster Kafka multi-broker gerenciado em
>    produção não deve reproduzir isso.
> 2. **Esgotamento de disco**: 200MM registros geram ~52GB no Azurite (blob) e dezenas
>    de GB adicionais no tópico Kafka de saída (mensagens JSON) — em runs consecutivos
>    sem limpeza agressiva de volumes órfãos, o disco da VM do Docker Desktop (125GB
>    originalmente) enche e derruba Kafka/MongoDB com `No space left on device`. Aumentar
>    o disco alocado ao Docker Desktop (testamos com 256GB) e corrigir um volume anônimo
>    órfão do MongoDB (agora nomeado, `mongo-data`, no `docker-compose.yml`) mitigou isso.
>
> Dado isso, a validação de 200MM completa foi feita via o método de capacidade (3
> containers/3 CPUs, tabela acima) — que também evita o teto do Kafka local, já que tem
> 1/3 dos consumers disputando o broker.

Reproduzir: `./scripts/capacity-test.sh <linesPerFile> [recordLength] [timeoutSeconds] [coresPerContainer]`.
Após rodar, sempre conferir `docker compose logs app-1 app-2 app-3 | grep "partitions assigned"`
para confirmar que os dígitos ativos caíram em containers diferentes antes de aceitar o resultado.

## Cenário 3 — matriz partitions-per-file × chunk-size (8 vs 10, 5000 vs 10000)

Motivação: os Cenários 1 e 2 fixavam `partitions-per-file=8` e `chunk-size=5000` como
"o valor bom" com base em testes pontuais. Para confirmar que essa era mesmo a melhor
combinação (e não só a primeira que funcionou), rodamos as **4 combinações**
(`partitions-per-file` ∈ {8, 10} × `chunk-size` ∈ {5000, 10000}) contra os **mesmos
volumes** dos Cenários 1 e 2 — 32 execuções no total. Reproduzir com
`./scripts/run-matrix-resume.sh` (reexecuta só o que falta, pulando combinações já
concluídas — útil porque essa bateria é longa e sobreviveu a duas quedas de disco/Docker
Desktop no meio do caminho, narradas abaixo).

> ⚠️ São execuções **únicas** por combinação (sem repetição/média), então há ruído
> normal de medição — principalmente nos volumes pequenos (1MM), onde custos fixos
> (restart, rebalance) dominam o tempo total. Trate diferenças pequenas (<15%) como
> dentro da margem de ruído; as diferenças grandes reportadas abaixo são consistentes
> o suficiente para embasar uma recomendação.

### 10 containers (1 CPU/container — topologia real)

Throughput de processamento (registros/s) por volume:

| Volume | p8/c5000 | p8/c10000 | p10/c5000 | p10/c10000 |
|---:|---:|---:|---:|---:|
| 1MM | 87.443 | 87.191 | 44.033 | 44.377 |
| 5MM | 213.438 | 167.123 | 125.650 | 153.515 |
| 10MM | 242.694 | 303.637 | 216.379 | 248.268 |
| 20MM | 302.425 | 377.943 | 290.723 | 366.105 |
| 100MM | 262.034 | 252.681 | 221.837 | 257.761 |
| **Média** | **221.607** | **237.715** | **179.724** | **214.005** |

### 3 containers (3 CPU/container — teste de capacidade)

Throughput por container (registros/s) por volume total:

| Volume total | p8/c5000 | p8/c10000 | p10/c5000 | p10/c10000 |
|---:|---:|---:|---:|---:|
| 18MM | 113.533 | 176.912 | 143.757 | 194.912 |
| 60MM | 148.877 | 177.014 | 140.940 | 186.614 |
| 200MM | 98.205 | 104.461 | 101.742 | 124.073 |
| **Média** | **120.205** | **152.796** | **128.813** | **168.533** |

### Conclusão: a resposta depende de quanta CPU o container tem

**Com 1 CPU/container (topologia real de produção)**: `partitions-per-file=8` vence
`=10` de forma consistente e, no volume pequeno (1MM), por uma margem enorme (quase
2x) — mais partições que CPU disponível só adiciona troca de contexto sem ganho real
(já diagnosticado antes nesta sessão). `chunk-size` é um empate técnico entre 5000 e
10000 (a média favorece levemente 10000, mas a vantagem troca de lado dependendo do
volume — dentro do ruído de medição). **A intuição original do usuário
(`partitions-per-file=8`) se confirma como a melhor escolha para 1 CPU/container**;
`chunk-size=5000` continua uma escolha segura, mas `10000` não é pior de forma
relevante.

**Com 3 CPUs/container (teste de capacidade)**: o resultado inverte —
`partitions-per-file=10` vence `=8` em toda a tabela, e `chunk-size=10000` vence
`5000` em toda a tabela, sem exceção. Faz sentido: mais CPU disponível permite
aproveitar mais partições em paralelo sem sobrecarregar o container, e chunks maiores
reduzem a frequência de escrita de metadados no MongoDB (menos overhead
transacional por registro processado). **`partitions-per-file=10, chunk-size=10000`
é a melhor combinação testada quando o container tem 3 CPUs.**

**Implicação prática**: o valor ideal de `partitions-per-file`/`chunk-size` não é uma
constante da aplicação — é uma função da CPU disponível por container/pod. Se o
dimensionamento real de produção no Kubernetes alocar mais de 1 CPU por pod, vale
reconsiderar esses dois parâmetros para cima (10/10000) em vez de manter o default
atual (8/5000) tunado para o cenário mais restrito (1 CPU) deste laboratório.

### Duas quedas de infraestrutura durante essa bateria (não relacionadas à aplicação)

1. **Esgotamento de disco recorrente**: com 32 execuções sequenciais gerando/limpando
   dezenas de GB cada, o disco da VM do Docker Desktop encheu (>95%) por duas vezes,
   mesmo após o aumento para 256GB. Na segunda vez, isso causou `input/output error`
   real no `containerd` (corrupção do disco virtual, não só "sem espaço") — precisou
   de um quit+reopen completo do Docker Desktop para remontar o disco, seguido de
   `docker container prune -f` + `docker volume prune -f` (liberou ~52GB de
   containers parados e ~150GB de volumes órfãos numa única limpeza). A partir daí,
   `scripts/run-matrix-resume.sh` ganhou uma checagem automática de disco
   (`check_disk`, roda antes de cada teste, limpa sozinho acima de 70% de uso) para
   não depender de intervenção manual.
2. Nenhuma das quedas indicou um problema na aplicação ou no `saldoFileJob` — os
   `RESULT` de cada combinação, quando completam, mostram sempre `completed=10`/`3`
   e 0 erros críticos.

Reproduzir a matriz completa (ou retomar de onde parou): `./scripts/run-matrix-resume.sh`.
Resultados individuais ficam em `/tmp/matrix/*.log`; o script pula automaticamente
qualquer combinação cujo log já tenha uma linha `=== RESULT`.

## Cenário 4 — Coletor de GC: ParallelGC vs G1GC (1 CPU/container)

Motivação: testes em uma máquina com mais recursos (PC da empresa) usando
`-XX:+UseParallelGC` mostraram pausas de até ~500ms em alto volume. Investigamos o log
de GC (`-Xlog:gc*:file=/tmp/gc.log`) de um container local (1 CPU/2GiB, `Parallel
Workers: 1` — com só 1 CPU disponível, o ParallelGC não tem workers extras pra
paralelizar a coleta, perdendo sua principal vantagem sobre o SerialGC) processando
100MM registros (`partitions-per-file=8`, `chunk-size=5000`, buffer blob 4MB) e comparamos
com o mesmo teste usando `-XX:+UseG1GC`.

### Pausas (stop-the-world)

| Métrica | ParallelGC | G1GC |
|---|---:|---:|
| Pausas Young — média | 293.8ms | 67.4ms |
| Pausas Young — máxima | 1196.9ms | 2641.6ms |
| Full GC — ocorrências | **3** | **0** |
| Full GC — média / máxima | 3301.8ms / 4489.5ms | — |
| Tempo total parado (STW) | 73.9s | **35.3s** |
| Throughput de processamento | ~261.000-262.000 registros/s | ~264.258 registros/s |

O G1GC **eliminou completamente o Full GC** (3 ocorrências de até 4.5s no ParallelGC,
zero no G1) e **cortou pela metade o tempo total de pausa** (73.9s → 35.3s), mantendo o
mesmo throughput (diferença de ~1%, dentro do ruído de medição). O único número em que
o ParallelGC "vence" (pausa Young máxima menor) é enganoso: a pior pausa real do
ParallelGC não foi uma Young, foi o Full GC de 4.5s — a pior pausa **geral** do G1
(2.6s) já é bem menor que a pior pausa geral do ParallelGC (4.5s).

**Por que isso importa além do número absoluto**: ao longo desta sessão de benchmark,
o Kafka (broker single-node local) apresentou repetidas instabilidades de coordenador e
expulsão de consumers por expiração de heartbeat sob carga. Uma pausa de STW de 4.5s é
exatamente o tipo de evento que pode disparar esse problema em produção (o container
fica tempo demais sem responder ao heartbeat do consumer group). Reduzir a pior pausa
de 4.5s para 2.6s reduz esse risco, mesmo sem ganho de throughput.

### Uso de memória (ocupação do heap)

| Métrica | ParallelGC | G1GC |
|---|---:|---:|
| Ocupação pós-GC — média | 513M | **180M** (~2.8x menor) |
| Capacidade do heap — média | 1163M | **495M** (~2.3x menor) |
| Capacidade do heap — pico máximo | 1374M | 1434M (só em picos pontuais) |

O ParallelGC mantém o heap **cronicamente perto do limite** (média de 1163M de um teto
de ~1374M) porque a geração antiga acumula lixo entre as coletas Young até estourar num
Full GC. O G1GC recicla de forma muito mais agressiva e constante — a ocupação pós-GC
fica baixa e estável (média 180M), e o heap só cresce perto do teto (1434M) em picos
pontuais, não como estado permanente.

### Conclusão

**G1GC é a escolha certa para este workload em containers de 1 CPU**: mesmo throughput
que o ParallelGC, mas sem Full GC, metade do tempo total de pausa, pior caso de pausa
bem menor, e uso de heap significativamente mais eficiente — sem nenhuma contrapartida
observada. `JAVA_TOOL_OPTIONS` no `docker-compose.yml` já reflete essa escolha
(`-XX:+UseG1GC`).

Reproduzir: aplicar `-XX:+UseG1GC` (ou `-XX:+UseParallelGC` para comparar) em
`JAVA_TOOL_OPTIONS`, `docker compose up -d`, rodar `./scripts/benchmark.sh 100000000 260 1200`,
depois `docker cp poc-app-1:/tmp/gc.log ./gc.log` e analisar as pausas (`grep -oE
'Pause \w+.*?[0-9.]+ms' gc.log`).
