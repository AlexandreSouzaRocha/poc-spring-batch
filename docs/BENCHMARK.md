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
