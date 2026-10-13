# Chappie Agent2Actor2Action A3
## Especificação técnica de Stateful Agents com mensageria durável, execução ordenada e replicação resiliente

**Projeto:** Chappie-Agent2Actor2Action-A3  
**Linguagem de implementação do runtime:** Zig  
**Arquitetura de comunicação:** in-memory broker local e transporte remoto via QUIC, com NATS como alternativa de mensageria distribuída  
**Objetivo:** criar agentes stateful com processamento controlado, persistência de eventos, recuperação após falhas e replicação de dados sem exigir que o banco de dados local esteja sempre disponível.

---

# Parte I — Explicação pedagógica

## 1. O problema que esta arquitetura resolve

Imagine um agente responsável por gerenciar a memória de outro agente. Esse agente precisa gravar informações em um banco local, como SQLite, Cozo ou RocksDB. Ao mesmo tempo, precisamos garantir que uma falha no banco local não faça com que eventos recebidos sejam perdidos.

Em uma arquitetura convencional, o agente recebe um evento, tenta gravá-lo no banco e devolve uma confirmação. Se o banco estiver indisponível, a operação pode falhar e a aplicação precisa decidir como preservar e repetir o evento.

O Chappie Agent2Actor2Action A3 propõe separar essas operações.

O evento é recebido e registrado em uma estrutura durável antes de ser entregue ao componente responsável por executá-lo. A partir desse momento, o sistema pode acompanhar o evento independentemente da disponibilidade imediata do banco.

O agente pode continuar recebendo eventos dentro dos limites de capacidade configurados, enquanto a execução local, a replicação remota e a recuperação seguem seus próprios estados.

A ideia fundamental é:

**Receber um evento, persistir sua intenção, executar a mutação e confirmar sua conclusão são etapas diferentes.**

Essa separação permite construir uma arquitetura mais previsível e resiliente.

## 2. Os componentes principais

### 2.1 Stateful Agent

Um Stateful Agent é um agente que mantém ou administra um estado persistente ao longo do tempo.

Exemplos incluem agentes que gerenciam memória de longo prazo, histórico de experiências, conhecimento consolidado ou projeções de dados.

Cada agente possui uma identidade, um ciclo de vida e uma fronteira de responsabilidade. Ele não precisa conhecer os detalhes internos de todos os componentes com os quais se comunica.

### 2.2 Sentinel

O Sentinel é o supervisor responsável pela entrada e pelo controle dos eventos do agente.

Ele recebe os eventos, valida o envelope, atribui um identificador sequencial, determina sua posição na fila e registra o evento na mailbox durável.

O Sentinel também acompanha as confirmações do Manager, controla a capacidade disponível e coordena o redirecionamento de eventos para réplicas.

O Sentinel não deve executar diretamente as mutações do banco. Sua função é organizar, proteger e coordenar o fluxo de eventos.

### 2.3 Mailbox/Outbox durável

A Mailbox é a fila persistente de eventos que ainda precisam ser processados ou cujo processamento ainda não foi confirmado.

A Outbox é o registro durável das mensagens que precisam ser encaminhadas a outros componentes, como o Clone remoto.

Esses conceitos podem compartilhar uma implementação física, mas seus estados e objetivos precisam continuar distintos.

Um evento pode, por exemplo, já ter sido aplicado ao banco local e ainda aguardar confirmação de replicação remota. Portanto, não é seguro remover o evento simplesmente porque uma das operações terminou.

### 2.4 ManagerAgent

O ManagerAgent é o componente que efetivamente executa as mutações no banco de dados.

Exemplos:

- `SQLiteManagerAgent`
- `CozoManagerAgent`
- `RocksManagerAgent`
- `RxManagerAgent`

O Manager lê os eventos persistidos, executa as operações correspondentes e publica uma confirmação contendo o identificador do evento e o resultado da operação.

O banco é acessado por seu adaptador específico. O protocolo de eventos não deve depender da implementação interna de um banco particular.

### 2.5 2ndBrainAgent

O `2ndBrainAgent` coordena o acesso à memória e ao armazenamento do Stateful Agent.

Ele se comunica com os Stateful Agents por meio do broker em memória e com os respectivos StorageManagerAgents por meio de canais internos.

O objetivo é centralizar a coordenação sem transformar o 2ndBrainAgent em um gargalo obrigatório para cada operação de baixo nível.

Cada armazenamento continua sendo administrado por seu próprio Manager, que conhece as operações suportadas e os requisitos de consistência do backend.

### 2.6 CloneAgent

O CloneAgent é uma instância remota ou alternativa capaz de receber eventos replicados e preservar os dados necessários à recuperação do agente original.

Ele mantém sua própria mailbox durável e um histórico mínimo dos eventos ainda não confirmados localmente pelo Original.

O Clone não deve ser tratado apenas como uma cópia temporária em memória. Ele precisa sobreviver a reinicializações e reconstruir seu estado a partir do armazenamento persistente.

---

## 3. Como os componentes se comunicam

O fluxo principal pode ser entendido como uma linha de produção.

1. O Stateful Agent produz um evento.
2. O Sentinel valida, identifica e registra o evento na mailbox durável.
3. O Sentinel disponibiliza o evento ao Manager por meio do broker em memória.
4. O Manager executa a mutação no banco local.
5. O Manager publica um ACK de processamento local.
6. O Sentinel registra a confirmação e libera o evento da fila de pendências locais, conforme as regras de retenção.
7. Paralelamente, o evento é replicado para o Clone.
8. O Clone persiste o evento e mantém seu estado de confirmação de replicação.
9. Se o Original perder seu banco local, o Clone continua preservando os eventos recebidos.
10. Quando o Original se recuperar, o protocolo de sincronização identifica as lacunas e entrega os eventos necessários para reconstruir o estado local.

A confirmação de persistência na mailbox precisa ocorrer antes de o sistema considerar o evento aceito de forma durável. A confirmação do Manager, por sua vez, precisa ocorrer somente depois de a mutação local ter sido efetivamente concluída.

Essas duas confirmações representam garantias diferentes.

---

# Parte II — Especificação técnica

## 4. Modelo arquitetural

A arquitetura é composta por quatro planos lógicos.

**Plano de ingestão:** recebe os eventos, atribui identidades e garante sua persistência inicial.

**Plano de execução:** entrega os eventos aos Managers, que aplicam as mutações nos bancos de dados.

**Plano de replicação:** transporta eventos e confirmações entre o Original e o Clone.

**Plano de recuperação:** reconcilia os eventos pendentes, detecta lacunas e reconstrói o estado do Original.

Cada plano possui responsabilidades específicas e não deve depender de suposições implícitas sobre o funcionamento dos outros.

### 4.1 Topologia lógica

    StatefulAgent
         |
         v
    Sentinel / Supervisor
         |
         v
    Durable Mailbox
         |
         +------> In-memory Broker ------> ManagerAgent
         |                                    |
         |                                    v
         |                               Local Database
         |
         +------> Replication Outbox
                        |
                        v
                   QUIC / NATS
                        |
                        v
                    CloneAgent
                        |
                        v
                 Clone Durable Log

O broker em memória é um mecanismo de comunicação e sinalização, não a fonte de verdade dos eventos aceitos. Se o processo for reiniciado, as mensagens necessárias precisam ser recuperadas da mailbox durável.

## 5. Identidade, ordenação e envelope dos eventos

Cada evento deve possuir um identificador estável, independente da tentativa de processamento.

Um envelope lógico mínimo deve conter:

- `event_id`: identificador único do evento.
- `agent_id`: identidade do agente responsável.
- `stream_id`: identidade do fluxo ordenado.
- `sequence`: posição do evento nesse fluxo.
- `event_type`: tipo da operação.
- `schema_version`: versão do contrato do evento.
- `payload`: dados necessários para executar a operação.
- `payload_hash`: hash utilizado para detectar inconsistências.
- `created_at`: horário de criação, quando disponível.
- `correlation_id`: identificador para rastrear operações relacionadas.

O identificador sequencial deve ser monotônico dentro de seu escopo. Para um agente com um único fluxo de escrita, isso permite identificar o último evento contíguo confirmado.

Para evitar ambiguidade, recomenda-se utilizar uma identidade composta, como `(agent_id, stream_id, sequence)`, e manter o `event_id` como identificador globalmente único.

A ordenação não deve depender exclusivamente de timestamps. Relógios podem divergir, e dois eventos podem compartilhar o mesmo horário.

Se o sistema aceitar múltiplos produtores simultâneos, o Sentinel deverá serializar a atribuição de sequência ou utilizar um mecanismo equivalente que garanta a unicidade e a ordem definida pelo contrato.

## 6. A Mailbox durável em Zig

### 6.1 Estratégia recomendada

A implementação inicial deve utilizar um **append-only write-ahead log (WAL)** com registros binários, checkpoints e recuperação após falhas.

O WAL é um arquivo ou conjunto de segmentos no qual novos registros são acrescentados sequencialmente. Em vez de reescrever a fila inteira, o sistema adiciona eventos e registra suas transições de estado.

Uma estrutura de registro pode conter:

- marcador de formato e versão;
- tamanho do registro;
- identificador do evento;
- sequência do fluxo;
- tipo de registro;
- tamanho do payload;
- checksum do registro;
- payload;
- informações de integridade necessárias para a recuperação.

O formato binário deve definir explicitamente a ordem dos bytes e o alinhamento. Não se deve presumir que a representação de uma `packed struct` Zig seja automaticamente um formato portátil para disco e rede.

O payload pode ser armazenado diretamente no registro ou em um formato de serialização versionado. A escolha deve ser validada por benchmarks e requisitos de compatibilidade.

### 6.2 Garantia de durabilidade

A gravação de dados em um arquivo não significa necessariamente que os dados já estejam protegidos contra uma queda de energia.

Para oferecer uma garantia de persistência explícita, a implementação deve definir os pontos de sincronização com o sistema operacional e o comportamento em caso de erro de escrita.

O protocolo de ingestão deve seguir esta ordem:

1. Validar o evento.
2. Atribuir o identificador e a sequência.
3. Acrescentar o registro ao WAL.
4. Executar a operação de sincronização durável necessária.
5. Somente então confirmar a aceitação durável ao produtor.

O custo de sincronização pode ser reduzido por *group commit*: vários eventos são gravados em conjunto e confirmados após uma única operação de sincronização.

Isso exige que a confirmação seja enviada apenas aos eventos que realmente fazem parte do lote persistido.

O uso de `mmap`, isoladamente, não garante durabilidade. Um arquivo mapeado em memória continua sujeito às regras de persistência do sistema operacional e do dispositivo de armazenamento.

### 6.3 Recuperação após reinicialização

Ao iniciar, o Sentinel deve:

1. Abrir os segmentos válidos do WAL.
2. Verificar cabeçalhos, tamanhos e checksums.
3. Reconhecer o último registro completo e válido.
4. Descartar ou isolar um sufixo incompleto, conforme o protocolo de recuperação.
5. Reconstruir o índice de eventos pendentes.
6. Restaurar os checkpoints e os estados de confirmação.
7. Recolocar os eventos ainda pendentes na fila de processamento.
8. Retomar a replicação dos eventos que ainda não foram confirmados pelo Clone.

A recuperação deve ser idempotente: executá-la novamente não pode criar novas mutações lógicas nem duplicar a identidade dos eventos.

### 6.4 Remoção lógica e compactação

Um ACK local não precisa apagar imediatamente um evento do arquivo.

A remoção lógica pode ser registrada por meio de um novo registro de confirmação. Posteriormente, o runtime pode compactar os segmentos que não contenham mais eventos necessários.

A compactação só pode descartar dados que não sejam necessários para nenhuma garantia ainda pendente.

Se o Original precisar recuperar eventos que já foram aplicados localmente, mas cuja replicação remota não esteja confirmada, esses eventos deverão continuar disponíveis na Outbox ou em outro log durável.

Por isso, a retenção deve considerar os diferentes estados de confirmação, e não apenas um único `last_acked_id`.

## 7. ACKs e estados de processamento

A arquitetura deve distinguir pelo menos três confirmações.

**ACK de persistência:** confirma que o evento foi registrado de forma durável na mailbox do destino.

**ACK de processamento local:** confirma que a mutação foi aplicada e confirmada pelo backend local.

**ACK de replicação:** confirma que o Clone persistiu o evento e pode recuperá-lo após uma reinicialização.

Essas confirmações não são intercambiáveis.

O evento poderá ter os seguintes estados:

`RECEIVED → DURABLE → PROCESSING → APPLIED_LOCAL`

A replicação possui seu próprio ciclo:

`NOT_REPLICATED → SENT → DURABLE_REMOTE`

Um evento poderá estar em `APPLIED_LOCAL` e ainda não estar em `DURABLE_REMOTE`, ou estar em `DURABLE_REMOTE` enquanto aguarda processamento local.

O Sentinel só poderá remover definitivamente o evento quando todas as obrigações de retenção relacionadas a ele tiverem sido satisfeitas.

Os ACKs devem ser idempotentes. Receber o mesmo ACK várias vezes não pode alterar incorretamente a posição do checkpoint nem eliminar eventos posteriores ainda pendentes.

## 8. O in-memory broker

O broker local deve ser otimizado para comunicação entre componentes do mesmo processo.

A implementação pode utilizar filas limitadas, referências a registros persistidos e primitivas de sincronização de Zig.

Para reduzir cópias de payload, o broker pode transportar descritores que contenham a identidade do evento e uma referência segura ao registro persistido.

A referência só pode ser liberada depois que todos os consumidores que dependem dela concluírem seu trabalho.

A fila em memória deve ter capacidade configurável e aplicar backpressure quando atingir seu limite.

O broker não pode ser utilizado como substituto da mailbox durável: uma reinicialização deve permitir reconstruir a fila a partir dos eventos persistidos.

### 8.1 Organização de memória

O runtime deve definir explicitamente:

- quem aloca cada mensagem;
- quem é responsável por liberar a memória;
- quando uma referência deixa de ser válida;
- como são sincronizados produtores e consumidores;
- como a fila se comporta quando está cheia;
- como os consumidores são acordados sem busy-waiting desnecessário.

Um ring buffer pode ser adequado para comunicação entre produtores e consumidores com um modelo de concorrência bem definido. Uma fila SPSC, por exemplo, não pode ser utilizada como se fosse uma fila MPMC sem implementar as garantias adicionais necessárias.

## 9. Escalonamento elástico e clonagem de agentes

O Sentinel deve observar a ocupação da fila e aplicar limites configuráveis.

Considere:

- capacidade máxima: \(C\);
- ocupação atual: \(Q\);
- watermark superior: \(H\);
- watermark inferior: \(L\).

A ativação do redirecionamento ocorre quando:

\[
Q \geq H = \lceil 0{,}9C \rceil
\]

A desativação ocorre quando:

\[
Q \leq L
\]

O valor inferior deve ser menor que o superior para evitar alternância frequente entre os estados de redirecionamento.

A política deve considerar tanto o número de eventos quanto o total de bytes pendentes, porque um evento pode conter um payload muito maior que outro.

### 9.1 Comportamento esperado

Quando o sistema atinge o watermark superior, o Sentinel ativa o redirecionamento e solicita a criação ou ativação de uma instância adicional.

O runtime mantém o agente oficial disponível para a entrada de novos eventos, desde que sua capacidade permaneça dentro dos limites definidos. As novas entradas são direcionadas conforme a política de admissão.

Quando a ocupação diminui suficientemente, o runtime pode desativar o redirecionamento.

### 9.2 Restrição importante

Clonar um agente não significa que o runtime possa duplicar livremente a responsabilidade de escrita sobre o mesmo estado.

Se dois agentes executarem mutações concorrentes sobre o mesmo fluxo, poderão produzir resultados inconsistentes, mesmo que ambos tenham recebido os mesmos eventos.

A solução recomendada é utilizar atribuição explícita de propriedade do fluxo, particionamento por chave ou uma política de liderança que impeça dois escritores ativos para o mesmo escopo.

A criação de uma nova instância também não deve ocorrer a cada ultrapassagem momentânea do watermark. O runtime deve reutilizar instâncias disponíveis e aplicar limites de escalonamento.

Se a capacidade de processamento continuar inferior à taxa de chegada dos eventos, criar clones apenas transferirá a fila de um lugar para outro. A arquitetura precisa prever limites globais, rejeição controlada ou backpressure para impedir crescimento ilimitado.

## 10. Replicação via QUIC ou NATS

A comunicação remota deve transmitir eventos persistidos e suas confirmações, com identificadores estáveis e regras explícitas de ordenação.

QUIC pode ser utilizado como transporte seguro e confiável de fluxos. NATS pode ser utilizado para mensageria e distribuição de eventos; quando forem necessárias persistência e recuperação de mensagens, deve-se definir e configurar o mecanismo de armazenamento correspondente, como JetStream.

Essas alternativas não são equivalentes em todos os aspectos. A escolha deve considerar o modelo de implantação, a topologia da rede, a ordenação exigida, a política de retenção e a complexidade operacional.

### 10.1 Protocolo mínimo de replicação

O Original envia um evento ao Clone.

O Clone valida o envelope e persiste o evento em sua própria mailbox durável.

Depois de garantir a persistência, o Clone responde com um ACK de replicação.

Se a resposta se perder, o Original poderá retransmitir o mesmo evento. O Clone deverá reconhecer sua identidade e evitar criar uma segunda ocorrência lógica.

O Original deve manter os eventos necessários à replicação até receber uma confirmação válida do Clone ou até aplicar uma política explícita de retenção alternativa.

O ACK remoto não significa que a mutação já foi aplicada ao banco do Clone. Ele confirma somente a garantia definida pelo protocolo — neste caso, persistência remota.

## 11. ActualHistory do Clone

O `ActualHistory` é a representação compacta do estado de sincronização do Clone.

Ele deve permitir identificar o último evento confirmado de forma contígua pelo Original, bem como os eventos persistidos no Clone que ainda precisam ser entregues ou confirmados.

A estrutura lógica contém:

- o último checkpoint contíguo confirmado;
- os eventos posteriores ainda pendentes;
- os metadados necessários para localizar esses eventos no log durável;
- o estado de sincronização do fluxo.

Não é suficiente guardar somente o último ID confirmado se as confirmações puderem chegar fora de ordem.

Por exemplo, se o Clone receber confirmação para os eventos 100 e 102, mas não para o 101, o checkpoint contíguo não pode avançar para 102.

O sistema pode manter um conjunto de confirmações fora de ordem, mas só deve avançar o checkpoint até o último evento para o qual todos os anteriores estejam confirmados.

O `ActualHistory` não precisa manter cópias completas de todos os payloads em RAM. O WAL é a fonte durável; o histórico pode utilizar índices e offsets para localizar os registros.

## 12. Sincronização após recuperação do Original

Quando o banco local do Original voltar a ficar disponível, o Sentinel deve iniciar um protocolo de sincronização.

O Original envia uma mensagem de sincronização contendo a identidade do fluxo, seu checkpoint local e as informações de versão necessárias.

O Clone identifica os eventos necessários para recuperar o estado e os envia em lotes ordenados.

O Original persiste os eventos recebidos em sua mailbox, encaminha-os ao Manager e aguarda a confirmação do processamento local.

O Clone mantém os eventos necessários à recuperação enquanto o Original ainda não tiver confirmado sua aplicação local.

Esse processo continua até que o Original alcance o ponto de sincronização definido pelo protocolo.

### 12.1 Buckets e backpressure

Os eventos podem ser agrupados em buckets para reduzir o overhead de transporte.

O tamanho de cada bucket deve ser configurável e validado por benchmarks. O limite deve considerar o tamanho do payload, os limites de memória, o framing do protocolo e o controle de congestionamento do transporte.

Um valor próximo de 1.200 bytes pode ser um ponto inicial para determinados cenários de transporte QUIC, mas não deve ser tratado como tamanho universal ou como requisito para cada mensagem.

O protocolo deve permitir:

- identificação do lote;
- sequência inicial e final;
- validação de integridade;
- reconhecimento dos eventos persistidos;
- repetição segura em caso de falha;
- limitação do número de lotes em trânsito.

O Original deve limitar a quantidade de dados de recuperação em voo. O Clone só envia novos lotes conforme a capacidade de ingestão e o progresso do Original.

Isso permite que a recuperação avance sem inundar a mailbox nem consumir toda a memória disponível.

### 12.2 Não misturar dados de recuperação e eventos novos sem política de ordenação

Durante a recuperação, novos eventos podem continuar chegando ao Original.

O runtime deve definir se esses eventos serão processados em um fluxo separado, serão colocados atrás dos eventos de recuperação ou serão ordenados por um mecanismo de sequência comum.

A implementação não pode presumir que eventos históricos e eventos novos podem ser misturados arbitrariamente.

Se as operações dependem umas das outras, a ordem lógica precisa ser preservada.

## 13. Mutação idempotente e recuperação de falhas

A arquitetura deve prever falhas em todas as fronteiras: antes da gravação, depois da gravação, durante a mutação, depois do commit e antes do ACK.

O caso mais importante ocorre quando o banco confirma uma mutação, mas o processo falha antes de registrar o ACK correspondente.

Na reinicialização, o evento será entregue novamente. Por isso, o Manager precisa reconhecer que a operação já foi aplicada ou garantir que a repetição seja segura.

Uma estratégia é utilizar uma tabela ou registro de deduplicação no mesmo contexto transacional da mutação, quando o backend permitir.

Outra estratégia é projetar operações idempotentes, de modo que executar novamente a mesma operação produza o mesmo estado lógico.

Não se deve prometer exatamente uma execução física de cada evento apenas com um WAL e ACKs. A garantia mais realista é entrega pelo menos uma vez com deduplicação e idempotência, produzindo um efeito lógico único dentro das condições garantidas pelo backend.

## 14. Interfaces lógicas em Zig

Os módulos devem ser separados por contrato, e não por dependências diretas entre implementações concretas.

Interfaces lógicas recomendadas:

- `EventEnvelope`: representa o evento serializado.
- `Mailbox`: registra e recupera eventos duráveis.
- `Sentinel`: valida, ordena, admite e distribui eventos.
- `InMemoryBroker`: transporta notificações e descritores locais.
- `StorageManager`: executa operações no backend.
- `ReplicationTransport`: envia eventos e recebe confirmações remotas.
- `ActualHistory`: acompanha os eventos pendentes de sincronização.
- `RecoveryCoordinator`: coordena a recuperação e a reconciliação.
- `CapacityController`: aplica watermarks e políticas de backpressure.

Em Zig, esses contratos podem ser implementados com tipos concretos, funções genéricas, estruturas de operações ou interfaces baseadas em ponteiros de função, conforme a necessidade de substituição dinâmica dos componentes.

O contrato deve especificar também os erros possíveis, a propriedade da memória, os limites dos payloads e a semântica de cada confirmação.

## 15. Requisitos de integridade e segurança

A implementação deve considerar:

- validação dos tamanhos antes de alocar memória;
- detecção de registros incompletos e corrompidos;
- validação de checksums;
- autenticação dos pares remotos;
- proteção contra repetição indevida de eventos;
- controle de versão dos envelopes;
- limites de memória e armazenamento;
- tratamento explícito de disco cheio;
- recuperação após falhas parciais;
- autorização para mutações;
- observabilidade sem exposição de payloads sensíveis.

O Sentinel não deve confirmar uma gravação durável se o disco estiver cheio ou se a operação de persistência falhar.

O sistema deve tornar a indisponibilidade explícita, aplicar backpressure e preservar os eventos já aceitos.

---

# Parte III — Invariantes que definem o contrato

A arquitetura deverá preservar os seguintes invariantes.

**I1 — Persistência antes da aceitação:** um evento só é aceito como durável depois que sua gravação satisfaz a política de persistência configurada.

**I2 — Identidade estável:** uma retransmissão mantém o mesmo identificador e a mesma identidade lógica.

**I3 — Ordem explícita:** a ordenação é definida por sequência e escopo, não apenas por timestamp ou ordem de chegada à rede.

**I4 — Confirmações independentes:** persistência, processamento local e replicação remota possuem estados distintos.

**I5 — Remoção segura:** um evento só pode ser eliminado quando não houver nenhuma obrigação de processamento, replicação ou recuperação que dependa dele.

**I6 — Recuperação idempotente:** reinicializar qualquer componente não deve criar mutações lógicas duplicadas.

**I7 — Escrita controlada:** somente o proprietário autorizado do fluxo pode executar mutações naquele escopo.

**I8 — Capacidade limitada:** filas em memória, lotes em trânsito e buffers de recuperação possuem limites configuráveis.

**I9 — Integridade do histórico:** o checkpoint do Clone representa o último evento contíguo confirmado, sem ignorar lacunas.

**I10 — Falhas explícitas:** erros de disco, rede ou banco não podem ser convertidos silenciosamente em confirmações de sucesso.

---

# Parte IV — Estratégia de implementação

## 16. Etapas recomendadas

**Etapa 1 — Contratos e envelopes.** Definir o formato dos eventos, identidade, sequência, ACKs e estados de processamento.

**Etapa 2 — WAL durável.** Implementar append, recuperação, checksums, checkpoints e testes de falha durante a escrita.

**Etapa 3 — Sentinel e broker.** Implementar a fila local, a entrega ao Manager, o controle de capacidade e a recuperação da fila após reinicialização.

**Etapa 4 — Manager e idempotência.** Criar um adaptador de armazenamento e garantir que o reprocessamento de um evento não produza mutações lógicas duplicadas.

**Etapa 5 — Outbox e replicação.** Implementar o envio de eventos, ACK remoto, repetição e deduplicação.

**Etapa 6 — ActualHistory e recuperação.** Implementar checkpoints contíguos, detecção de lacunas, buckets e controle de fluxo.

**Etapa 7 — Escalonamento elástico.** Adicionar watermarks, redirecionamento e gerenciamento do ciclo de vida dos clones.

**Etapa 8 — Benchmarks e testes de resiliência.** Medir throughput, latência, consumo de memória, tempo de recuperação e comportamento sob falhas.

## 17. Critérios de aceitação

A implementação não deverá ser considerada concluída apenas porque os eventos são escritos e lidos com sucesso em condições normais.

Ela deverá demonstrar, por testes automatizados, que:

1. Um evento aceito continua recuperável após a reinicialização do processo.
2. Um registro incompleto não é interpretado como um evento válido.
3. Uma falha do banco não elimina eventos pendentes.
4. A perda de um ACK provoca repetição segura, não perda de dados.
5. Uma retransmissão não duplica a mutação lógica.
6. O Clone mantém os eventos necessários à recuperação mesmo após reiniciar.
7. A sincronização detecta lacunas e não avança checkpoints indevidamente.
8. A capacidade configurada é respeitada sob sobrecarga.
9. O redirecionamento não cria escritores concorrentes não autorizados.
10. O disco cheio e as falhas de rede são tratados sem falsas confirmações de sucesso.

---

# Conclusão

O Chappie Agent2Actor2Action A3 propõe uma arquitetura de Stateful Agents na qual o processamento de eventos é separado de sua persistência, replicação e recuperação.

O Sentinel controla a entrada e a ordenação; a mailbox durável preserva o trabalho pendente; os Managers executam as mutações; o broker em memória coordena a comunicação local; e o Clone mantém os dados necessários à recuperação remota.

A principal propriedade dessa arquitetura não é simplesmente a velocidade de uma fila ou a capacidade de criar clones. É a existência de contratos explícitos que determinam quando um evento foi aceito, quando foi processado, quando foi replicado e quando pode ser descartado com segurança.

O Zig oferece controle sobre memória, concorrência e representação binária para implementar esses mecanismos de forma eficiente. Entretanto, a durabilidade e a consistência dependem do protocolo completo, do armazenamento subjacente e dos testes de falhas, não apenas da linguagem ou de uma estrutura de dados específica.

A prioridade de implementação deve ser construir primeiro o caminho correto e recuperável de um único evento. Depois de provar as garantias de persistência, deduplicação e recuperação, o runtime poderá acrescentar escalonamento elástico, replicação paralela e otimizações de throughput sem comprometer a integridade do estado.

