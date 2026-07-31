# 📦 Store — Microsserviços com Apache Kafka

[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.0-brightgreen)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9.1-231F20)](https://kafka.apache.org/)
[![Java](https://img.shields.io/badge/Java-21-orange)](https://www.oracle.com/java/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)](https://docs.docker.com/compose/)
[![Maven](https://img.shields.io/badge/Maven-Build-red)](https://maven.apache.org/)

> Dois **microsserviços** que se comunicam exclusivamente por **eventos**, usando o **Apache Kafka** como broker de mensageria. O projeto demonstra, na prática, comunicação assíncrona, desacoplamento total entre serviços, consumer groups, controle de offset e **tolerância a falhas do consumidor**.

---

## 🎯 Objetivo

Este projeto foi desenvolvido para demonstrar o domínio de **arquitetura orientada a eventos**, resolvendo os problemas que aparecem quando se troca a chamada HTTP síncrona entre serviços por mensageria:

- Como um serviço notifica outro **sem conhecê-lo**?
- Como responder ao cliente **sem esperar** o processamento downstream?
- O que acontece com as mensagens quando o consumidor **está fora do ar**?
- Como o consumidor sabe **onde parou** ao voltar, sem perder nem reprocessar eventos?
- Como adicionar novos consumidores ao mesmo evento **sem tocar no produtor**?

O domínio de negócio é simples de propósito — registrar uma venda e dar baixa no estoque — justamente para manter o foco na **arquitetura de mensageria**, e não em regras de negócio.

---

## 🏗️ Arquitetura

Dois serviços independentes que **não se conhecem**. Não existe `RestTemplate`, `FeignClient` ou qualquer chamada HTTP entre eles — o único ponto de contato é o nome de um tópico. Na frente deles, o **BFF** serve as duas telas e traduz o formulário para o contrato do produtor.

```
                   ┌───────────────┐     ┌──────────────────┐
                   │   Tela Web    │     │   Tela Mobile    │
                   └───────┬───────┘     └────────┬─────────┘
                           │ /bff/web             │ /bff/mobile
                           └───────────┬──────────┘
                          ┌────────────▼─────────────┐
                          │        bff-service       │  ← um BFF por tela
                          │  gera o UUID · agrega    │
                          └────────────┬─────────────┘
                                       │ POST :8081/venda
                          ┌────────────▼─────────────┐
                          │      venda-service       │  ← Producer
                          │      (KafkaTemplate)     │
                          └────────────┬─────────────┘
                                       │ publica o evento
                          ┌────────────▼─────────────┐
                          │   Apache Kafka (KRaft)   │
                          │   tópico: estoque-topic  │
                          │                          │
                          │   [0][1][2][3][4][5]…    │  ← log persistente
                          │             ▲            │
                          │             └─ offset do │
                          │                consumer  │
                          │                group     │
                          └────────────┬─────────────┘
                                       │ consome (grupo: estoque-group)
                          ┌────────────▼─────────────┐
                          │     estoque-service      │  ← Consumer
                          │     (@KafkaListener)     │
                          └────────────┬─────────────┘
                                       │
                                       ▼
                              baixa no estoque
```

### Fluxo de uma requisição

0. A tela (`:8082`) manda apenas `{ "nome": ..., "produto": ... }` para o **bff-service**, que gera o `id` (UUID) e monta o JSON do evento.
1. O BFF faz um `POST /venda` no **venda-service** (`:8081`).
2. O serviço publica a mensagem no tópico `estoque-topic` e **responde imediatamente** — não há espera pelo processamento.
3. O **Kafka** grava o evento em disco. A mensagem existe independentemente de haver alguém consumindo.
4. O **estoque-service**, inscrito no tópico pelo grupo `estoque-group`, recebe o evento e o processa.
5. O Kafka registra o **offset** do grupo, marcando até onde ele já consumiu.

O produtor não sabe quantos consumidores existem — nem se existe algum. Poderiam surgir serviços de nota fiscal, e-mail e BI lendo o mesmo tópico sem alterar **uma linha** do `venda-service`.

---

## 🧩 Os Serviços

| Serviço             | Porta  | Papel                                                 | Componente-chave                     |
| ------------------- | ------ | ----------------------------------------------------- | ------------------------------------ |
| **bff-service**     | `8082` | Serve as telas web e mobile, cada uma com o seu BFF   | `RestClient` + `AdminClient`         |
| **venda-service**   | `8081` | Registra a venda e publica o evento                   | `KafkaTemplate` + `ProducerFactory`  |
| **estoque-service** | `8080` | Consome o evento e atualiza o estoque                 | `@KafkaListener` + `ConsumerFactory` |
| **kafka**           | `9092` | Broker de mensageria (modo **KRaft**, sem Zookeeper)  | `apache/kafka:3.9.1`                 |
| **kafka-ui**        | `8090` | Inspeção de tópicos, mensagens e lag                  | `provectuslabs/kafka-ui`             |

| Configuração        | Valor                    |
| ------------------- | ------------------------ |
| Tópico              | `estoque-topic`          |
| Consumer group      | `estoque-group`          |
| Serialização        | `String` (chave e valor) |
| `auto-offset-reset` | `earliest`               |

---

## 🔥 Tolerância a Falhas (o coração do projeto)

Este é o ponto central que a arquitetura foi desenhada para demonstrar: **o consumidor pode cair sem derrubar o sistema**.

Numa arquitetura síncrona, se o serviço de estoque sai do ar, toda venda feita nesse intervalo falha — a falha se propaga para o cliente. Com mensageria, não. As vendas continuam sendo registradas normalmente, e o estoque processa tudo quando voltar.

### O experimento

```bash
# 1. Derrube o consumidor
docker compose stop estoque-service

# 2. Registre 3 vendas com ele fora do ar
'{"id":"B"}'
'{"id":"C"}'
'{"id":"D"}'
# → HTTP 200 nas três. O produtor não faz ideia de que o consumidor caiu.

# 3. Confira o acúmulo no broker
```

```
GROUP           TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
estoque-group   estoque-topic  0          1               4               3
```

O **lag de 3** é a métrica que traduz o problema: existem 3 mensagens publicadas que este grupo ainda não processou. Elas estão guardadas, não perdidas.

```bash
# 4. Suba o consumidor de volta
```

```
estoque-group: partitions assigned: [estoque-topic-0]

### Venda recebida no estoque: {"id":"B"}
### Venda recebida no estoque: {"id":"C"}
### Venda recebida no estoque: {"id":"D"}
```

```
GROUP           TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
estoque-group   estoque-topic  0          4               4               0
```

### O detalhe que prova o mecanismo

Repare que ele consumiu **B, C e D — e não reprocessou o A**, que já havia sido confirmado antes da queda. O grupo retomou exatamente do ponto onde parou. 🎯

Isso funciona por três razões:

| Mecanismo                                       | Por quê                                                                                                                           |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| **O Kafka é um log, não uma fila volátil**      | A mensagem é gravada em disco na publicação e permanece pelo tempo de retenção, tenha sido consumida ou não.                      |
| **O offset pertence ao grupo, não ao processo** | O broker guarda, por grupo e partição, qual foi a última mensagem confirmada. Um processo novo do mesmo grupo herda essa posição. |
| **O produtor está desacoplado do consumidor**   | O `send()` conversa apenas com o broker. A saúde do consumidor é irrelevante para o fluxo de venda.                               |

> **`auto-offset-reset=earliest`** define o comportamento apenas quando um grupo é _totalmente novo_ e ainda não tem offset registrado: nesse caso ele lê o tópico desde o início, em vez de ignorar o histórico. Com o grupo já estabelecido, quem manda é o offset commitado.

---

## 📤 O Produtor

`KafkaProducerConfig` monta a fábrica de produtores — endereço do broker e serializadores — e expõe o `KafkaTemplate`:

```java
@Bean
public ProducerFactory<String, String> producerFactory() {
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return new DefaultKafkaProducerFactory<>(configProps);
}
```

E o controller publica em uma linha, sem bloquear a resposta:

```java
@PostMapping("/venda")
public String registrarVenda(@RequestBody String venda) {
    kafkaTemplate.send(topico, venda);
    return "Venda registrada com sucesso! " + venda;
}
```

---

## 📥 O Consumidor

`KafkaConsumerConfig` é o espelho do produtor — mesmo broker, deserializadores no lugar dos serializadores, mais o `groupId` e a política de offset:

```java
configProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
```

Com o `ConcurrentKafkaListenerContainerFactory` no contexto, escutar o tópico é uma anotação:

```java
@KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
public void consumirVenda(String venda) {
    // Aqui viria a lógica de atualizar o estoque, tratar exceções etc.
    System.out.println("### Venda recebida no estoque: " + venda);
}
```

O Spring cuida de conexão, rebalanceamento de partições e commit de offset. O tópico e o grupo vêm do `application.properties` via `${...}`, evitando strings duplicadas entre dois projetos independentes.

---

## 🔌 Os Dois Listeners do Broker

Detalhe de infraestrutura que costuma derrubar setups de Kafka em Docker. O broker anuncia **dois endereços**:

| Listener   | Endereço         | Quem usa                                      |
| ---------- | ---------------- | --------------------------------------------- |
| `EXTERNAL` | `localhost:9092` | Aplicações rodando na máquina host (IDE, CLI) |
| `INTERNAL` | `kafka:29092`    | Containers na rede do Docker                  |

Um endereço único não atende aos dois casos: `localhost` dentro de um container aponta para o próprio container, não para o broker. Do lado da aplicação, um único `application.properties` cobre os dois cenários:

```properties
spring.kafka.bootstrap-servers=${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
```

Na IDE a variável não existe e vale o default; no Docker o Compose a injeta como `kafka:29092`. Sem `if`, sem profile duplicado.

---

## 🛠️ Tecnologias Utilizadas

- **Linguagem**: Java 21
- **Framework**: Spring Boot 4.1.0
- **Mensageria**: Apache Kafka 3.9.1 em modo **KRaft** (sem Zookeeper)
  - **Spring for Apache Kafka** — `KafkaTemplate` e `@KafkaListener`
  - **Consumer Groups** — controle de offset e escalabilidade horizontal
  - **Kafka UI** — inspeção de tópicos, mensagens e lag
- **Containerização**: Docker + Docker Compose (multi-stage build)
- **Testes**: JUnit Jupiter 6 + Mockito, via `spring-boot-starter-webmvc-test`
- **Build**: Maven (com wrapper `mvnw`)

---

## 🚀 Como Executar

### Pré-requisitos

- **Docker Desktop** com Compose v2 — é o que sobe o Kafka
- **JDK 21** e uma IDE, apenas se for rodar os serviços fora do container

### Clonar

```bash
git clone git@github.com:GuilhermeSalles/MS-Pratice.git
cd MS-Pratice
```

### Opção A — Tudo em container

```bash
docker compose up -d --build
docker compose ps
```

```
NAME              STATUS                   PORTS
bff-service       Up                       0.0.0.0:8082->8082/tcp
estoque-service   Up                       0.0.0.0:8080->8080/tcp
kafka             Up (healthy)             0.0.0.0:9092->9092/tcp
kafka-ui          Up                       0.0.0.0:8090->8080/tcp
venda-service     Up                       0.0.0.0:8081->8081/tcp
```

Com tudo no ar, abra a tela web em **http://localhost:8082** e a mobile em **http://localhost:8082/mobile.html**.

### Opção B — Kafka no container, serviços na IDE

Suba só a infraestrutura e rode as aplicações como _Spring Boot App_:

```bash
docker compose up -d kafka kafka-ui
```

Depois execute `VendaServiceApplication`, `EstoqueServiceApplication` e `BffServiceApplication`.

> ⚠️ Não rode as duas opções ao mesmo tempo: os containers e a IDE disputam as portas `8080`, `8081` e `8082`.

### 📊 Kafka UI

Disponível em **http://localhost:8090** — use _Topics → estoque-topic → Messages_ para ver os eventos publicados, e _Consumers → estoque-group_ para acompanhar o **lag** durante o experimento de tolerância a falhas.

---

## 🖥️ O BFF e as Duas Telas

Duas telas, servidas pelo mesmo serviço na porta `8082`:

| Tela       | URL                                 | BFF que ela consome  |
| ---------- | ----------------------------------- | -------------------- |
| **Web**    | `http://localhost:8082`             | `/bff/web/painel`    |
| **Mobile** | `http://localhost:8082/mobile.html` | `/bff/mobile/painel` |

Abrindo a raiz num celular, o desvio para a tela mobile é automático — três linhas no `<head>` do `index.html`, antes de o CSS carregar, então não há piscada:

```js
if (!location.search.includes('web') && window.matchMedia('(max-width: 700px)').matches) {
    location.replace('/mobile.html');
}
```

Cada tela tem um link para a outra, para conseguir ver as duas de qualquer aparelho. O link do mobile aponta para `/?web`, que pula o desvio — e, como usa `location.replace`, o botão voltar do navegador não fica preso num vai e volta.

> Para abrir do celular na mesma rede, troque `localhost` pelo IP da máquina (ex.: `http://192.168.0.10:8082`).

### Por que um BFF por tela

Um BFF é uma camada de backend **dedicada a um tipo de frontend**. Ele agrega chamadas de vários microsserviços e devolve a resposta já no formato daquela tela — e cada tela precisa de coisas diferentes:

- a **web** é grande e cabe tudo: offsets, lag, as duas listas lado a lado, o UUID de cada venda;
- o **mobile** é pequeno: mostra o lag, o estado do consumidor e as últimas vendas. Mais que isso é rede e bateria gastas à toa.

Uma API genérica tentaria agradar as duas e não agradaria nenhuma. Aqui, cada uma tem a sua — e a diferença é medível. Mesmo estado, mesma hora:

```bash
curl -s localhost:8082/bff/web/painel | wc -c      # 439 bytes
curl -s localhost:8082/bff/mobile/painel | wc -c   # 102 bytes
```

```json
// GET /bff/web/painel — DTO rico
{"kafka":{"topico":"estoque-topic","grupo":"estoque-group","gravados":19,"lidos":19,
"lag":0,"consumidores":1,"brokerOnline":true},"estoqueOnline":true,"consumidorAtivo":true,
"publicadas":[{"id":"d4fc8600-f1bd-437e-b784-03a992310481","nome":"Bruce Wayne",
"produto":"Teclado mecanico","horario":"17:00:21"}],"processadas":[{...}]}

// GET /bff/mobile/painel — DTO enxuto
{"lag":0,"consumidorAtivo":true,"ultimasVendas":[{"produto":"Teclado mecanico","horario":"17:00:21"}]}
```

O `MobileBffController` nem chega a chamar `estoque.processadas()` — a tela pequena não mostra essa lista, então é **uma chamada a menos** para o downstream. A diferença entre os dois BFFs vem da tela, nunca da regra de negócio.

### As rotas

| Método | Rota                                    | Descrição                                                      |
| ------ | --------------------------------------- | -------------------------------------------------------------- |
| `POST` | `/bff/{web\|mobile}/vendas`             | Recebe `{nome, produto}`, **gera o UUID** e publica             |
| `GET`  | `/bff/web/painel`                       | DTO rico: offsets, lag, consumidor e as duas listas             |
| `GET`  | `/bff/mobile/painel`                    | DTO enxuto: lag, consumidor e as 5 últimas vendas               |
| `POST` | `/bff/{web\|mobile}/consumidor/parar`   | Derruba o listener do estoque (simulação de falha)              |
| `POST` | `/bff/{web\|mobile}/consumidor/iniciar` | Sobe o listener de volta                                        |

Repare no que **não** mudou: o `venda-service` continua com o mesmo `POST /venda` recebendo uma `String` crua, sem saber que existe um BFF na frente dele. A adaptação para a tela mora no BFF — não no microsserviço, nem no navegador.

### As quatro classes que fazem o BFF

```
client/VendaClient      → publica no venda-service (POST /venda)
client/EstoqueClient    → pergunta ao estoque-service o que ele já processou
kafka/KafkaMonitor      → lê offsets e lag do broker (AdminClient, só leitura)
service/RegistroDeVendas→ gera o UUID e guarda o histórico da sessão
```

Os dois controllers injetam essas classes, agregam e montam o DTO. É isso — um BFF é, na essência, um controller que chama vários clients e devolve um DTO sob medida.

O `RegistroDeVendas` é compartilhado pelos dois BFFs de propósito: é a mitigação clássica da **duplicação entre BFFs** — a orquestração comum fica num lugar só e cada controller cuida apenas do formato da sua tela.

O BFF também **não produz nem consome** eventos. O `AdminClient` é só leitura, e é de lá que sai o número central do painel:

```java
// lag = o que foi gravado no log  -  o que o grupo confirmou ter lido
long lag = gravados - lidos;
```

É o mesmo número do `kafka-consumer-groups.sh --describe` — não uma contagem da aplicação.

### Falha parcial: o BFF trata como estado, não como erro

Se o `estoque-service` cai, o `EstoqueClient` devolve lista vazia e `offline` em vez de propagar a exception. A tela continua registrando vendas normalmente — só o pedaço que depende do estoque aparece apagado. Somado ao timeout curto de 2s em toda chamada de saída, é o que impede um serviço morto de derrubar o painel inteiro.

### O experimento pela tela

1. Registre uma venda: a célula do offset aparece em **verde**, já lida.
2. Clique em **Derrubar consumidor**: o selo vira `consumidor parado` e os *consumidores no grupo* caem para `0`.
3. Registre mais três vendas — todas respondem normalmente. As células entram em **âmbar** e o lag sobe para `3`.
4. Clique em **Subir consumidor**: as três viram verde e o aviso `3 eventos processados de uma vez` aparece.

As vendas anteriores à queda continuam verdes o tempo todo: o grupo retoma do offset commitado e não reprocessa nada. Funciona igual nas duas telas.

> Derrubar o `estoque-service` de verdade (`docker compose stop estoque-service`) produz o mesmo efeito — o selo passa a `consumidor fora do ar`. A diferença é que a lista **Processadas** fica em memória e reinicia junto com o serviço; o offset, que é o que importa, continua no broker.

### BFF ≠ API Gateway

| | Papel |
| --- | --- |
| **BFF** | Conhece a **tela**. Agrega e formata dados para um frontend específico. Tem lógica de apresentação. |
| **API Gateway** | Genérico. Roteamento, auth, rate limit, TLS para todo mundo. Não sabe o que a tela precisa. |

Na prática coexistem: o Gateway é a porta de entrada, e atrás dele vivem os BFFs.

---

## 📡 Endpoints

| Método | Rota                                       | Descrição                                              |
| ------ | ------------------------------------------ | ------------------------------------------------------ |
| `POST` | `http://localhost:8082/bff/web/vendas`     | Registra a venda pela tela web (o BFF gera o `id`)     |
| `POST` | `http://localhost:8082/bff/mobile/vendas`  | O mesmo, com resposta enxuta para a tela mobile        |
| `POST` | `http://localhost:8081/venda`              | Registra a venda e publica o evento em `estoque-topic` |

### No Postman

**Body** → `raw` → `JSON`:

```json
{
  "id": "{{$randomUUID}}",
  "nome": "{{$randomFullName}}",
  "produto": "{{$randomCommerceProductName}}"
}
```

As variáveis dinâmicas do Postman geram dados novos a cada _Send_ — prático para disparar várias vendas em sequência sem editar nada.

**Resposta:**

```
Venda registrada com sucesso! {
  "id": "bfbbd379-24ec-4675-a1ec-c6161e61390a",
  "nome": "Bruce Wayne",
  "produto": "Ergonomic Wooden Keyboard"
}
```

**Console do estoque-service:**

```
### Venda recebida no estoque: {
  "id": "bfbbd379-24ec-4675-a1ec-c6161e61390a",
  "nome": "Bruce Wayne",
  "produto": "Ergonomic Wooden Keyboard"
}
```

---

## 🧪 Testes

Cada serviço tem uma classe de teste com **JUnit Jupiter** validando a sua regra principal. Nenhuma delas exige um broker no ar — rodam isoladas, em qualquer máquina ou pipeline de CI.

O JUnit e o Mockito não aparecem no `pom.xml`: vêm transitivamente do `spring-boot-starter-webmvc-test`, com as versões já alinhadas pelo BOM do Spring Boot.

| Serviço             | Classe                       | O que valida                                                                |
| ------------------- | ---------------------------- | --------------------------------------------------------------------------- |
| **venda-service**   | `VendaControllerTest`        | O `POST /venda` responde `200` e publica a mensagem **no tópico correto**   |
| **estoque-service** | `EstoqueListenerServiceTest` | O listener processa a mensagem recebida do tópico                           |
| **bff-service**     | `RegistroDeVendasTest`       | O BFF **gera o UUID** e publica no `venda-service` — o id nunca vem da tela |

---

## 🗂️ Estrutura do Repositório

```
MS-Pratice/
├── docker-compose.yml              # Kafka (KRaft) + Kafka UI + os três serviços
├── bff-service/                    # BFF       — :8082  (as duas telas)
│   ├── Dockerfile
│   ├── src/main/java/com/store/bff_service/
│   │   ├── client/
│   │   │   ├── VendaClient.java               # fala com o venda-service
│   │   │   └── EstoqueClient.java             # fala com o estoque-service
│   │   ├── controller/
│   │   │   ├── WebBffController.java          # /bff/web    → DTO rico
│   │   │   └── MobileBffController.java       # /bff/mobile → DTO enxuto
│   │   ├── dto/                               # NovaVenda, Venda, VendaResumo, Painel*
│   │   ├── kafka/KafkaMonitor.java            # offsets e lag (AdminClient)
│   │   ├── service/RegistroDeVendas.java      # gera o UUID, compartilhado pelos 2 BFFs
│   │   └── config/BffConfig.java              # RestClients + AdminClient
│   └── src/main/resources/static/
│       ├── index.html                         # tela web
│       ├── mobile.html                        # tela mobile
│       ├── css/  (base + web + mobile)
│       └── js/   (web.js + mobile.js)
├── venda-service/                  # PRODUCER  — :8081
│   ├── Dockerfile
│   ├── src/main/java/com/store/venda_service/
│   │   ├── config/KafkaProducerConfig.java
│   │   └── controller/VendaController.java
│   └── src/test/java/com/store/venda_service/
│       └── controller/VendaControllerTest.java
└── estoque-service/                # CONSUMER  — :8080
    ├── Dockerfile
    ├── src/main/java/com/store/estoque_service/
    │   ├── config/KafkaConsumerConfig.java
    │   ├── controller/EstoqueController.java   # eventos + parar/subir o listener
    │   └── service/
    │       ├── EstoqueListenerService.java
    │       └── MemoriaEstoque.java             # eventos processados, em memória
    └── src/test/java/com/store/estoque_service/
        └── service/EstoqueListenerServiceTest.java
```

---

## 💡 Conceitos Demonstrados

- ✅ Arquitetura **orientada a eventos** com serviços totalmente desacoplados
- ✅ **Comunicação assíncrona** — resposta ao cliente sem esperar o downstream
- ✅ **Producer e Consumer** configurados explicitamente com Spring for Apache Kafka
- ✅ **Consumer Group** com controle de offset e retomada após falha
- ✅ **Tolerância a falhas** — o consumidor cai, o produtor continua, nada se perde
- ✅ **Persistência de eventos** — o Kafka como log, não como fila volátil
- ✅ **Kafka em modo KRaft**, sem dependência de Zookeeper
- ✅ **Containerização** com Docker Compose e build multi-stage
- ✅ Configuração **portável** entre execução local e containerizada
- ✅ **Testes automatizados** com JUnit Jupiter, isolados de infraestrutura
- ✅ **Backend for Frontend** — um BFF por tela (web e mobile), sem tocar nos microsserviços
- ✅ **Observabilidade do broker** via `AdminClient` (offsets e lag lidos na fonte)
- ✅ **Falha como estado, não como erro** — o painel segue funcionando com o consumidor fora do ar

---

## 👤 Autor

**Guilherme Baltazar Vericimo de Sales**

[![LinkedIn](https://img.shields.io/badge/-LinkedIn-%230077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/guilherme-baltazar-0028361a1)
[![Instagram](https://img.shields.io/badge/-Instagram-%23E4405F?style=for-the-badge&logo=instagram&logoColor=white)](https://instagram.com/yguilhermeb)

---
