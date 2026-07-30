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

Dois serviços independentes que **não se conhecem**. Não existe `RestTemplate`, `FeignClient` ou qualquer chamada HTTP entre eles — o único ponto de contato é o nome de um tópico.

```
                          ┌──────────────────────────┐
                          │    Cliente / Postman     │
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

1. O cliente faz um `POST /venda` no **venda-service** (`:8081`).
2. O serviço publica a mensagem no tópico `estoque-topic` e **responde imediatamente** — não há espera pelo processamento.
3. O **Kafka** grava o evento em disco. A mensagem existe independentemente de haver alguém consumindo.
4. O **estoque-service**, inscrito no tópico pelo grupo `estoque-group`, recebe o evento e o processa.
5. O Kafka registra o **offset** do grupo, marcando até onde ele já consumiu.

O produtor não sabe quantos consumidores existem — nem se existe algum. Poderiam surgir serviços de nota fiscal, e-mail e BI lendo o mesmo tópico sem alterar **uma linha** do `venda-service`.

---

## 🧩 Os Serviços

| Serviço             | Porta  | Papel                                                | Componente-chave                     |
| ------------------- | ------ | ---------------------------------------------------- | ------------------------------------ |
| **venda-service**   | `8081` | Registra a venda e publica o evento                  | `KafkaTemplate` + `ProducerFactory`  |
| **estoque-service** | `8080` | Consome o evento e atualiza o estoque                | `@KafkaListener` + `ConsumerFactory` |
| **kafka**           | `9092` | Broker de mensageria (modo **KRaft**, sem Zookeeper) | `apache/kafka:3.9.1`                 |
| **kafka-ui**        | `8090` | Inspeção de tópicos, mensagens e lag                 | `provectuslabs/kafka-ui`             |

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
estoque-service   Down                       0.0.0.0:8080->8080/tcp
kafka             Up (healthy)             0.0.0.0:9092->9092/tcp
kafka-ui          Up                       0.0.0.0:8090->8080/tcp
venda-service     Down                       0.0.0.0:8081->8081/tcp
```

Depois execute `EstoqueServiceApplication` e `VendaServiceApplication` como _Spring Boot App_. Porém necessário pausar estoque-service e venda-service no docker.

> ⚠️ Não rode as duas opções ao mesmo tempo: os containers e a IDE disputam as portas `8080` e `8081`.

### 📊 Kafka UI

Disponível em **http://localhost:8090** — use _Topics → estoque-topic → Messages_ para ver os eventos publicados, e _Consumers → estoque-group_ para acompanhar o **lag** durante o experimento de tolerância a falhas.

---

## 📡 Endpoint

| Método | Rota                          | Descrição                                              |
| ------ | ----------------------------- | ------------------------------------------------------ |
| `POST` | `http://localhost:8081/venda` | Registra a venda e publica o evento em `estoque-topic` |

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

| Serviço             | Classe                       | O que valida                                                              |
| ------------------- | ---------------------------- | ------------------------------------------------------------------------- |
| **venda-service**   | `VendaControllerTest`        | O `POST /venda` responde `200` e publica a mensagem **no tópico correto** |
| **estoque-service** | `EstoqueListenerServiceTest` | O listener processa a mensagem recebida do tópico                         |

No produtor, o `KafkaTemplate` é substituído por um mock: o teste verifica o **contrato da publicação**, não a infraestrutura.

```java
@WebMvcTest(VendaController.class)
class VendaControllerTest {

    @MockitoBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void devePublicarVendaNoTopico() throws Exception {
        mockMvc.perform(post("/venda")
                .contentType(MediaType.APPLICATION_JSON)
                .content(venda))
                .andExpect(status().isOk());

        verify(kafkaTemplate).send("estoque-topic", venda);
    }
}
```

```bash
cd venda-service && ./mvnw test
cd ../estoque-service && ./mvnw test
```

---

## 🗂️ Estrutura do Repositório

```
MS-Pratice/
├── docker-compose.yml              # Kafka (KRaft) + Kafka UI + os dois serviços
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
    │   └── service/EstoqueListenerService.java
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

---

## 👤 Autor

**Guilherme Baltazar Vericimo de Sales**

- **LinkedIn**: [guilhermebaltazar-v](https://www.linkedin.com/in/guilhermebaltazar-v)
- **GitHub**: [@GuilhermeSalles](https://github.com/GuilhermeSalles)

---
