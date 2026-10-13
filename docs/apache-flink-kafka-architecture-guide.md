# Guía de Arquitectura Empresarial: Apache Flink & Kafka
## Arquetipo de Stream Processing con Semántica Exactly-Once en Tiempo Real

> **Estado del Documento:** Arquitectura de Referencia Aprobada  
> **Audiencia Objetivo:** Arquitectos Empresariales, Ingenieros de Plataforma de Datos, Ingenieros de Software Backend  
> **Alcance:** Guía de referencia idempotente y autocontenida para procesamiento distribuido de flujos con Apache Flink 2.x, Apache Kafka, Infraestructura Cloud en AWS, y patrones de producción adoptados de la ingeniería de Uber.

---

## Tabla de Contenidos
1. [Resumen Ejecutivo y Propósito del Arquetipo](#1-resumen-ejecutivo-y-propósito-del-arquetipo)
2. [Modelo Mental de Stream Processing y Fundamentos de Apache Flink](#2-modelo-mental-de-stream-processing-y-fundamentos-de-apache-flink)
   - [2.1 Taxonomía y Mapeo de Conceptos: Debezium, Kafka, Flink y Kubernetes](#21-taxonomía-y-mapeo-de-conceptos-debezium-kafka-flink-y-kubernetes)
   - [2.2 Topología de Ejecución: Arquitectura Master-Worker, 5 Piezas Anatómicas y Decisiones de Red](#22-topología-de-ejecución-arquitectura-master-worker-en-runtime)
   - [2.3 Semántica de Tiempo y Watermarks](#23-semántica-de-tiempo-y-watermarks)
   - [2.4 Gestión de Estado y State Backends](#24-gestión-de-estado-y-state-backends)
   - [2.5 Tolerancia a Fallos: Asynchronous Barrier Snapshotting (ABS)](#25-tolerancia-a-fallos-asynchronous-barrier-snapshotting-abs)
   - [2.6 Checkpoints vs Savepoints](#26-checkpoints-vs-savepoints)
3. [Integración Kafka & Flink: El Camino a Exactly-Once (EOS)](#3-integración-kafka--flink-el-camino-a-exactly-once-eos)
   - [3.1 La Falacia de "Kafka por sí solo garantiza Exactly-Once"](#31-la-falacia-de-kafka-por-sí-solo-garantiza-exactly-once)
   - [3.2 KafkaSource: Gestión de Offsets y Partition Discovery](#32-kafkasource-gestión-de-offsets-y-partition-discovery)
   - [3.3 KafkaSink y el Protocolo Two-Phase Commit (2PC)](#33-kafkasink-y-el-protocolo-two-phase-commit-2pc)
   - [3.4 Trade-off: Intervalo de Checkpoint vs Latencia End-to-End](#34-trade-off-intervalo-de-checkpoint-vs-latencia-end-to-end)
   - [3.5 Patrón de Ingesta CDC: Debezium -> Kafka -> Apache Flink](#35-patrón-de-ingesta-cdc-debezium---kafka---apache-flink)
   - [3.6 Patrón Empresarial: Enriquecimiento Multi-Stream Asíncrono, Reglas Dinámicas y Persistencia de Estado](#36-patrón-empresarial-enriquecimiento-multi-stream-asíncrono-reglas-dinámicas-broadcast-state-y-persistencia-de-estado)
4. [Caso de Estudio: Procesamiento de Eventos de Ads en Uber Engineering](#4-caso-de-estudio-procesamiento-de-eventos-de-ads-en-uber-engineering)
   - [4.1 Problema de Negocio y Desafíos Técnicos](#41-problema-de-negocio-y-desafíos-técnicos)
   - [4.2 Blueprint de Arquitectura: Kafka + Flink + Docstore + Pinot](#42-blueprint-de-arquitectura-kafka--flink--docstore--pinot)
   - [4.3 Patrones de Arquitectura a Adoptar](#43-patrones-de-arquitectura-a-adoptar)
     - [Patrón A: UUIDs de Registro Deterministas y Downstream Upsert](#patrón-a-uuids-de-registro-deterministas-y-downstream-upsert)
     - [Patrón B: Deduplicación Temprana en el Stream con KeyedState y TTL](#patrón-b-deduplicación-temprana-en-el-stream-con-keyedstate-y-ttl)
     - [Patrón C: Agregación Incremental Eficiente en Memoria](#patrón-c-agregación-incremental-eficiente-en-memoria)
     - [Patrón D: Disaster Recovery Activo-Activo Multi-Región](#patrón-d-disaster-recovery-activo-activo-multi-región)
5. [Arquitectura de Infraestructura e Ingeniería Cloud (AWS)](#5-arquitectura-de-infraestructura-e-ingeniería-cloud-aws)
   - [5.1 Transición Conceptual: De Docker a Kubernetes (AWS EKS)](#51-transición-conceptual-de-docker-a-kubernetes-aws-eks)
   - [5.2 Evaluación de Plataforma de Cómputo: EKS vs Fargate vs EC2](#52-evaluación-de-plataforma-de-cómputo-eks-vs-fargate-vs-ec2)
   - [5.3 Topología de Clúster: EKS Dedicado vs EKS Compartido con Kafka](#53-topología-de-clúster-eks-dedicado-vs-eks-compartido-con-kafka)
   - [5.4 Niveles de Almacenamiento: Volúmenes NVMe Scratch y Almacén S3](#54-niveles-de-almacenamiento-volúmenes-nvme-scratch-y-almacén-s3)
   - [5.5 Topología de Red y Conectividad Cross-VPC](#55-topología-de-red-y-conectividad-cross-vpc)
   - [5.6 Estrategia de Despliegue en Producción (Zero Data Loss para Misión Crítica)](#56-estrategia-de-despliegue-en-producción-zero-data-loss-para-misión-crítica)
6. [Configuración del Proyecto Java y Screaming Architecture](#6-configuración-del-proyecto-java-y-screaming-architecture)
   - [6.1 Screaming Architecture / Vertical Slices](#61-screaming-architecture--vertical-slices)
   - [6.2 Alcance de Dependencias Maven y Estrategias de Shading](#62-alcance-de-dependencias-maven-y-estrategias-de-shading)
   - [6.3 Cadencia de Versiones LTS en Java y Riesgos de Java 27](#63-cadencia-de-versiones-lts-en-java-y-riesgos-de-java-27)
   - [6.4 Eficiencia de Serialización POJO y Trampas de Kryo](#64-eficiencia-de-serialización-pojo-y-trampas-de-kryo)
7. [Experiencia del Desarrollador Local y Runbook (`poc-apache-flink`)](#7-experiencia-del-desarrollador-local-y-runbook-poc-apache-flink)
   - [7.1 Stack de Arquitectura Local](#71-stack-de-arquitectura-local)
   - [7.2 Pirámide de Testing: Unit, Harness, MiniCluster y Cloud Emulado](#72-pirámide-de-testing-unit-harness-minicluster-y-cloud-emulado)
8. [Architecture Decision Records (ADRs)](#8-architecture-decision-records-adrs)
   - [ADR-001: Segregación Estricta Cómputo Flink vs Almacenamiento Kafka](#adr-001-segregación-estricta-entre-cómputo-flink-y-almacenamiento-kafka)
   - [ADR-002: EmbeddedRocksDBStateBackend con Checkpoints en S3](#adr-002-embeddedrocksdbstatebackend-con-nvme-local-y-checkpoints-en-s3)
   - [ADR-003: Claves de Idempotencia Deterministas para Sinks](#adr-003-claves-de-idempotencia-deterministas-para-sinks-downstream)
   - [ADR-004: Screaming Architecture y Serialización POJO en Java 21 LTS](#adr-004-screaming-architecture-y-serialización-pojo-estricta-en-java-21-lts)
   - [ADR-005: Broadcast State Pattern para Reglas Dinámicas en Memoria (Cero Redis)](#adr-005-broadcast-state-pattern-para-reglas-dinámicas-en-memoria-cero-redis-vs-primitivas-de-transporte)

---

## 1. Resumen Ejecutivo y Propósito del Arquetipo

Las organizaciones modernas orientadas a eventos enfrentan un dilema recurrente: los microservicios tradicionales de solicitud-respuesta y los procesadores por micro-lotes (como Spark Streaming con lotes de 1 segundo) tienen dificultades para ofrecer toma de decisiones en submilisegundos, gestión de estado de sesiones y exactitud financiera auditable de manera simultánea.

El **arquetipo `poc-apache-flink`** provee un blueprint reproducible y de grado productivo para construir pipelines distribuidos de procesamiento de flujos con estado (*stateful stream processing*). Implementa un modelo estricto de **Semántica Exactly-Once (EOS)** a lo largo de todo el ciclo de vida del evento:

```text
┌──────────────────────┐         At-Least-Once        ┌───────────────────────┐
│   Event Producers    ├─────────────────────────────►│     Apache Kafka      │
│   (Mobile / Edge)    │                              │ (KRaft Ingestion Log) │
└──────────────────────┘                              └───────────┬───────────┘
                                                                  │ read_committed
                                                                  ▼
┌──────────────────────┐   Checkpoints & Savepoints   ┌───────────────────────┐
│    AWS S3 / Floci    │◄·····························┤   Apache Flink 2.x    │
│  (Durable Storage)   │                              │ (JobManager + Workers)│
└──────────────────────┘                              └───┬───────────────┬───┘
                                                          │               │
                            Idempotent Put (UUID)         │               │ 2PC Exactly-Once
                                  ┌───────────────────────┘               ▼
                                  ▼                           ┌───────────────────────┐
                       ┌──────────────────────┐               │   Downstream Kafka    │
                       │ DynamoDB Ledger Store│               │  (Aggregated Topics)  │
                       └──────────────────────┘               └───────────────────────┘
```

### Objetivos de Negocio y Técnicos Clave
- **Cero Pérdida de Datos y Cero Doble Conteo:** Exactitud rigurosa para libros contables financieros (*ledgers*), facturación publicitaria, detección de fraude en tiempo real y saldos de inventario.
- **Latencia End-to-End en Subsegundos:** Arquitectura de procesamiento evento a evento en lugar de acumulación por micro-lotes.
- **Operaciones Autónomas:** Infraestructura como Código (Terraform), modelos de despliegue declarativos y migración de estado sin indisponibilidad mediante Savepoints.
- **Velocidad de Desarrollo:** Ciclo de retroalimentación interna ultra rápido mediante MiniClusters integrados en el IDE y emulación de AWS en contenedores (Floci).

---

## 2. Modelo Mental de Stream Processing y Fundamentos de Apache Flink

### 2.1 Taxonomía y Mapeo de Conceptos: Debezium, Kafka, Flink y Kubernetes

En una arquitectura de misión crítica de CDC (Change Data Capture), no se debe asumir conocimiento implícito. Para operar y diagnosticar este pipeline, los ingenieros y arquitectos deben dominar la correlación física y lógica entre las cuatro capas de la plataforma:

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        MAPEO ESTRUCTURAL DE CONCEPTOS Y ESCALA                         │
└────────────────────────────────────────────────────────────────────────────────────────┘

 [1] Base de Datos (PostgreSQL / MySQL)
     └── WAL / Binlog: Registro append-only de transacciones físicas.
         └── LSN (Log Sequence Number): Puntero monótono irrefutable de orden cronológico.
              │
              ▼ (Debezium CDC en Kafka Connect)
 [2] Apache Kafka (KRaft Broker)
     └── Topic: Canal lógico de eventos (`transactions.incoming`).
         └── Partición (Partition): Unidad física de almacenamiento y concurrencia.
             └── Offset: Índice posicional inmutable del mensaje en la partición.
                  │
                  ▼ (Flink KafkaSource)
 [3] Apache Flink (Streaming Runtime)
     └── JobManager (Master): Coordinador central de DAG, barreras y Checkpoints.
     └── TaskManager (Worker JVM): Proceso que aloja slots de cómputo y RocksDB local.
         └── Task Slot: Unidad atómica de ejecución (1 slot ≈ 1 hilo de CPU en la JVM).
              │
              ▼ (Kubernetes EKS Orchestration)
 [4] AWS EKS (Infraestructura Cloud)
     └── Worker Node: Instancia EC2 física/virtual con CPU, RAM y NVMe local.
         └── Pod: Contenedor Linux que hospeda exactamente un proceso JVM de TaskManager.
             └── PersistentVolume / hostPath: Montaje del SSD NVMe para RocksDB (`/opt/flink/rocksdb`).
```

#### Glosario de Conceptos Fundamentales:

1. **Particiones de Kafka (Kafka Partitions), Offsets y Clave Primaria (PK):**
   - **Aislamiento Multitabla por Tópico:** Debezium asigna cada tabla a su propio tópico independiente en Kafka (ej. `cdc.orders` y `cdc.products`). Los identificadores numéricos de tablas distintas jamás se solapan ni compiten porque residen en canales desacoplados.
   - **Partición:** Es la unidad física de almacenamiento y paralelismo en Kafka. **Kafka garantiza orden cronológico estricto ÚNICAMENTE dentro de una misma partición**.
   - **Offset:** Es un número secuencial asignado a cada mensaje dentro de una partición (análogo a un índice de array). Es inmutable.
   - **Hash Partitioning por PK (Evitando el Colapso de Rendimiento):** No se crea una partición por cada registro de base de datos (eso colapsaría Kafka con millones de particiones), ni tampoco una sola partición por tabla (lo que reduciría el consumo a un único hilo). Debezium utiliza la **Primary Key (PK)** de la tabla como clave del mensaje, y Kafka calcula:
     `Partición = hash(PK) % Total_Particiones`
     De esta forma, todas las mutaciones (`INSERT`, `UPDATE`, `DELETE`) de un mismo registro (ej. `order_id = 99`) caen **siempre en la misma partición** preservando orden estricto, mientras que miles de registros distintos se procesan en paralelo a través de todas las particiones del tópico.

2. **Task Slots, TaskManagers y Pods de Kubernetes (EKS):**
   - **¿Qué se replica en Kubernetes? (Scale-Out de Pods):** En Kubernetes, tú replicas **el Pod del TaskManager** (`replicas: N`). El TaskManager no es una lógica de negocio codificada en una imagen distinta; es un motor worker genérico de ejecución JVM (`flink:2.0.2-java21`). Tu código de negocio (el JAR) reside en el JobManager, el cual compila el DAG, lo fragmenta en subtareas paralelas (*subtasks*) e inyecta dinámicamente esas subtareas dentro de los slots de los Pods replicados.
   - **Aislamiento Estricto: 1 Pod = 1 TaskManager:** Cada Pod aloja **exactamente un proceso JVM de TaskManager**. No se ejecutan múltiples TaskManagers dentro de un mismo Pod para no vulnerar el aislamiento de recursos de Linux (`cgroups`), no generar contención en memoria off-heap y permitir que el orquestador gestione reinicios y autoescalado por contenedor de forma atómica.
   - **Task Slot (No es una Unidad Cloud):** Un slot no equivale a instancias de cómputo ni a Fargate; es la unidad lógica de concurrencia dentro de la JVM (un hilo de CPU que comparte el heap y la memoria administrada).
   - **Relación con Nodos EC2:** EKS es el plano de control que gestiona la flota de instancias EC2 (Worker Nodes). Los Pods se distribuyen sobre estas EC2 para disponer de almacenamiento NVMe físico local para RocksDB, ventaja que entornos como AWS Fargate no ofrecen (ver [Sección 5.2](#52-evaluación-de-plataforma-de-cómputo-eks-vs-fargate-vs-ec2)).
   - **La Regla de Oro del Paralelismo:**
     `Paralelismo Máximo de Consumo = Número de Particiones del Topic de Kafka`
     *Ejemplo:* Si el topic tiene 12 particiones, asignar un paralelismo de 16 slots dejará 4 slots completamente ociosos en el operador Source. Se dimensiona: **12 Particiones -> 12 Task Slots -> distribuidos en 3 Pods (con 4 slots por Pod)**.

3. **Debezium CDC: WAL, LSN y Envelopes:**
   - **WAL (Write-Ahead Log) / Binlog:** El mecanismo de almacenamiento de la base de datos donde se registran todas las modificaciones antes de aplicarlas a las páginas de datos en disco.
   - **LSN (Log Sequence Number):** Un entero monótono creciente de 64 bits que representa la posición física del commit en el WAL. Es el único valor que garantiza cuál transacción ocurrió antes en la historia del motor de base de datos.
   - **Envelope CDC:** El JSON/Avro generado por Debezium que envuelve el evento, conteniendo el estado anterior (`before`), el nuevo estado (`after`), el tipo de operación (`op`) y el metadato (`source.lsn`).

| Capa | Entidad Lógica | Unidad Física de Concurrencia | Localizador de Posición / Progreso |
|---|---|---|---|
| **Base de Datos** | Tabla Relacional | Transacciones Concurrentes en Motor | **LSN** (Postgres) / Binlog Position (MySQL) |
| **Debezium CDC** | Conector de Tabla | Tarea (Task Worker) en Kafka Connect | Offset de lectura de WAL guardado en Kafka |
| **Apache Kafka** | Topic | **Partición (Partition)** | **Offset** de Mensaje dentro de la Partición |
| **Apache Flink** | Stream Graph (DAG) | **Task Slot** (Hilo de Ejecución) | **Checkpoint Barrier** + Offsets guardados en S3 |
| **AWS EKS** | `FlinkDeployment` CRD | **Pod** (Contenedor TaskManager) | Identificador de Pod / Asignación en Node EC2 |

---

### 2.2 Topología de Ejecución: Arquitectura Master-Worker en Runtime

Apache Flink emplea una arquitectura distribuida tipo Master-Worker:

```text
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             Cliente (CLI / IDE)                                  │
│                   Envía JobGraph (JAR / Local MiniCluster)                       │
└────────────────────────────────────────┬─────────────────────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         JobManager (Master Node)                                 │
│  ┌─────────────────────────┐  ┌───────────────────────┐  ┌────────────────────┐  │
│  │       Dispatcher        │  │     JobMaster         │  │  ResourceManager   │  │
│  │   (REST API / Web UI)   │  │ (Coordina Checkpoints)│  │ (Asigna Slots)     │  │
│  └─────────────────────────┘  └───────────────────────┘  └────────────────────┘  │
└───────────────────────────┬──────────────────────────────────┬───────────────────┘
                            │ Asignación de Slots              │
                            ▼                                  ▼
┌──────────────────────────────────────────────┐   ┌───────────────────────────────┐
│         TaskManager 1 (Worker Pod)           │   │  TaskManager 2 (Worker Pod)   │
│  ┌────────────────────┐┌───────────────────┐ │   │ ┌───────────────────────────┐ │
│  │ Task Slot 1 (Hilo) ││Task Slot 2 (Hilo) │ │   │ │    Task Slot 1 (Hilo)     │ │
│  └────────────────────┘└───────────────────┘ │   │ └───────────────────────────┘ │
│  ┌─────────────────────────────────────────┐ │   │ ┌───────────────────────────┐ │
│  │ EmbeddedRocksDBStateBackend (NVMe Local)│ │   │ │ EmbeddedRocksDBStateBackend │ │
│  └─────────────────────────────────────────┘ │   │ └───────────────────────────┘ │
└──────────────────────┬───────────────────────┘   └───────────────▲───────────────┘
                       │                                           │
                       └──────── Network Buffers / Data Shuffle ───┘
```

#### Componentes Principales
1. **JobManager (Nodo Master):**
   - **Dispatcher:** Expone la interfaz REST y el Web Dashboard para someter y administrar aplicaciones.
   - **ResourceManager:** Monitorea la disponibilidad de *task slots* e interactúa con el orquestador base (Kubernetes, YARN o Standalone).
   - **JobMaster:** Se instancia por cada job en ejecución. Transforma el **DAG (Directed Acyclic Graph)** del usuario en un `ExecutionGraph` físico distribuido, coordina las barreras de checkpoint y gestiona reinicios de tareas.
   - **¿Qué es el DAG en Runtime?** No es un mero diagrama cosmético de la interfaz gráfica. Es la estructura matemática de datos que modela el pipeline:
     - **Grafo:** Operadores (nodos: Source, Filter, KeyBy, Sink) interconectados por flujos de registros (aristas).
     - **Dirigido:** El dato fluye en un único sentido hacia adelante sin retrocesos.
     - **Acíclico:** Libre de bucles infinitos cerrados.
     - **Propósito Operativo:** El JobMaster compila el DAG para calcular los requisitos de red (canales Netty), particionar el estado entre slots y trazar la ruta exacta por donde viajarán las barreras de Checkpoint.
2. **TaskManager (Nodos Worker):**
   - Ejecutan los operadores del flujo (Source, Map, Filter, KeyBy, Window, Sink).
   - Dividen sus recursos en **Task Slots** (un slot típicamente equivale a un hilo de CPU que comparte el JVM heap).
   - Mantienen el motor de estado local (`EmbeddedRocksDBStateBackend`).
   - Intercambian registros entre nodos a través de buffers de red gestionados por Netty.

#### Anatomía de un Pipeline Flink: Nodos, Aristas, Inmutabilidad y Reglas de Orden

Un pipeline de streaming en Flink no es un script imperativo tradicional; es una composición funcional declarativa de operadores interconectados.

##### 1. La Inmutabilidad del DAG (Tuberías Fijas vs Válvulas de Flujo)
- **El Grafo es Inmutable:** Una vez que se ejecuta `env.execute()`, la topología del DAG queda congelada en memoria y desplegada en los Pods. **Un mensaje individual no puede alterar, crear ni eliminar nodos en tiempo de ejecución.**
- **Manejo de Bifurcaciones Dinámicas (Side Outputs):** Si la lógica de negocio exige desviar eventos según condiciones de negocio (ej. separar transacciones legítimas de transacciones fraudulentas o registros con errores), **todas las rutas deben pre-cablearse en el DAG**. Se utiliza el patrón de **Side Outputs (Salidas Laterales)** mediante un `OutputTag`:

```text
                                       ┌────────► [Sink: DynamoDB] (Ruta Principal: Válidas)
                                       │
 [KafkaSource] ──► [KeyedProcessFunction]
  (Nodo Origen)      (Válvula if/else)
                                       │
                                       └────────► [Sink: Kafka Dead-Letter] (Side Output: Fraudes)
```

##### 2. Las 5 Piezas Anatómicas de un Pipeline

| Categoría | Rol en el Grafo | Primitivas Disponibles | ¿Genera Tráfico de Red (Shuffle)? |
|---|---|---|---|
| **[1] Source** | Punto de entrada del stream. | `fromSource(KafkaSource)`, `fromSource(FileSource)` | No (corre en el slot del reader). |
| **[2] Stateless** | Transformación pura 1:1, 1:N o filtro sin memoria. | `map()`, `flatMap()`, `filter()` | No (se encadena en el mismo hilo vía *Operator Chaining*). |
| **[3] Aristas (Particionado)** | Define cómo se redistribuyen los datos entre slots. | `keyBy()`, `rebalance()`, `rescale()`, `broadcast()`, `forward()` | **SÍ** (excepto `forward` que se mantiene en el mismo hilo). |
| **[4] Stateful & Time** | Operaciones complejas con memoria local y temporizadores. | `KeyedProcessFunction`, `window()`, `intervalJoin()` | Procesa en el slot asignado al hash de la clave. |
| **[5] Sink** | Punto final de persistencia o emisión. | `sinkTo(KafkaSink)`, `sinkTo(DynamoDBSink)` | Depende del particionado final. |

##### 3. Estrategias de Red (Aristas entre Nodos)
El desarrollador decide explícitamente cómo viajan los datos entre operadores y cómo se distribuyen entre los Task Slots del clúster:

- **`keyBy(key)`:** Agrupa registros por clave calculando el hash (`Math.abs(key.hashCode()) % maxParallelism`). **Es la única forma de habilitar estado claveado (`KeyedState`) y temporizadores por clave.** Todos los eventos con la misma clave viajan obligatoriamente al mismo slot.
- **`rebalance()`:** Distribuye registros en modo Round-Robin uniforme entre todos los slots del operador siguiente (remedio contra desbalances de datos o *data skew*, pero fuerza serialización y transferencia por red a través de Netty).
- **`rescale()`:** Round-Robin restringido únicamente a los slots locales dentro del mismo TaskManager o nodos emparejados (minimiza saltos de red entre nodos EC2/Pods, reduciendo overhead).
- **`broadcast()`:** **Clona y replica cada evento a la TOTALIDAD de los slots del clúster.** Utilizado para distribuir configuraciones dinámicas, catálogos maestros o reglas de negocio que deben residir en la memoria local de cada worker.
- **`forward()`:** Entrega directa en memoria dentro del mismo hilo del slot (cero serialización, cero red, exige paralelismo 1 a 1 entre operador emisor y receptor).

##### 4. ¿Por qué Broadcast State y NO Nodos Stateless ni otras Aristas de Red? (Decisiones de Diseño)

En el procesamiento de transacciones financieras y reglas de negocio dinámicas, surgen dos preguntas arquitecturales fundamentales:

###### A. ¿Por qué NO resolver las reglas en nodos Stateless (`map`, `filter`, `flatMap`)?

| Primitiva Stateless | Cardinalidad | Limitación Fatal para el Escenario de Negocio |
|---|---|---|
| **`map()`** | 1 a 1 | Procesa cada evento en aislamiento sin memoria entre eventos anteriores o posteriores. No puede recordar si una moneda ya llegó ni almacenar una regla mutable sin recurrir a variables estáticas volátiles o llamadas bloqueantes a Redis. |
| **`filter()`** | 1 a 0/1 | Solo puede evaluar condiciones booleanas estáticas. No tiene acceso a `TimerService` ni a `SideOutput` para desviar transacciones huérfanas ante expiración de tiempo. |
| **`flatMap()`** | 1 a N | Aunque puede emitir cero o múltiples eventos, carece de estado gestionado (`Managed State`) respaldado por RocksDB y checkpoints en S3. Ante un reinicio del Pod, cualquier regla en memoria se pierde irremediablemente. |

**Conclusión Arquitectural:** Los nodos Stateless son exclusivos para **higiene de datos** (parseo JSON, normalización de campos, descarte de nulos). Si el requerimiento exige correlacionar dos eventos asíncronos (movimiento + moneda) o evaluar una regla que cambia en caliente sin recompilar el JAR, **el uso de Nodos Stateful (`KeyedProcessFunction`, `BroadcastProcessFunction`) es mandatorio.**

###### B. Matriz Comparativa: `broadcast()` frente a las demás Aristas de Transporte

¿Por qué usar `broadcast()` para las reglas y no `keyBy()`, `rebalance()`, `rescale()` o `forward()`?

| Arista de Red | Semántica de Enrutamiento | ¿Qué pasaría si la usamos para Reglas Dinámicas? | Veredicto Arquitectural |
|---|---|---|---|
| **`broadcast()`** | **1 a TODOS los Slots (Replicación Total)** | Cada worker mantiene una copia completa y actualizada de la regla en su RAM local (`ReadOnlyBroadcastState`). Cualquier transacción que llegue a cualquier slot se valida en nanosegundos en memoria. | **ELEGIDA (Patrón Correcto)**: Garantiza consistencia global y elimina llamadas de red a Redis. |
| **`keyBy(ruleId)`** | 1 a 1 Slot (por Hash de Clave) | La regla `RULE_LIMIT` se guardaría únicamente en el Slot donde cayó su hash. Cuando el Slot 2 procese una transacción, no sabrá que la regla existe porque vive en la memoria de otro TaskManager. | **RECHAZADA**: Provocaría inconsistencia total; los slots no tendrían las reglas asignadas a otros slots. |
| **`rebalance()`** | 1 a 1 Slot (Round-Robin Uniforme) | Si inyectas 3 reglas consecutivas, la Regla 1 va al Slot 1, la Regla 2 al Slot 2 y la Regla 3 al Slot 3. Cada slot tendría solo un tercio de las reglas del negocio. | **RECHAZADA**: Transacciones idénticas serían aceptadas o rechazadas según el slot aleatorio en el que caigan. |
| **`rescale()`** | 1 a Subconjunto de Slots Locales | Similar a `rebalance()`, pero restringido al TaskManager local. Las reglas no llegarían a los otros Pods del clúster. | **RECHAZADA**: Mismo defecto de fragmentación de reglas que `rebalance`. |
| **`forward()`** | 1 a 1 Slot (Mismo Hilo de CPU) | Solo conecta operadores adyacentes con idéntico paralelismo. No puede conectar un stream de configuración desacoplado hacia múltiples hilos paralelos de transacciones. | **RECHAZADA**: Físicamente incapaz de propagar reglas a través de un clúster distribuido. |

##### 5. ¿El Orden de los Operadores es Estricto?

Flink impone **dos reglas estrictas de compilación** y otorga **total libertad de diseño** en el resto:

1. **Regla de Fronteras (Obligatoria):**
   - Todo flujo **DEBE comenzar con un Source** y **DEBE finalizar en al menos un Sink** (o emisión lateral). No pueden existir nodos huérfanos sin destino.
2. **Regla de Dependencia de Clave (Obligatoria):**
   - Cualquier operador que requiera estado por clave (como `KeyedProcessFunction`, `ValueState` o `window()`) **EXIGE un `keyBy()` inmediatamente antes**. Intentar invocar un `window()` o acceder a `getRuntimeContext().getState()` sobre un stream sin clave generará un error fatal en tiempo de compilación/inicio:
     ```text
     UnsupportedOperationException: The state is only accessible from a KeyedStream.
     ```
3. **Libertad de Composición (Flexible):**
   - Puedes encadenar múltiples `filter()` y `map()` antes o después de un `keyBy()`.
   - Puedes tener **múltiples Sources** que se unen mediante `.union()` o `.connect()`.
   - Puedes tener **múltiples Sinks** conectados a diferentes etapas del DAG (ej. persistir métricas agregadas en DynamoDB y a la vez emitir eventos crudos a S3).


---

### 2.3 Semántica de Tiempo y Watermarks

En redes de producción reales, los eventos no llegan en orden cronológico estricto debido a jitter en redes móviles, reintentos y desincronización de relojes. Flink clasifica tres dominios de tiempo:

```text
Evento Generado                    Evento Recibido                    Evento Procesado
    [10:00:01]                        [10:00:05]                         [10:00:06]
        │                                  │                                  │
        ▼                                  ▼                                  ▼
   Event Time                       Ingestion Time                     Processing Time
(Dentro del payload)             (Timestamp Broker Kafka)             (Reloj del TaskManager)
```

1. **Event Time:** La marca de tiempo capturada en el momento en que ocurrió el evento en el dispositivo/cliente de origen. **Es el único tiempo admisible para garantizar exactitud de negocio, ventanas temporales y auditabilidad.**
2. **Ingestion Time:** La marca de tiempo en la que el broker de Kafka añade el registro a su commit log.
3. **Processing Time:** La hora del reloj del sistema local en el hilo del TaskManager que ejecuta la transformación. Produce resultados no deterministas ante repeticiones de flujos.

#### Watermarks
Un **Watermark** es un registro de control especial que fluye intercalado en el stream e indica: *"El pipeline ha observado todos los eventos con timestamp hasta $T$. Cualquier evento futuro con timestamp $t \le T$ se considera demorado (late event)."*

```text
Stream de Entrada                    Operador de Ventana (Tumbling 1 min)
        │                                             │
        ├── Event (ts: 10:00:05) ────────────────────►│ Registrado en acumulador
        ├── Event (ts: 10:00:15) ────────────────────►│ Registrado en acumulador
        ├── Event (ts: 10:00:08) [Desordenado] ──────►│ Aceptado (10:00:08 > Watermark actual)
        ├── Watermark (ts: 10:00:10) ────────────────►│ ¡Dispara cálculo de ventana [10:00:00 - 10:00:10]!
        │                                             │
        └── Event (ts: 10:00:02) [EVENTO DEMORADO] ──►│ Rechazado (ts <= 10:00:10)
                                                      │  └──> Redirigido a Side Output (DLQ)
```

- **Estrategia Monótona vs Bounded-out-of-orderness:** En producción se configuran con tolerancia acotada (ej. `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))`).
- **Manejo de Datos Demorados:** Los eventos que llegan después del watermark pueden descartarse, forzar recálculos de ventana, o canalizarse mediante **Side Outputs** a una Dead-Letter Queue (DLQ) para auditoría.

---

### 2.4 Gestión de Estado y State Backends

El procesamiento con estado permite operaciones complejas como deduplicación, agregaciones continuas y detección de patrones a través de múltiples eventos.

```text
                        ┌───────────────────────────────────────────┐
                        │              Managed State                │
                        └─────────────────────┬─────────────────────┘
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
             ┌─────────────────┐                             ┌─────────────────┐
             │   Keyed State   │                             │ Operator State  │
             │ (Ligado a keyBy)│                             │ (Broadcast/Src) │
             └────────┬────────┘                             └─────────────────┘
                      │
     ┌────────────────┼────────────────┬────────────────┐
     ▼                ▼                ▼                ▼
ValueState<T>    ListState<T>     MapState<K, V>  ReducingState<T>
```

#### Comparativa de State Backends: HashMap vs EmbeddedRocksDB

| Característica | `HashMapStateBackend` | `EmbeddedRocksDBStateBackend` |
|---|---|---|
| **Almacenamiento Primario** | JVM Heap (Objetos Java en memoria). | Off-heap RocksDB (proceso C++ en disco local/NVMe). |
| **Latencia de Acceso** | Submicrosegundo (referencia directa a memoria). | Microsegundos (deserialización desde archivos SST). |
| **Límite de Capacidad** | Limitado por el tamaño del JVM Heap (~decenas de GB). | Delimitado por la capacidad del disco local (Terabytes de estado). |
| **Impacto en Garbage Collector** | Alto uso de Heap puede generar pausas Stop-The-World prolongadas. | Mínimo; el estado reside fuera del Heap en cache de bloques y disco. |
| **Velocidad de Checkpoint** | Copia completa del estado en cada snapshot. | **Checkpoints Incrementales** (solo transfiere archivos SST modificados a S3). |
| **Ajuste Productivo** | Trabajos con estado pequeño (<10 GB), filtros simples y SLAs submilisegundo. | **Estándar institucional recomendado** para gran volumen de estado, deduplicación y ventanas. |

---

### 2.5 Tolerancia a Fallos: Asynchronous Barrier Snapshotting (ABS)

Flink implementa una variante del algoritmo distribuido de **Chandy-Lamport**, conocida como Asynchronous Barrier Snapshotting (ABS):

```text
Paso  Origen        Destino        Acción
────  ────────────  ─────────────  ─────────────────────────────────────────────────────────────
 1    JobManager    Source Task    Inyecta Checkpoint Barrier N en el flujo de Kafka.
 2    Source Task   Source Task    Pausa consumo momentáneamente y captura los offsets de Kafka.
 3    Source Task   AWS S3         Sube de forma asíncrona el snapshot de los offsets.
 4    Source Task   ProcessTask    Propaga la Barrier N hacia adelante junto con los registros.
 5    ProcessTask   ProcessTask    Alinea las barreras de todos los canales de entrada.
 6    ProcessTask   ProcessTask    Ejecuta flush de memtable en RocksDB (KeyedState).
 7    ProcessTask   AWS S3         Transfiere de forma asíncrona los archivos SST modificados.
 8    ProcessTask   JobManager     Confirma: "Snapshot de tarea N completado con éxito".
 9    JobManager    JobManager     Registra el Checkpoint N como CONFIRMADO en los metadatos.
```

- **Ejecución No Bloqueante:** El procesamiento continúa procesando eventos en paralelo mientras los snapshots se transfieren de forma asíncrona hacia S3.
- **Alineación de Barreras vs Checkpoints No Alineados (Unaligned):**
  - *Aligned Checkpoints:* Los operadores esperan la barrera en todos los canales de entrada antes de generar el snapshot. Garantiza tamaño óptimo de checkpoint, pero backpressure severa puede causar timeouts.
  - *Unaligned Checkpoints:* Las barreras adelantan a los buffers de red en tránsito y guardan los datos de los buffers directamente en almacenamiento persistente. Evita fallos de checkpoint durante congestión a expensas de mayor volumen en S3.

---

### 2.6 Checkpoints vs Savepoints

| Dimensión | Checkpoint | Savepoint |
|---|---|---|
| **Iniciador** | Mecanismo automático del runtime (temporizador del JobManager). | Invocación manual por un operador vía CLI o Kubernetes Operator. |
| **Propósito Principal** | Recuperación ante fallos inesperados (resiliencia ante caídas). | Gestión del ciclo de vida operativo (upgrades, migraciones, re-sharding). |
| **Formato de Almacenamiento** | Específico del motor, optimizado para transferencias incrementales (archivos SST). | Formato canónico estándar, agnóstico y compatible entre versiones de Flink. |
| **Ciclo de Vida** | Vinculado al ciclo de vida del job; se descarta al cancelar (salvo configuración retain). | Artefacto permanente; persiste indefinidamente hasta ser eliminado explícitamente. |
| **Sobrecarga de Rendimiento** | Mínima; incremental y asíncrono. | Mayor; serialización completa de todo el estado a almacenamiento durable. |

---

## 3. Integración Kafka & Flink: El Camino a Exactly-Once (EOS)

### 3.1 La Falacia de "Kafka por sí solo garantiza Exactly-Once"

Existe la creencia errónea de que habilitar `enable.idempotence=true` y configurar `transactional.id` en un Producer de Kafka garantiza procesamiento Exactly-Once de punta a punta en toda la empresa.

**Realidad:** Las transacciones de Kafka solo aseguran atomicidad cuando se transfieren mensajes de un topic de Kafka a otro topic de Kafka dentro del mismo clúster.
En cuanto una arquitectura interactúa con estado local externo (RocksDB) o almacenes persistentes downstream (DynamoDB, S3, Pinot), Kafka por sí solo es incapaz de mantener la consistencia. Lograr End-to-End Exactly-Once exige coordinar la tolerancia a fallos del motor (ver [Sección 2.5: Asynchronous Barrier Snapshotting](#25-tolerancia-a-fallos-asynchronous-barrier-snapshotting-abs)) con un **protocolo de confirmación distribuido** en el Sink (ver [Sección 3.3: KafkaSink y 2PC](#33-kafkasink-y-el-protocolo-two-phase-commit-2pc)).

---

### 3.2 KafkaSource: Gestión de Offsets y Partition Discovery

El `KafkaSource` moderno de Flink (arquitectura FLIP-27) administra las particiones de forma dinámica:

```java
KafkaSource<TransactionEvent> source = KafkaSource.<TransactionEvent>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("transactions.incoming")
    .setGroupId("flink-transaction-validator")
    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
    .setValueOnlyDeserializer(new TransactionJsonDeserializationSchema())
    .setProperty("partition.discovery.interval.ms", "30000") // Detección dinámica de particiones
    .build();
```

#### Asignación y Enrutamiento de Particiones a Slots Replicados

¿Cómo sabe cada TaskManager y cada slot qué partición de Kafka debe procesar sin duplicar lecturas ni competir entre Pods?
Flink desacopla el descubrimiento de particiones de su lectura física mediante el patrón **SplitEnumerator / SourceReader** (estándar FLIP-27):

```text
┌────────────────────────────────────────────────────────────────────────┐
│                   JobMaster (Master - Control Plane)                   │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ SplitEnumerator: Detecta particiones de Kafka [P0, P1, ..., P11] │  │
│  │ y mapea deterministamente: 1 Partición ──► 1 Slot específico    │  │
│  └──────┬────────────────────────────┬────────────────────────────┬─┘  │
└─────────┼────────────────────────────┼────────────────────────────┼────┘
          │ Asigna P0..P3              │ Asigna P4..P7              │ Asigna P8..P11
          ▼                            ▼                            ▼
┌──────────────────┐         ┌──────────────────┐         ┌──────────────────┐
│ Pod 1 (TM 1)     │         │ Pod 2 (TM 2)     │         │ Pod 3 (TM 3)     │
│  Slot 1 ──► P0   │         │  Slot 1 ──► P4   │         │  Slot 1 ──► P8   │
│  Slot 2 ──► P1   │         │  Slot 2 ──► P5   │         │  Slot 2 ──► P9   │
│  Slot 3 ──► P2   │         │  Slot 3 ──► P6   │         │  Slot 3 ──► P10  │
│  Slot 4 ──► P3   │         │  Slot 4 ──► P7   │         │  Slot 4 ──► P11  │
└──────────────────┘         └──────────────────┘         └──────────────────┘
```

1. **SplitEnumerator (Centralizado en el JobMaster):** Consulta los metadatos de Kafka para listar las particiones activas (`partition.discovery.interval.ms`).
2. **Asignación Balanceada:** El enumerador evalúa los slots registrados en el clúster y entrega a cada slot un conjunto disjunto de particiones. Ninguna partición se asigna a dos slots simultáneamente.
3. **SourceReader (Ejecutado en cada Slot del TaskManager):** Cada slot ejecuta un lector independiente que abre un socket TCP contra el broker de Kafka para consumir exclusivamente las particiones que le fueron asignadas.
4. **Resiliencia ante Fallos:** Si un Pod colapsa, el JobMaster detecta la desconexión del TaskManager, reasigna las particiones huérfanas a los slots supervivientes o a un nuevo Pod desplegado por Kubernetes, y rebobina los offsets al último Checkpoint confirmado en S3.

#### Semántica de Confirmación de Offsets
- En Flink, **los offsets de Kafka se almacenan dentro de los Checkpoints en S3**, NO en el topic interno `__consumer_offsets` de Kafka.
- Durante la recuperación tras un fallo, Flink descarta los offsets del consumer group de Kafka y rebobina las particiones a los offsets registrados en el último checkpoint exitoso.
- Confirmar offsets hacia Kafka (mediante `setCommitOffsetsOnCheckpoints(true)`) es una cortesía operativa para que herramientas de observabilidad externas (como AKHQ, Burrow o Prometheus) puedan graficar el *consumer lag*.

---

### 3.3 KafkaSink y el Protocolo Two-Phase Commit (2PC)

Para emitir datos a un topic de salida o almacenamiento externo con garantía Exactly-Once, Flink implementa el protocolo **Two-Phase Commit (2PC)** sincronizado con su mecanismo de checkpoints:

```text
FASE 1: PRE-COMMIT
────────────────────────────────────────────────────────────────────────────────────────────────
 1. TaskManager (Sink) ─── Inicia Transacción Kafka (TID-1) ──────────► Broker Kafka
 2. TaskManager (Sink) ─── Escribe registros bajo TID-1 ──────────────► Broker Kafka (no visibles)
 3. JobManager         ─── Llega Checkpoint Barrier N ────────────────► TaskManager (Sink)
 4. TaskManager (Sink) ─── Flush de datos en tránsito y Pre-Commit ───► Broker Kafka
 5. TaskManager (Sink) ─── Confirma Pre-Commit N exitoso ─────────────► JobManager

FASE 2: COMMIT
────────────────────────────────────────────────────────────────────────────────────────────────
 6. JobManager         ─── Todos los operadores confirmaron ──────────► JobManager (Estado OK)
 7. JobManager         ─── Notifica Checkpoint N completado ──────────► TaskManager (Sink)
 8. TaskManager (Sink) ─── Commit Transacción (TID-1) ────────────────► Broker Kafka
 9. Broker Kafka       ─── Escribe marcador COMMIT ───────────────────► ¡Visible para read_committed!
```

#### Nivel de Aislamiento del Consumidor (Isolation Level)
Los consumidores downstream que leen el topic resultante **DEBEN** configurar:
```properties
isolation.level = read_committed
```
- Si un consumidor opera con `read_uncommitted` (el valor por defecto en Kafka), observará transacciones no confirmadas de forma prematura, violando el contrato Exactly-Once.
- Con `read_committed`, el broker retiene los mensajes en su búfer interno hasta que Flink escribe el marcador `COMMIT`.

---

### 3.4 Trade-off: Intervalo de Checkpoint vs Latencia End-to-End

Dado que los consumidores bajo `read_committed` solo pueden observar los eventos tras completarse la confirmación del checkpoint en Flink (ver protocolo 2PC en la [Sección 3.3](#33-kafkasink-y-el-protocolo-two-phase-commit-2pc)), **el intervalo de checkpoint determina directamente la latencia visible downstream**:

```text
Latencia Visible Downstream ≈ Intervalo de Checkpoint + Duración del Checkpoint
```

| Intervalo de Checkpoint | Throughput del Sistema | Latencia Downstream | Sobrecarga de Metadata en S3 | Costo de Recuperación ante Fallos |
|---|---|---|---|---|
| **500 ms – 1 seg** | Menor (frecuentes flushes y sincronizaciones 2PC) | Muy Baja (subsegundo) | Muy Alta (millones de PUTs diarios en S3) | Ventana mínima de reemisión de eventos |
| **10 seg – 30 seg** | Equilibrado | Moderada (10–30s) | Moderada | Reemisión de 10 a 30 segundos de Kafka |
| **1 min – 2 min** *(Estándar de Uber)* | **Máximo** (óptimo batching y compresión) | Alta (1–2 minutos) | Muy Baja (económica y estable) | Reemite hasta 2 minutos ante caída |

---

### 3.5 Patrón de Ingesta CDC: Debezium -> Kafka -> Apache Flink

Cuando el origen de los datos es una base de datos relacional (PostgreSQL, MySQL) capturada a través de **Debezium**, la arquitectura introduce restricciones críticas de sistemas distribuidos:

```text
┌───────────────────────┐
│ Relational Database   │
│ (PostgreSQL / MySQL)  │
└──────────┬────────────┘
           │ Write-Ahead Log (WAL / Binlog)
           ▼
┌───────────────────────┐
│   Debezium Connector  │  (Cluster dedicado de Kafka Connect)
│ (Key: DB Primary Key) │
└──────────┬────────────┘
           │ CDC Envelope (op: c, u, d, r + source metadata)
           ▼
┌───────────────────────┐
│     Apache Kafka      │
│   (Compacted Topic)   │
└──────────┬────────────┘
           │ Flink KafkaSource
           ▼
┌───────────────────────┐
│     Apache Flink      │
│ (KeyedProcessFunction)│  Filtro monotónico de LSN en KeyedState
└──────────┬────────────┘
           │ Upsert Idempotente (UUID)
           ▼
┌───────────────────────┐
│ Target Data Lakehouse │
│  (DynamoDB / Pinot)   │
└───────────────────────┘
```

#### Consideraciones Críticas de Arquitectura en CDC:

> [!IMPORTANT]
> **La Ingesta CDC es At-Least-Once por Naturaleza:**  
> Debezium lee el registro de transacciones de la base de datos (PostgreSQL WAL o MySQL binlog). Ante reinicios del conector, cortes de red o rebalances en Kafka Connect, Debezium rebobina sus offsets al último punto confirmado. En consecuencia, **Kafka RECIBIRÁ eventos CDC duplicados y retransmitidos**. Flink no puede asumir un flujo libre de duplicados; los operadores con estado DEBEN aplicar reconciliación monotónica e idempotencia.

1. **Particionado por Primary Key (Garantía de Causalidad):**
   - Como se estableció en la taxonomía de la [Sección 2.1 (Punto 1)](#21-taxonomía-y-mapeo-de-conceptos-debezium-kafka-flink-y-kubernetes), Kafka únicamente garantiza el orden de los mensajes *dentro de una misma partición*.
   - Por tanto, Debezium **DEBE** usar la Primary Key de la tabla fuente (ej. `account_id` o `order_id`) como clave de particionamiento. Repartir mutaciones de una misma fila entre particiones distintas (vía round-robin) provocará que un `UPDATE` o `DELETE` se procese antes que su respectivo `INSERT`.

2. **Deduplicación y Orden Monotónico mediante LSN:**
   - Aprovechando el LSN capturado en el envelope de Debezium (ver definición en [Sección 2.1, Punto 3](#21-taxonomía-y-mapeo-de-conceptos-debezium-kafka-flink-y-kubernetes)), `KeyedProcessFunction` almacena el LSN más alto observado en un `ValueState<Long>` respaldado por RocksDB (ver [Sección 2.4](#24-gestión-de-estado-y-state-backends)).
   - Cualquier evento entrante con $LSN \le LSN_{almacenado}$ se identifica como un duplicado rezagado y se descarta:

```java
public class DebeziumCdcOrderFilterFunction 
    extends KeyedProcessFunction<String, CdcPayload, CleanTransactionEvent> {

    // Keyed State: almacena el LSN más alto observado por cada entidad
    private transient ValueState<Long> highestLsnState;

    @Override
    public void open(OpenContext ctx) {
        ValueStateDescriptor<Long> descriptor = 
            new ValueStateDescriptor<>("highestLsnState", Long.class);
        highestLsnState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
            CdcPayload cdcEvent, 
            Context ctx, 
            Collector<CleanTransactionEvent> out) throws Exception {

        long incomingLsn = cdcEvent.getSource().getLsn();
        Long lastSeenLsn = highestLsnState.value();

        // Control de obsolescencia: descartar o auditar registros retransmitidos por Debezium
        if (lastSeenLsn != null && incomingLsn <= lastSeenLsn) {
            // Duplicado detectado: desviar a Side Output de auditoría
            ctx.output(STALE_CDC_SIDE_OUTPUT, cdcEvent);
            return;
        }

        // Avanzar la marca de LSN en el estado
        highestLsnState.update(incomingLsn);

        // Procesar inserción, modificación o eliminación con seguridad
        if ("d".equals(cdcEvent.getOp())) {
            // Manejar eliminación de la fila
            out.collect(CleanTransactionEvent.tombstone(cdcEvent.getBefore()));
        } else {
            // Manejar creación o actualización
            out.collect(CleanTransactionEvent.from(cdcEvent.getAfter()));
        }
    }
}
```

3. **Manejo de Eliminaciones y Tombstones:**
   - Para un `DELETE` en SQL, Debezium genera dos registros consecutivos:
     1. Un registro con `op: "d"` que contiene el estado previo (`before`).
     2. Un **registro tombstone** posterior (`payload: null`, conservando la clave primaria) para permitir que el Log Compaction de Kafka libere espacio en disco.
   - **Requisito en Flink:** El `DeserializationSchema` personalizado debe comprobar explícitamente cuerpos de mensaje `null` antes de deserializar para evitar caídas catastróficas por `NullPointerException` en los TaskManagers.

4. **Evolución de Esquemas y Registro Centralizado:**
   - Cuando migraciones DDL modifican la estructura de las tablas (ej. `ALTER TABLE ADD COLUMN`), Debezium transmite la nueva definición.
   - Es obligatorio emplear **Avro + Schema Registry** (Confluent o AWS Glue) en lugar de JSON plano en producción para asegurar compatibilidad hacia adelante y hacia atrás, evitando que cambios en la base de datos interrumpan los pipelines de Flink.

5. **Segregación de Infraestructura (Kafka Connect):**
   - Debezium opera dentro de un clúster de **Kafka Connect**.
   - **Nunca ubicar Kafka Connect junto a los TaskManagers de Flink.** Despliega Kafka Connect en su propio clúster dedicado (ej. Strimzi `KafkaConnect` en Kubernetes o AWS MSK Connect) para evitar contención de CPU y memoria heap entre los conectores y las tareas de streaming.

---

### 3.6 Patrón Empresarial: Enriquecimiento Multi-Stream Asíncrono, Reglas Dinámicas (Broadcast State) y Persistencia de Estado

En arquitecturas transaccionales y de motores de incentivos (como procesamiento de movimientos y puntos), los pipelines enfrentan dos retos críticos de diseño:
1. **Dispersión de Datos en Tablas Desacopladas:** La transacción de negocio (ej. movimiento o débito de saldo) viaja en un topic de Kafka, pero datos mandatorios para su procesamiento (como la moneda de la cuenta o el balance) residen en otra tabla capturada en un topic independiente. Debido a la naturaleza asíncrona de los brokers, el orden de llegada entre streams es impredecible.
2. **Reglas de Configuración Dinámicas:** Ciertas condiciones de filtrado y activación son editadas manualmente por operadores en un microservicio externo (ej. corriendo en AWS Fargate) y persistidas en una caché de Redis. Consultar Redis por red en cada evento destruye el rendimiento del motor de streaming.

#### Topología Integral del Arquetipo de Enriquecimiento

```text
 [Topic: Monedas/Cuentas] ──► (keyBy: account) ──┐
                                                 ▼
 [Topic: Movimientos CDC] ──► (keyBy: account) ──► [KeyedCoProcessFunction]
                                                    (Enriquecimiento con RocksDB + Timers)
                                                         │
                                                         ▼
 [Topic: Configuración]   ──► .broadcast() ──────► [BroadcastProcessFunction]
                                                    (Evaluación de Reglas en Memoria Local)
                                                         │
                                        ┌────────────────┴────────────────┐
                                        │                                 │
                                        ▼                                 ▼
                             [Sink Principal: DynamoDB]      [Side Output: DLQ / Alertas]
```

> [!TIP]
> **Diagrama Editable del Pipeline:** Consulta el modelo visual interactivo completo en [docs/architecture-pipeline.drawio](file:///Users/renzojared/Developer/me/poc-apache-flink/docs/architecture-pipeline.drawio).

#### Dilema 1: Enriquecimiento Asíncrono Stream-to-Stream (`connect()` + `KeyedCoProcessFunction`)

En lugar de recurrir a consultas bloqueantes síncronas contra bases de datos externas, ambos streams se unifican mediante el operador `.connect()` tras particionarse por la clave común de negocio (`account` o `account_id`):

1. **Resolución de Llegada Desfasada mediante RocksDB State:**
   - **Escenario Normal (La Moneda llega PRIMERO):** El operador almacena la definición de la moneda en un `ValueState<Currency>` respaldado por RocksDB. No emite registros aún.
   - **Llegada del Movimiento:** Al recibirse el movimiento, el operador consulta su estado local off-heap: `Currency c = currencyState.value()`. Si la moneda existe, enriquece el movimiento inmediatamente y lo emite hacia la siguiente fase.
   - **Escenario Inverso (El Movimiento llega ANTES que la Moneda):** Si el movimiento arriba antes de que Debezium propague la creación de la moneda, Flink no descarta el registro:
     1. Encola el movimiento en un `ListState<Movement>` gestionado por RocksDB.
     2. Programa un temporizador (*Timer*) de espera (ej. 30 segundos en tiempo de procesamiento o marcas de agua).
     3. Cuando el evento de la moneda llega dentro de la ventana de espera, el operador despierta, drena el `ListState`, inyecta la moneda y emite todos los movimientos pendientes en orden causal.
     4. **Tolerancia a Inconsistencias (Side Output):** Si el temporizador expira sin recibir la moneda (`onTimer()`), Flink desvía los movimientos pendientes hacia una **Side Output** (Dead-Letter Queue en Kafka) para conciliación contable sin bloquear el resto del pipeline.

2. **Independencia de Particionado en Kafka vs Data Shuffle de Flink (Cero Co-Partitioning):**
   - **El Mito del Co-Partitioning (Kafka Streams vs Flink):** En frameworks como *Kafka Streams*, unir dos flujos exige obligatoriamente *Co-partitioning* (ambos topics de Kafka deben tener exactamente el mismo número de particiones y estar particionados con la misma clave en el broker).
   - **La Ventaja de Flink:** Flink desacopla la lectura del procesamiento. En los `KafkaSource`, cada slot simplemente lee las particiones de Kafka asignadas. Inmediatamente después, el operador `.keyBy(movementId)` ejecuta un **Network Shuffle** a través de buffers Netty:
     ```text
     [Kafka Topic: Movimientos (12 Particiones)] ──► Reader Slots ──┐
                                                                    ├──► [keyBy(movementId)] ──► Mismo Task Slot (RocksDB)
     [Kafka Topic: Monedas     (2 Particiones)]  ──► Reader Slots ──┘    (Data Shuffle Netty)
     ```
   - **Resultado:** `movements.incoming` puede tener 30 particiones para soportar alto throughput y `movements.currencies` puede tener solo 2 particiones. La función de hash de Flink (`hash(movementId) % maxParallelism`) garantiza que tanto la moneda como el movimiento viajen al **mismo Task Slot físico y compartan el mismo estado en RocksDB**.

#### Dilema 2: Reglas Dinámicas sin Cuellos de Botella (Broadcast State Pattern vs Antipatrón de Redis)

> [!WARNING]
> **El Antipatrón de Consultas Síncronas a Redis en Streaming:**  
> Realizar una consulta por red (TCP/HTTP) a Redis por cada evento que atraviesa Flink convierte a la base de datos externa en un cuello de botella letal. La latencia de red (1–3 ms por round-trip) degrada el throughput del clúster de 50,000 eventos/s a menos de 1,000 eventos/s y genera riesgo de saturación y caídas en Redis.

##### La Solución Arquitectónica: Broadcast State Pattern (`broadcast()`)
1. **Flujo de Publicación de Reglas:** Cuando un operador edita o complementa la configuración en el microservicio en Fargate, el servicio persiste la regla en Redis (para consultas de portales web) y **simultáneamente publica un evento inmutable con la nueva configuración en un topic de Kafka** (`company.rules.config`).
2. **Replicación en Memoria (*BroadcastStream*):** Flink consume el topic de reglas y aplica `.broadcast(ruleStateDescriptor)`. El motor replica la regla directamente en el estado de memoria local de **todos los TaskManagers del clúster**.
3. **Evaluación Ultrarrápida sin Red:** Cuando fluyen las transacciones, la función `BroadcastProcessFunction` valida el movimiento contra la regla **consultando su memoria local en nanosegundos**, sin emitir una sola petición por red a Redis.
4. **Actualizaciones en Caliente (Hot Reloading):** Si la configuración cambia 2 horas después, un nuevo evento en Kafka actualiza el Broadcast State en todos los TaskManagers instantáneamente, sin necesidad de reiniciar el pipeline ni interrumpir el procesamiento.

#### Garantías de Persistencia de Estado: Checkpoints y Savepoints

> [!IMPORTANT]
> **¿Checkpoints y Savepoints Persisten Toda Esta Información en Memoria?**  
> **SÍ, AL 100%.** Flink garantiza que ningún estado en memoria se pierde ante fallos o nuevos despliegues.

Durante la ejecución del algoritmo Asynchronous Barrier Snapshotting (ABS, ver [Sección 2.5](#25-tolerancia-a-fallos-asynchronous-barrier-snapshotting-abs)), Flink captura un snapshot consistente y transaccional de todos los tipos de estado presentes en el pipeline:

| Tipo de Estado | Estructura en el Pipeline | Mecanismo de Persistencia en Checkpoint / S3 | Comportamiento tras Fallo o Despliegue con Savepoint |
|---|---|---|---|
| **`KeyedState`** | `ValueState<Currency>` (Moneda cacheada) | Snapshot incremental de SST files de RocksDB transferidos a AWS S3. | Se restaura en NVMe local; la moneda sigue disponible de inmediato. |
| **`KeyedState`** | `ListState<Movement>` (Movimientos en espera) | Serializado incrementalmente a S3 dentro de RocksDB. | Los movimientos encolados reanudan su espera sin perder un solo registro. |
| **`Timers`** | Temporizadores activos de vencimiento (`TimerService`) | Serializados y versionados junto con el estado del operador en S3. | Flink reprograma los temporizadores pendientes con sus marcas de tiempo exactas. |
| **`BroadcastState`** | Mapa de Reglas de Configuración activas | Persistido en S3 como parte del *Operator State* en el metadata del checkpoint. | Todos los TaskManagers restauran la regla exacta que regía al momento del snapshot. |

##### Resiliencia ante Despliegues y Caídas:
- **Ante caídas imprevistas (Pod OOM / Fallo de EC2):** El JobManager detecta la desconexión del TaskManager, Kubernetes levanta un nuevo Pod, Flink descarga el último checkpoint exitoso de S3, restaura las monedas, los buffers de movimientos y las reglas, y rebobina los offsets de Kafka.
- **Ante actualizaciones de código (Upgrade con Savepoint):** El operador de Kubernetes drena el pipeline con `upgradeMode: savepoint` (ver [Sección 5.6](#56-estrategia-de-despliegue-en-producción-zero-data-loss-para-misión-crítica)), congelando el estado exacto en S3. La nueva versión del JAR arranca desde dicho Savepoint, reanudando la evaluación sin duplicados ni pérdida de eventos en espera.

---

## 4. Caso de Estudio: Procesamiento de Eventos de Ads en Uber Engineering

*Referencia: "Real-Time Exactly-Once Ad Event Processing with Apache Flink, Kafka, and Pinot" (Uber Engineering).*

### 4.1 Problema de Negocio y Desafíos Técnicos
Uber gestiona una plataforma de publicidad multillonaria integrada en Uber Eats y Mobility. Los anunciantes pagan por clic o impresión, y los usuarios reciben promociones basadas en estas interacciones.
- **Riesgo Financiero:** Omitir una impresión causa subfacturación (pérdida directa de ingresos); procesar dos veces un clic genera sobrefacturación a anunciantes (daño reputacional y contingencias legales).
- **Escala:** Cientos de miles de interacciones por segundo distribuidas en múltiples regiones geográficas globales.
- **Sesgo de Datos y Llegadas Tardías:** Dispositivos móviles pierden cobertura en túneles o transporte subterráneo, transmitiendo ráfagas de clics con horas de retraso.

---

### 4.2 Blueprint de Arquitectura: Kafka + Flink + Docstore + Pinot

```text
[Capa de Ingesta]
  Mobile Clients / Edge Gateway
    │ Impresiones y Clics (At-Least-Once)
    ▼
  Topics de Ingesta Apache Kafka

[Capa de Procesamiento en Streaming: Apache Flink]
  Kafka Ingest Topics
    │ Consumo distribuido
    ▼
  Topología Publicitaria en Apache Flink
    ├── Cache Lookups (TTL) ◄──► Uber Docstore (Almacén Clave-Valor)
    ├── Checkpoints (2 min) ────► Almacenamiento Durable (HDFS / Cloud S3)
    └── 2PC Exactly-Once
          │
          ▼
[Capa OLAP en Tiempo Real: Apache Pinot]
  Topics de Salida Kafka (read_committed)
    │
    ▼
  Clúster Apache Pinot (Tablas Real-Time con Upsert por UUID)
    ├── Dashboards de Anunciantes en Tiempo Real
    └── Pipeline Automatizado de Facturación

[Capa Histórica / Batch]
  Flink Dump ────► Data Lake Apache Hive (Auditoría y Reconciliación Fría)
```

- **Sincronización y Durabilidad:** La topología adopta checkpoints cada 2 minutos para maximizar el throughput global (ver análisis de trade-offs en la [Sección 3.4](#34-trade-off-intervalo-de-checkpoint-vs-latencia-end-to-end)) y emite hacia topics de salida mediante el protocolo 2PC (ver [Sección 3.3](#33-kafkasink-y-el-protocolo-two-phase-commit-2pc)).

---

### 4.3 Patrones de Arquitectura a Adoptar

#### Patrón A: UUIDs de Registro Deterministas y Downstream Upsert
En lugar de depender exclusivamente del estado interno de Flink para neutralizar duplicados, Flink calcula un **UUID determinista** para cada evento de negocio:

```text
UUID_Registro = SHA256(ad_id + user_id + tipo_interacción + ventana_tiempo)
```

- Cuando Flink escribe hacia almacenes OLAP (como Apache Pinot) o bases de datos NoSQL (como AWS DynamoDB), la tabla destino utiliza este `UUID de Registro` como su clave primaria.
- **Resultado:** Si un worker de Flink falla y reemite eventos desde un checkpoint anterior, el sink downstream realiza una operación de **upsert idempotente** en lugar de insertar un registro duplicado.
- **Complementariedad:** Este patrón complementa la deduplicación por LSN en origen (ver [Sección 3.5](#35-patrón-de-ingesta-cdc-debezium---kafka---apache-flink)) y queda formalizado en el [ADR-003](#adr-003-claves-de-idempotencia-deterministas-para-sinks-downstream).

#### Patrón B: Deduplicación Temprana en el Stream con KeyedState y TTL
Para evitar que registros redundantes consuman recursos a lo largo de todo el DAG, un operador de filtrado temprano particiona el flujo por la huella digital del evento mediante `KeyedState` con TTL respaldado por RocksDB (ver [Sección 2.4](#24-gestión-de-estado-y-state-backends)):

```java
public class DeduplicationFilterFunction extends KeyedProcessFunction<String, AdEvent, AdEvent> {
    private ValueState<Boolean> seenState;

    @Override
    public void open(OpenContext ctx) {
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.hours(24)) // Rechazar duplicados observados en las últimas 24 horas
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("seenState", Boolean.class);
        desc.enableTimeToLive(ttlConfig);
        seenState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(AdEvent event, Context ctx, Collector<AdEvent> out) throws Exception {
        if (seenState.value() == null) {
            seenState.update(true);
            out.collect(event); // Emitir la primera aparición
        } else {
            // Duplicado detectado: enviar a métricas o Side Output de auditoría
        }
    }
}
```

#### Patrón C: Agregación Incremental Eficiente en Memoria
- **Anti-patrón (`ProcessWindowFunction`):** Almacena en búfer *cada registro crudo* en memoria o en RocksDB hasta que la ventana de 1 minuto se cierra. Bajo tráfico elevado, esto satura el Heap y produce checkpoints gigantescos.
- **Patrón de Uber (`AggregateFunction`):** Mantiene un acumulador incremental. La huella de memoria es estrictamente $O(1)$ por clave, independientemente de si la ventana recibe 10 o 10,000,000 de eventos.

```java
// Agregación incremental que mantiene únicamente 1 acumulador en memoria por clave
public class AdMetricsAggregator implements AggregateFunction<AdEvent, AdMetricAccumulator, AdMetricSummary> {
    @Override
    public AdMetricAccumulator createAccumulator() {
        return new AdMetricAccumulator();
    }

    @Override
    public AdMetricAccumulator add(AdEvent value, AdMetricAccumulator acc) {
        acc.incrementImpressions();
        acc.addSpend(value.getBidAmount());
        return acc;
    }

    @Override
    public AdMetricSummary getResult(AdMetricAccumulator acc) {
        return new AdMetricSummary(acc.getCount(), acc.getTotalSpend());
    }

    @Override
    public AdMetricAccumulator merge(AdMetricAccumulator a, AdMetricAccumulator b) {
        a.merge(b);
        return a;
    }
}
```

#### Patrón D: Disaster Recovery Activo-Activo Multi-Región
Uber ejecuta topologías idénticas de Flink en dos regiones geográficas aisladas (Región A y Región B):
1. Las aplicaciones móviles publican eventos en el clúster de Kafka de su región más próxima.
2. Clústeres independientes de Flink procesan los flujos regionales de forma autónoma.
3. Ambos pipelines de Flink envían sus resultados hacia un clúster global unificado de Apache Pinot usando los UUIDs de registro deterministas.
4. **Disaster Recovery:** Si la Región A colapsa, el tráfico se redirige a la Región B sin intervención manual, y la capacidad de upsert de Pinot absorbe cualquier traslape sin generar doble conteo.

---

## 5. Arquitectura de Infraestructura e Ingeniería Cloud (AWS)

### 5.1 Transición Conceptual: De Docker a Kubernetes (AWS EKS)

El paso de un entorno local basado en contenedores hacia una plataforma de producción distribuida exige un cambio de modelo mental fundamental. Un error frecuente en equipos que migran desde Docker es considerar a Kubernetes simplemente como "un Docker Compose más grande". En arquitecturas de procesamiento distribuido de flujos con estado (*stateful stream processing*), la diferencia es estructural.

#### El Salto Arquitectónico: Del Host Aislado a la Malla Distribuida

1. **Docker Compose (Modelo Mono-Host):**
   - En desarrollo local (`poc-apache-flink`), Docker gestiona contenedores en **un único demonio y un único kernel de Linux**.
   - Los límites de CPU y memoria (`cgroups`) compiten dentro de una sola máquina física o virtual.
   - **Punto Único de Fallo Total:** Si el host sufre un kernel panic, pierde energía o agota sus recursos, todo el ecosistema (Kafka, Zookeeper/KRaft, JobManager, TaskManagers, Floci) colapsa simultáneamente. No existe concepto de migración de procesos, balanceo elástico entre servidores ni resiliencia Multi-AZ.

2. **Kubernetes / AWS EKS (Sistema Operativo de Clúster Distribuido):**
   - Kubernetes no es un ejecutor de contenedores aislado; es un **orquestador distribuido declarativo** que abstrae una flota heterogénea de servidores (en AWS: instancias EC2) tratándolas como un único pool elástico de cómputo, memoria y almacenamiento.
   - Desacopla la definición del software de la máquina física donde corre mediante un plano de control que evalúa continuamente el estado deseado frente al estado real (*reconciliation loop*).

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│               ARQUITECTURA DE ORQUESTACIÓN DISTRIBUIDA EN AWS EKS                      │
└────────────────────────────────────────────────────────────────────────────────────────┘

 [1] AWS Managed Control Plane (SLA 99.95%, Multi-AZ Redundante)
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │   etcd (Almacén Distribuido) ◄──► kube-apiserver ◄──► kube-scheduler / controllers   │
 └──────────────────────────────────────────┬───────────────────────────────────────────┘
                                            │ API Declarativa (YAML / CRDs)
                                            ▼
 [2] Operador Especializado de Streaming
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │                      Flink Kubernetes Operator Pod                                    │
 │    (Reconcilia FlinkDeployment CRD: Savepoints automáticos, upgrades coordinados)    │
 └──────────────────────────────────────────┬───────────────────────────────────────────┘
                                            │ Programa Pods y Gestiona Ciclo de Vida
                                            ▼
 [3] AWS EKS Data Plane: Flota de Worker Nodes (Instancias EC2)
 ┌──────────────────────────────────────┐      ┌──────────────────────────────────────┐
 │ EC2 Worker Node A (AZ us-east-1a)    │      │ EC2 Worker Node B (AZ us-east-1b)    │
 │ ┌──────────────────────────────────┐ │      │ ┌──────────────────────────────────┐ │
 │ │   JobManager Pod (Master)        │ │      │ │  TaskManager 2 Pod (Worker)      │ │
 │ │   - Coordina Checkpoints / ABS   │ │      │ │  - Slots de Procesamiento        │ │
 │ └──────────────────────────────────┘ │      │ │  - Memoria Off-Heap RocksDB      │ │
 │ ┌──────────────────────────────────┐ │      │ └──────────────────┬───────────────┘ │
 │ │  TaskManager 1 Pod (Worker)      │ │      │                    │                 │
 │ │  - Slots de Procesamiento        │ │      │                    ▼                 │
 │ └──────────────────┬───────────────┘ │      │      Volume NVMe Local Scratch       │
 │                    │                 │      │      (SST Files / Memtables)         │
 │                    ▼                 │      └──────────────────────────────────────┘
 │      Volume NVMe Local Scratch       │
 │      (SST Files / Memtables)         │
 └────────────────────┬─────────────────┘
                      │
                      │ Checkpoints Incrementales Asíncronos
                      ▼
 ┌──────────────────────────────────────────────────────────────────────────────────────┐
 │                      AWS S3 Bucket (Almacén Durable y Persistente)                   │
 │                     s3://poc-apache-flink-storage-prod/checkpoints                   │
 └──────────────────────────────────────────────────────────────────────────────────────┘
```

#### Abstracciones Fundamentales de Kubernetes para Streaming Distribuido

Para ingenieros con base en Docker, estas son las equivalencias y nuevos conceptos que gobiernan Flink en EKS:

- **Pod (Unidad Atómica de Ejecución):**
  Como se introdujo en la [Sección 2.1 (Punto 2)](#21-taxonomía-y-mapeo-de-conceptos-debezium-kafka-flink-y-kubernetes), en Kubernetes cada TaskManager o JobManager se ejecuta dentro de un Pod dedicado. A diferencia de un contenedor Docker aislado, el Pod agrupa contenedores bajo el mismo espacio de red (`localhost`), IPC y volúmenes montados.
- **Worker Nodes & Managed Node Groups:**
  Las instancias EC2 reales que se unen al clúster de EKS para proveer capacidad de cómputo. En AWS se administran mediante *Managed Node Groups*, que automatizan el aprovisionamiento de Auto Scaling Groups (ASG), actualización de parches de seguridad de AMI y registro en el clúster.
- **Auto-healing y Detección de Fallos:**
  En Docker, si un contenedor muere por `OutOfMemory` (OOM), el host simplemente detiene el proceso o intenta reiniciarlo en la misma máquina agotada. En Kubernetes, el `kubelet` monitorea los pods mediante sondas de vida (*liveness probes*) y preparación (*readiness probes*). Si un nodo EC2 sufre una falla de hardware, Kubernetes reprograma inmediatamente los pods de TaskManager en otra instancia EC2 sana.
- **Flink Kubernetes Operator (El SRE Automatizado de Streaming):**
  Un `Deployment` estándar de Kubernetes no comprende el ciclo de vida de streaming ni la semántica de Savepoints (ver [Sección 2.6](#26-checkpoints-vs-savepoints)). El Operador de Flink actúa como un SRE automatizado: intercepta actualizaciones, drena la ingesta de Kafka, dispara un Savepoint síncrono a S3 y reanuda el job sin pérdida de datos (ver implementación detallada en la [Sección 5.6](#56-estrategia-de-despliegue-en-producción-zero-data-loss-para-misión-crítica)).

#### Matriz Comparativa: Docker Compose vs AWS EKS para Stream Processing

| Dimensión Arquitectónica | Docker Compose (Desarrollo Local) | AWS EKS (Producción Cloud) |
|---|---|---|
| **Límite de Infraestructura** | 1 Máquina / 1 Kernel de SO | Clúster Multi-Host / Multi-AZ elástico |
| **Recuperación ante Caídas** | Reinicio local del contenedor en el mismo host | *Auto-healing* distribuido con reprogramación en otros nodos |
| **Gestión de Recursos** | Límites compartidos en un solo host; contención severa | *Requests* y *Limits* garantizados por cgroups a nivel de Pod |
| **Ciclo de Vida de Streaming** | Detención y arranque destructivo manual | Orquestación declarativa orientada a Savepoints vía Operator |
| **Almacén de Estado RocksDB** | Montaje de directorio local en disco de la laptop | Discos NVMe locales dedicados tipo Scratch montados vía `hostPath` |
| **Resiliencia de Red** | Red tipo puente virtual local (`bridge network`) | AWS VPC CNI nativo (cada Pod recibe una IP real de la VPC) |

---

### 5.2 Evaluación de Plataforma de Cómputo: EKS vs ECS vs EC2 Standalone vs Fargate

```text
               ┌────────────────────────────────────────────────────────────────────────┐
               │              Matriz de Evaluación de Plataformas de Cómputo            │
               └────────────────────────────────────────────────────────────────────────┘
                    │                                        │
     ┌──────────────┴──────────────┐          ┌──────────────┴──────────────┐
     ▼                             ▼          ▼                             ▼
AWS EKS (Operator)              AWS ECS     EC2 Standalone / ASG        AWS Fargate
  [RECOMENDADO]              [ANTI-PATRÓN]       [ALTO TOIL]           [ANTI-PATRÓN]
```

| Criterio de Evaluación | AWS EKS + Flink K8s Operator | AWS ECS (EC2 / Fargate) | EC2 Standalone / ASG | AWS Fargate Puro | Amazon Managed (MSF) |
|---|---|---|---|---|---|
| **Automatización con Operador** | ✅ **Flink K8s Operator** nativo (Savepoints y Upgrades declarativos) | ❌ **Sin soporte de operadores** (ECS no soporta CRDs de K8s) | ❌ Scripts bash manuales, Ansible o SSH | ❌ Sin soporte de operadores | ✅ Administrado por AWS |
| **Ciclo de Vida de Despliegue** | ✅ **Atómico**: Drena ingesta, congela Savepoint a S3 y reanuda | ❌ **Destructivo**: Envía SIGTERM a ciegas sin coordinar Savepoint | ⚠️ Manual y frágil ante fallos en scripts | ❌ Destructivo (SIGTERM ciego) | ⚠️ Opciones fijas de AWS |
| **Almacenamiento Local (RocksDB)** | ✅ **NVMe local scratch** montado vía `hostPath` directo | ❌ Sin NVMe en Fargate; complejo en EC2 | ✅ Acceso directo a NVMe de la VM | ❌ Sin NVMe (estrangula IOPS) | ⚠️ Almacenamiento interno opaco |
| **Recuperación ante Caídas (Auto-healing)** | ✅ **Segundos**: K8s reasigna el Pod en otro nodo sano del clúster | ⚠️ Minutos: Tarea ECS recreada pero sin contexto de streaming | ❌ Minutos: ASG debe crear una VM desde cero | ⚠️ Lento arranque de contenedor | ✅ Recuperación automática |
| **Eficiencia de Costos** | ✅ Alta densidad + TaskManagers en **instancias Spot** (ahorro 70%) | ⚠️ Costoso en Fargate; rígido en EC2 | ❌ Máquinas sobredimensionadas fijas | ❌ Costo elevado vCPU/RAM | ❌ Recargo elevado por KPU/h |

#### Análisis Técnico de Descarte de Alternativas

##### 1. ¿Por qué NO EC2 Standalone? (El Alto Costo del Toil Operativo)
- **Despliegues Manuales Frágiles:** En EC2 puro, actualizar un JAR o modificar paralelismo requiere scripts de automatización (Bash, Ansible, Python) que ejecutan comandos remotos por SSH. Si la red parpadea durante la ejecución de `flink stop`, el clúster queda en estado inconsistente o huérfano.
- **Auto-healing Lento:** Si la instancia EC2 subyacente sufre una falla de hardware en el datacenter de AWS, el proceso Java muere. El Auto Scaling Group (ASG) de AWS tarda entre 3 y 5 minutos en detectar la falla, lanzar una nueva máquina virtual, inicializar el sistema operativo, descargar dependencias y arrancar el TaskManager, prolongando el downtime del pipeline.
- **Sobredimensionamiento Rígido:** Las instancias EC2 se dimensionan para picos de tráfico, manteniendo capacidad ociosa no compartida durante horas valle.

##### 2. ¿Por qué NO AWS ECS (Elastic Container Service)? (El Anti-patrón de Despliegue Destructivo)
Aunque ECS es excelente para microservicios HTTP stateless (solicitud-respuesta), **es un anti-patrón para motores de streaming con estado como Apache Flink**:
- **Ausencia de un Flink Operator:** ECS no dispone de un mecanismo equivalente a los Custom Resource Definitions (CRDs) ni controladores de reconciliación de Kubernetes. No existe un "Flink ECS Operator".
- **El Dilema del SIGTERM y la Pérdida de Estado:** Cuando ECS actualiza una definición de tarea (*Task Definition*), utiliza un rolling deployment tradicional: inicia la nueva tarea y envía una señal `SIGTERM` a la tarea antigua, esperando un tiempo límite (`stopTimeout`, por defecto 30s) antes de enviar `SIGKILL`. ECS **es físicamente incapaz de coordinarse con el JobManager de Flink** para pausar la lectura de Kafka, esperar a que todos los operadores confirmen un Savepoint síncrono a S3 y solo entonces apagar los TaskManagers. En consecuencia, un despliegue en ECS mata los procesos a ciegas, abortando checkpoints en curso y forzando relecturas masivas desde Kafka.
- **Gestión Inflexible de Volúmenes Scratch:** RocksDB requiere acceso de ultra baja latencia a discos locales NVMe. Configurar volúmenes locales efímeros montados en contenedores ECS sobre instancias EC2 carece de la madurez de los montajes de volumen tipo scratch de Kubernetes (`emptyDir` o `hostPath` aislados con cgroups).
- **Desacoplamiento Artificial Master-Worker:** En ECS tendrías que definir dos servicios independientes (JobManager y TaskManager). ECS no entiende que el TaskManager no puede iniciar sin el JobManager, ni puede gestionar el escalado reactivo basado en las necesidades internas del DAG de streaming.

##### 3. ¿Por qué NO AWS Fargate? (Cuello de Botella en Disco)
Como se detalla en la [Sección 5.4](#54-niveles-de-almacenamiento-volúmenes-nvme-scratch-y-almacén-s3), `EmbeddedRocksDBStateBackend` exige cientos de miles de IOPS de lectura y escritura aleatoria para memtables y compactaciones en disco. AWS Fargate provee almacenamiento efímero virtualizado que estrangula los IOPS, provocando caídas de rendimiento catastróficas y timeouts en los checkpoints.

##### 4. Conclusión: AWS EKS + Flink Kubernetes Operator como Estándar Institucional
EKS combina la potencia del cómputo físico (Worker Nodes EC2 con discos NVMe locales) con la inteligencia del **Flink Kubernetes Operator**, que opera como un SRE autónomo:
- Intercepta el manifiesto `FlinkDeployment` y coordina Savepoints antes de cualquier reemplazo de Pods.
- Permite la ejecución de TaskManagers sobre **instancias Spot**, reduciendo los costos de cómputo hasta en un 70% sin riesgo gracias a la recuperación rápida de Pods.
- Ofrece auto-healing a nivel de Pod en menos de 10 segundos ante fallos de procesos JVM.

#### 5.2.1 Anatomía de Infraestructura en EKS: De la Instancia EC2 al Task Slot

Una duda fundamental al diseñar la arquitectura física en la nube es la correlación entre la máquina virtual (EC2), los contenedores (Pods) y los hilos de streaming (Slots):

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│               JERARQUÍA ATÓMICA DE CÓMPUTO EN STREAMING DISTRIBUIDO                    │
└────────────────────────────────────────────────────────────────────────────────────────┘

 [Nivel 1] Instancia EC2 (VM Física/Virtual en AWS)
           └── Provista de vCPUs, RAM del Host y Discos SSD NVMe locales.
               │
               ▼ (Orquestado por Kubernetes / cgroups)
 [Nivel 2] Pod de Kubernetes (1 Pod = 1 JVM Worker)
           └── Aislamiento estricto de recursos (ej. limit: 4 vCPU, 16 GB RAM).
           └── Montaje hostPath exclusivo al NVMe de la máquina (/opt/flink/rocksdb).
               │
               ▼ (Gestionado por el Runtime de Flink)
 [Nivel 3] Task Slots (Unidad de Paralelismo dentro de la JVM)
           └── Cada Slot equivale a 1 hilo concurrente de CPU compartiendo el Heap.
           └── Dimensionamiento: 1 TaskManager Pod aloja típicamente N slots (ej. 2 a 4).
               │
               ▼ (Asignado dinámicamente por el JobMaster)
 [Nivel 4] Subtareas del DAG (Subtasks)
           └── Slot 1: Ejecuta Source + CoProcessFunction (KeyGroup 0..63).
           └── Slot 2: Ejecuta DynamicRuleBroadcastProcessFunction (Broadcast Replica).
```

##### Segregación de Node Pools en AWS EKS (Master vs Worker)

En un diseño de producción institucional no se mezclan las cargas de trabajo en un único tipo de máquina:
1. **Node Pool de Control / Master (Instancias On-Demand m5.large):**
   - Aloja el **JobManager Pod** y el **Flink Kubernetes Operator Pod**.
   - No requiere discos NVMe especiales porque el JobManager no almacena claves ni ejecuta RocksDB; únicamente coordina metadatos, registra slots disponibles e inyecta barreras de Checkpoint.
   - **¿Por qué aislado en una EC2 propia?** Para que un TaskManager saturado por un pico de datos o un error de memoria (OOM) jamás comprometa ni degrade el cerebro del clúster (`JobMaster` y `ResourceManager`).
2. **Node Pool de Cómputo / Worker (Instancias c5d.4xlarge / i3en Spot con NVMe local):**
   - Aloja exclusivamente los **TaskManager Pods**.
   - Cada instancia tiene acoplado físicamente uno o más discos NVMe ultrarrápidos montados mediante `hostPath` en `/mnt/nvme` para alojar los memtables y archivos SSTables de RocksDB.

##### ¿Cuántos Pods de TaskManager caben en una sola instancia EC2? (Bin-Packing vs 1:1)

Una confusión frecuente es asumir que 1 EC2 equivale estrictamente a 1 TaskManager:
- **Estrategia Bin-Packing (Múltiples Pods por EC2):** Si seleccionas una instancia `c5d.4xlarge` (16 vCPU, 32 GB RAM), Kubernetes puede empaquetar **dos (2) TaskManager Pods** en esa misma máquina (cada uno limitado por cgroups a 4 vCPU y 8 GB RAM). Cada Pod ejecuta su propio proceso JVM aislado, y ambos comparten el disco NVMe local de alta velocidad mediante subdirectorios separados (`/mnt/nvme/pod-1` y `/mnt/nvme/pod-2`).
- **Estrategia 1:1 (1 Pod por EC2):** Si usas instancias más compactas como `c5d.2xlarge` (8 vCPU, 16 GB RAM), dimensionas el Pod para consumir casi la totalidad de la máquina, garantizando cero contención entre procesos vecinos.

##### ¿Qué ocurre exactamente cuando escalas el manifiesto YAML (`spec.taskManager.replicas`)?

Cuando el equipo de ingeniería o un autoscaler actualiza el manifiesto, **se escalan Pods, no instancias EC2 directamente**:

```text
 1. GitOps / Developer ──► Actualiza FlinkDeployment: replicas de 2 a 4
                                 │
                                 ▼
 2. AWS EKS API Server  ──► Notifica el cambio al Flink Kubernetes Operator Pod
                                 │
                                 ▼
 3. Flink Operator      ──► Crea 2 nuevos Pods de TaskManager en el clúster
                                 │
                                 ▼
 4. ¿Falta capacidad?   ──► Karpenter / Cluster Autoscaler detecta Pods "Pending"
                            y aprovisiona una nueva instancia EC2 en AWS en < 45s
                                 │
                                 ▼
 5. EKS Scheduler       ──► Ubica el nuevo TaskManager Pod en la nueva instancia EC2
                                 │
                                 ▼
 6. TaskManager Pod     ──► Inicia JVM, formatea directorio en el NVMe local,
                            levanta sus N Task Slots y se auto-registra ante el JobManager
                                 │
                                 ▼
 7. JobManager          ──► Reasigna las particiones de Kafka y los Key Groups de RocksDB
                            ¡Pipeline escalado horizontalmente con CERO intervención manual!
```

##### ¿El escalado duplica necesariamente los recursos (2x)? (Granularidad vs Particiones de Kafka)

Una duda recurrente en comités de infraestructura es si escalar implica duplicar las máquinas o los pods (ej. pasar obligatoriamente de 3 a 6 trabajadores). En producción, el comportamiento es **granular y adaptable**:

1. **A nivel de Pods (Trabajadores de Flink):**
   - El escalado **no obliga a duplicar**. Puedes escalar de 3 a 4, a 5 o a 6 Pods según las necesidades del tráfico.
   - **El Techo Real lo Mandan las Particiones de Kafka:**
     - Si el topic `movements.incoming` tiene **6 particiones**:
       - Con **3 Pods** (1 slot c/u): cada pod procesa **2 particiones** (6 / 3 = 2).
       - Con **6 Pods** (1 slot c/u): cada pod procesa **1 partición** (6 / 6 = 1). Máxima eficiencia y perfecto balance.
       - Con **4 Pods**: dos pods procesan 2 particiones y dos pods procesan 1 partición (desbalance leve pero funcional).
       - Con **7 Pods**: **1 Pod se queda 100% ocioso** (cero rendimiento ganado), porque una partición de Kafka jamás puede dividirse entre dos lectores concurrentes del mismo grupo de consumo.
2. **A nivel de Servidores Físicos (Instancias EC2):**
   - **Las máquinas EC2 jamás se duplican por decreto.**
   - Si tu servidor actual tiene holgura de CPU/RAM, el nuevo Pod se coloca en la misma máquina (**cero costo extra de infraestructura**).
   - Si el servidor se satura, **Karpenter aprovisiona únicamente la máquina mínima necesaria** para alojar los Pods pendientes, no un clon completo de la flota existente.

---

### 5.3 Topología de Clúster: EKS Dedicado vs EKS Compartido con Kafka

Una discusión recurrente en comités de arquitectura de datos surge de la propuesta: *"Aprovechemos el clúster EKS existente y coloquemos allí los brokers de Kafka en instancias EC2, compartiendo la infraestructura con Apache Flink"*.

Para abordar esta propuesta con rigor técnico, es imperativo aclarar la terminología: en Kubernetes **no existen "instancias EC2 dentro de EKS" como islas independientes**. Un clúster EKS está constituido enteramente por instancias EC2 que operan como *Worker Nodes*. Por lo tanto, la disyuntiva real radica en: **¿Deben los Pods de Kafka y los Pods de Flink residir en el mismo clúster EKS, y bajo qué condiciones físicas de aislamiento?**

Existen dos caminos arquitectónicos válidos según el nivel de madurez y criticidad del negocio:

```text
OPCIÓN 1: Coexistencia en 1 Clúster EKS con Aislamiento Físico Riguroso (Taints & Tolerations)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                   Clúster AWS EKS Único                                │
│                                                                                        │
│   Managed Node Group: kafka-storage-pool              Managed Node Group: flink-compute│
│   (Instancias EC2 i3en.2xlarge - NVMe + EBS)          (Instancias EC2 c5ad.4xlarge)    │
│   Taint: dedicated=kafka:NoSchedule                   Taint: dedicated=flink:NoSchedule│
│   ┌────────────────────────────────────────┐          ┌──────────────────────────────┐ │
│   │ Kafka Broker Pods (Strimzi Operator)   │          │ Flink TaskManager Pods       │ │
│   │ - PageCache dedicado en Linux Host     │          │ - CPU intensivo              │ │
│   │ - Ancho de banda de red reservado      │          │ - Scratch NVMe para RocksDB  │ │
│   └────────────────────────────────────────┘          └──────────────────────────────┘ │
│                         ▲                                           │                  │
│                         └─────────── Comunicación Pod-a-Pod ────────┘                  │
└────────────────────────────────────────────────────────────────────────────────────────┘

OPCIÓN 2 [ESTÁNDAR ENTERPRISE]: Segregación Total de Infraestructura y Blast Radius
┌───────────────────────────────────────┐       ┌────────────────────────────────────────┐
│  Clúster EKS Dedicado: Cómputo Flink  │       │ Capa Dedicada: Persistencia de Kafka   │
│                                       │       │             (AWS MSK)                  │
│   ┌───────────────────────────────┐   │       │   ┌────────────────────────────────┐   │
│   │         JobManager Pod        │   │       │   │   Kafka Broker 1 (Multi-AZ)    │   │
│   └───────────────────────────────┘   │       │   └────────────────────────────────┘   │
│   ┌───────────────────────────────┐   │       │   ┌────────────────────────────────┐   │
│   │ TaskManager 1 (NVMe Scratch)  │   │       │   │   Kafka Broker 2 (Multi-AZ)    │   │
│   └───────────────────────────────┘   │       │   └────────────────────────────────┘   │
│   ┌───────────────────────────────┐   │       │   ┌────────────────────────────────┐   │
│   │ TaskManager 2 (NVMe Scratch)  │   │       │   │   Kafka Broker 3 (Multi-AZ)    │   │
│   └───────────────────────────────┘   │       │   └────────────────────────────────┘   │
└───────────────────┬───────────────────┘       └───────────────────▲────────────────────┘
                    │                                               │
                    └────────── PrivateLink / Cross-VPC Peering ────┘
```

#### Opción 1: Coexistencia en un Único Clúster EKS con Aislamiento Físico Riguroso

Esta opción es viable en fases tempranas o bajo restricciones estrictas de presupuesto para evitar duplicar el costo fijo de múltiples planos de control de EKS ($73 USD mensuales por clúster) y simplificar la observabilidad en un solo panel de Kubernetes.

Sin embargo, para evitar la degradación del sistema, aplica una **regla inquebrantable**:  
> **Kafka y Flink NUNCA deben ejecutarse en los mismos Worker Nodes (instancias EC2 compartidas)** (ver decisión corporativa en el [ADR-001](#adr-001-segregación-estricta-entre-cómputo-flink-y-almacenamiento-kafka)).

##### El Peligro del "Vecino Ruidoso" (*Noisy Neighbor*):
Si un Pod de Kafka y un Pod de TaskManager de Flink comparten la misma instancia EC2:
- **Competencia por PageCache:** Kafka depende críticamente de la memoria RAM del sistema operativo libre para almacenar el log en caché (*Linux PageCache*) y lograr transferencias con cero copia (*zero-copy sendfile*). Flink ejecuta operaciones off-heap agresivas con RocksDB. RocksDB desalojará el PageCache de Kafka, disparando lecturas a disco y multiplicando la latencia de publicación.
- **Saturación de la Interfaz de Red (ENI):** Una redistribución de datos (*data shuffle*) en Flink durante una ventana de agregación masiva saturará la interfaz de red (ENI) de la instancia EC2. Esto causará que los brokers de Kafka pierdan sus latidos (*heartbeats*) con el cluster metadata controller (KRaft), desatando elecciones de líderes y rebalances de particiones continuos en toda la empresa.

##### Implementación Mandatoria en Kubernetes: Taints, Tolerations y Node Affinity
Para garantizar que los Pods se ubiquen estrictamente en sus máquinas correspondientes sin cruzarse jamás, se deben configurar dos **Managed Node Groups** independientes en EKS:

1. **Node Group para Kafka (`kafka-storage-pool`):**
   - Instancias optimizadas para almacenamiento (ej. familia `i3en.2xlarge` o `r5b.2xlarge` con volúmenes EBS `io2` / `gp3`).
   - Etiqueta de Nodo (*Label*): `workload: kafka`
   - Restricción de Nodo (*Taint*): `dedicated=kafka:NoSchedule`
2. **Node Group para Flink (`flink-compute-pool`):**
   - Instancias optimizadas para cómputo y memoria (ej. familia `c5ad.4xlarge` o `m5ad.4xlarge` con discos NVMe locales scratch).
   - Etiqueta de Nodo (*Label*): `workload: flink`
   - Restricción de Nodo (*Taint*): `dedicated=flink:NoSchedule`

```yaml
# Manifiesto FlinkDeployment: Garantiza aislamiento físico en el pool de cómputo
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: poc-apache-flink-transaction-validator
spec:
  image: flink:2.0.2-java21
  flinkVersion: v2_0
  podTemplate:
    spec:
      # Toleration: Permite que el Pod de Flink sea programado en nodos con el Taint de Flink
      tolerations:
        - key: "dedicated"
          operator: "Equal"
          value: "flink"
          effect: "NoSchedule"
      # Node Affinity: Obliga a que el Pod de Flink se ubique EXCLUSIVAMENTE en el pool de cómputo
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: workload
                    operator: In
                    values:
                      - flink
```

---

#### Opción 2: Segregación Estricta de Infraestructura (Estándar Enterprise Recomendado)

En arquitecturas de misión crítica, el estándar global de la industria (adoptado por organizaciones como Uber, Netflix y LinkedIn) es **segregar completamente la capa de persistencia (Kafka) de la capa de cómputo (Flink)**, desplegando Kafka en **AWS MSK (Managed Streaming for Apache Kafka)** o en una VPC independiente.

##### Justificación Técnica de Alto Nivel: Aislamiento del Radio de Impacto (*Blast Radius*)

1. **Ciclos de Vida y Actualizaciones de Kubernetes Dispares:**
   - **Frecuencia de Actualizaciones de EKS:** AWS EKS retira versiones de Kubernetes periódicamente, exigiendo actualizar versiones menores (ej. de 1.29 a 1.30) cada 12 meses. La actualización de un clúster EKS requiere realizar un drenado secuencial (*kubectl drain*) de todos los Worker Nodes para reemplazar las AMIs.
   - **Riesgo Operativo Crítico en Kafka:** Si Kafka corre dentro de ese EKS, drenar un nodo implica mover brokers, obligando al clúster de Kafka a reasignar líderes de partición y sincronizar réplicas (*in-sync replicas - ISR*) a través de la red interna. Si un drenado falla o satura los enlaces, se desata una interrupción de ingesta que afecta a toda la compañía.
   - **Ventaja de la Segregación:** Con Kafka desacoplado en AWS MSK, el clúster EKS de Flink puede destruirse, reconstruirse o actualizarse sin poner en riesgo ni un solo mensaje de la infraestructura troncal de datos.

2. **Gobernanza y Propiedad Organizacional:**
   - Kafka es el *Enterprise Data Backbone*, administrado por el equipo central de Plataforma de Datos con SLAs del 99.99%.
   - Los pipelines de Flink son desarrollados y desplegados frecuentemente por múltiples equipos de producto y analítica. Un error de código, un memory leak en un UDF de Java o un bug en una librería externa en Flink jamás debe amenazar la disponibilidad del clúster de Kafka.

##### Veredicto de Decisión para el Equipo:
- Si el presupuesto es sumamente ajustado: Implementar la **Opción 1** bajo la condición no negociable de **Dedicated Node Groups + Taints y Tolerations**.
- Si el escenario maneja transacciones financieras, facturación o alta criticidad: Adoptar la **Opción 2** con **AWS MSK + EKS Dedicado para Flink**.

---

### 5.4 Niveles de Almacenamiento: Volúmenes NVMe Scratch y Almacén S3

```text
               ┌────────────────────────────────────────────────────────┐
               │           Arquitectura de Almacenamiento en 2 Niveles  │
               └────────────────────────────────────────────────────────┘
                                            │
                    ┌───────────────────────┴───────────────────────┐
                    ▼                                               ▼
       Nivel 1: Local de Alta Velocidad                 Nivel 2: Remoto y Durable
      ┌─────────────────────────────┐                ┌─────────────────────────────┐
      │  Disco Local NVMe (Scratch) │                │     AWS S3 / S3 Express     │
      │  - Memtables de RocksDB     │                │  - Metadatos de Checkpoints │
      │  - Archivos SST Activos     │                │  - Savepoints Permanentes   │
      │  - Latencia en microsegundos│                │  - Durabilidad 99.999999999%│
      └─────────────────────────────┘                └─────────────────────────────┘
```

1. **Almacenamiento Local NVMe en Instancias:**
   - Montar el disco NVMe local en la ruta `/opt/flink/rocksdb` utilizando volúmenes de Kubernetes tipo `hostPath` o PersistentVolumes locales.
   - Configuración requerida: `state.backend.rocksdb.localdir: /opt/flink/rocksdb` (adoptado en el [ADR-002](#adr-002-embeddedrocksdbstatebackend-con-nvme-local-y-checkpoints-en-s3)).
2. **Almacenamiento Remoto Persistente en Objetos:**
   - Configurar el bucket S3 con políticas de ciclo de vida para checkpoints antiguos:
   ```properties
   state.backend: embeddedrocksdb
   state.backend.incremental: true
   state.checkpoints.dir: s3://poc-apache-flink-storage-prod/checkpoints
   state.savepoints.dir: s3://poc-apache-flink-storage-prod/savepoints
   ```

---

### 5.5 Topología de Red y Conectividad Cross-VPC

```text
┌─────────────────────────────────┐         VPC Peering          ┌─────────────────────────────────┐
│        Flink Compute VPC        │◄────────────────────────────►│        Kafka Storage VPC        │
│                                 │      o AWS PrivateLink       │                                 │
│  ┌───────────────────────────┐  │                              │  ┌───────────────────────────┐  │
│  │   EKS Private Subnet      │  │                              │  │   MSK Private Subnet      │  │
│  │   TaskManager Nodes       │  │                              │  │   Kafka Broker Nodes      │  │
│  └─────────────┬─────────────┘  │                              │  └─────────────▲─────────────┘  │
└────────────────┼────────────────┘                              └────────────────┼────────────────┘
                 │                                                                │
                 ▼ (S3 VPC Gateway Endpoint)                                      │
┌─────────────────────────────────┐                                               │
│       AWS S3 Storage            │                                               │
│       Checkpoint Store          │                                               │
└─────────────────────────────────┘                                               │
```
- **VPC Gateway Endpoints:** Provisionar un Gateway Endpoint de S3 dentro de la VPC de Flink para garantizar que el tráfico de checkpoints viaje íntegramente por la red privada de AWS sin generar costos de transferencia vía NAT Gateway.
- **Subredes Privadas:** Los TaskManagers y los Brokers de Kafka deben operar exclusivamente dentro de subredes privadas no enrutables a internet.

---

### 5.6 Estrategia de Despliegue en Producción (Zero Data Loss para Misión Crítica)

En microservicios sin estado (*stateless*), las estrategias comunes como el **Rolling Update** de Kubernetes reemplazan pods gradualmente. En procesamiento de flujos con estado distribuido y criticidad financiera, **el Rolling Update tradicional está PROHIBIDO**:

> [!CAUTION]
> **Por qué el Rolling Update Tradicional es Destructivo en Flink:**  
> Si Kubernetes mata un Pod de TaskManager para actualizar su imagen, el JobManager interpreta una caída imprevista y entra en modo de recuperación de fallos (*failover*, ver [Sección 2.2](#22-topología-de-ejecución-arquitectura-master-worker-en-runtime)), deteniendo todo el pipeline. Al arrancar un nuevo Pod con una versión modificada del JAR mientras otros pods corren la versión previa, se desatan incompatibilidades binarias al deserializar el estado de RocksDB y se rebobinan offsets desordenadamente, rompiendo la consistencia Exactly-Once (ver [Sección 3.3](#33-kafkasink-y-el-protocolo-two-phase-commit-2pc)).

#### Estrategia 1: Actualización Dirigida por Savepoint con Flink K8s Operator (Recomendada para CI/CD)

Es el estándar de la industria cuando los cambios en el código preservan compatibilidad de estado (adición de lógica, corrección de bugs, optimizaciones):

```text
Flujo Automatizado del Flink Kubernetes Operator (upgradeMode: savepoint):

 [1] Pipeline CI/CD ────────► Actualiza manifiesto FlinkDeployment (nueva imagen o JAR)
                                    │
                                    ▼
 [2] Flink K8s Operator ────► Dispara Savepoint síncrono a S3 (Drena y pausa ingesta)
                                    │
                                    ▼
 [3] AWS S3 Bucket ─────────► Savepoint confirmado en s3://.../savepoints/savepoint-xyz
                                    │
                                    ▼
 [4] Flink K8s Operator ────► Detiene y elimina JobManager y TaskManagers antiguos
                                    │
                                    ▼
 [5] Flink K8s Operator ────► Lanza nuevos Pods con nueva imagen/JAR pasando:
                              --fromSavepoint s3://.../savepoints/savepoint-xyz
                                    │
                                    ▼
 [6] Nuevo Pipeline ────────► Reanuda consumo desde offsets exactos del Savepoint
                              ¡CERO pérdida de datos, CERO duplicados!
```

- **Configuración en el manifiesto declarativo de Kubernetes:**
```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: poc-apache-flink-transaction-validator
spec:
  image: flink:2.0.2-java21
  flinkVersion: v2_0
  upgradeMode: savepoint # Ordena al operador pausar con savepoint antes de actualizar
  state:
    savepointsDir: s3://poc-apache-flink-storage-prod/savepoints
```

#### Estrategia 2: Blue/Green con Shadow Pipeline (Para Migraciones de Estado Incompatibles)

Cuando se implementa un cambio estructural incompatible en las clases de estado (ej. cambio en el tipo de dato de un `ValueState` existente o redefinición radical del `keyBy`), Flink no puede restaurar el Savepoint anterior. En este escenario crítico se implementa **Shadow Pipeline**:

```text
                                  ┌─────────────────────────────┐
                                  │   Topic Kafka: Ingesta      │
                                  └──────────────┬──────────────┘
                                                 │
                        ┌────────────────────────┴────────────────────────┐
                        │ Consumer Group: flink-blue                      │ Consumer Group: flink-green
                        ▼                                                 ▼
          ┌───────────────────────────┐                     ┌───────────────────────────┐
          │  Clúster Blue (Producción) │                     │ Clúster Green (Candidato) │
          │  Versión Actual (v1.0.0)  │                     │ Nueva Versión (v2.0.0)    │
          └─────────────┬─────────────┘                     └─────────────┬─────────────┘
                        │ Escrituras Activas                              │ Escrituras en Shadow Sink
                        ▼                                                 ▼
          ┌───────────────────────────┐                     ┌───────────────────────────┐
          │     DynamoDB Producción   │                     │  DynamoDB Tabla Shadow /  │
          │   (Sirve a usuarios/APIs) │                     │  Verificación de Métricas │
          └───────────────────────────┘                     └───────────────────────────┘
```

1. **Despliegue Aislado:** Se despliega el pipeline **Green** con un nuevo `group.id` en Kafka, permitiendo que lea los eventos concurrentemente con Blue sin interferir en sus offsets.
2. **Período de Observación (Warm-up & Shadowing):** Green procesa eventos en paralelo y escribe hacia una tabla shadow o emite métricas a un topic de validación. Se comparan resultados contables y throughput contra Blue.
3. **Conmutación Controlada:** Una vez verificada la paridad al 100%, se activa la escritura de Green hacia las tablas principales de negocio y se cancela el clúster Blue con un Savepoint de respaldo final.

#### Automatización del Ciclo de Vida: Helm vs Kustomize en IaC y CI/CD

En producción corporativa, el despliegue manual mediante comandos ad-hoc de terminal está estrictamente prohibido. La arquitectura desacopla el ciclo de vida en dos niveles complementarios:

| Nivel | Herramienta Recomendada | Rol y Alcance de Responsabilidad | Orquestador / Automatizador |
|---|---|---|---|
| **Plataforma (Infraestructura)** | **Helm** | Empaquetado e instalación del **Flink Kubernetes Operator**, Webhooks de admisión, RBAC y CRDs oficiales de Apache Flink. | Terraform / OpenTofu (recurso `helm_release`) |
| **Aplicaciones (Pipelines)** | **Kustomize** | Parametrización declarativa de los manifiestos `FlinkDeployment` mediante `base/` y parches limpios en `overlays/` (Dev, QA, Prod) sin plantillas complejas. | CI/CD (GitHub Actions / GitLab CI) o GitOps (ArgoCD / Flux) |

```text
Flujo de Automatización Continua (IaC + CI/CD):

 [1] Terraform (IaC) ────► Aprovisiona EKS Control Plane y Node Groups (EC2 c5ad)
           │
           ▼
 [2] Terraform (Helm) ───► Instala Flink Kubernetes Operator en EKS
           │
           ▼
 [3] Pipeline CI/CD ─────► 1. Compila JAR en Java 21 (mvn clean package)
                           2. Publica JAR inmutable en S3 (s3://.../jars/v1.0.0/app.jar)
                           3. Actualiza versión del JAR en overlay de Kustomize
                           4. Sincroniza en EKS (kubectl apply -k overlays/prod o ArgoCD)
           │
           ▼
 [4] Flink K8s Operator ─► Detecta cambio en FlinkDeployment, ejecuta Savepoint síncrono
                           a S3, reinicia Pods y reanuda el consumo con Zero Data Loss.
```

#### Estrategia Agnóstica de CI/CD: Gestión de Dependencias (Day-0, Day-1, Day-2) y el Problema "Greenfield"

Un desafío crítico en arquitecturas empresariales es la **gestión de dependencias temporales** entre la infraestructura y las aplicaciones. Si un equipo fusiona código limpio de `dev` a `main` en un repositorio compartido (monorepo), los motores de CI/CD (GitHub Actions, GitLab CI, Azure DevOps) por defecto intentan ejecutar todos los pipelines en paralelo. Esto produce graves problemas operativos:
- **Condición de Carrera en Despliegue:** El pipeline de la aplicación intenta aplicar `FlinkDeployment` antes de que Terraform termine de aprovisionar EKS o antes de que Helm instale los CRDs de Flink.
- **Acoplamiento Tóxico y Lentitud:** Un cambio de una sola línea en el código Java (`src/`) no debe disparar un análisis de Terraform (`terraform plan`), lo cual añadiría entre 15 y 25 minutos innecesarios al ciclo de entrega.
- **Fallas en Cadena:** Una falla temporal en un recurso de cloud bloquea la entrega de parches de código que ya estaban listos y probados.

##### Taxonomía del Ciclo de Vida: Day-0, Day-1 y Day-2

Para desacoplar el flujo de forma agnóstica a cualquier herramienta de CI/CD, la plataforma se divide en tres fases estrictas:

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        DESACOPLAMIENTO DE PIPELINES POR FASES                          │
└────────────────────────────────────────────────────────────────────────────────────────┘

 [FASE DAY-0: Infraestructura Base (Terraform / OpenTofu)]
  Trigger: Cambios en `iac/**` o manual con aprobación de SRE.
  State: Remoto e independiente (`terraform.tfstate` en S3 + DynamoDB Lock).
  Acción: Aprovisiona VPC, EKS Control Plane, Subnets, MSK y S3 Buckets.
      │
      ▼ (Se ejecuta una única vez o ante cambios de arquitectura de red/cómputo)
 [FASE DAY-1: Plataforma y Bootstrap (Helm)]
  Trigger: Integrado en el pipeline de Infraestructura (como recurso `helm_release`)
           o ejecutado como stage post-aprovisionamiento de EKS.
  Acción: Instala Cert-Manager y el Flink Kubernetes Operator oficial con sus CRDs.
      │
      ▼ (El clúster EKS queda 100% listo para recibir aplicaciones de streaming)
 [FASE DAY-2: Ciclo de Vida de la Aplicación (Maven + Kustomize)]
  Trigger: Cambios en `src/**`, `pom.xml` o `k8s/**`.
  Desacoplamiento Total: NO toca ni ejecuta Terraform.
  Acción: 
    1. Build & Test: Compila el JAR con Java 21 (`mvn clean package`).
    2. Publicación Inmutable: Sube el artefacto a S3 (`s3://.../jars/<commit-sha>.jar`).
    3. Pre-Flight Health Gate: Verifica que el clúster y el operador estén listos.
    4. Despliegue Declarativo: Aplica `kubectl apply -k k8s/overlays/<env>`.
```

##### Solución Arquitectónica para Pases Limpios (Greenfield) y Concurrencia

1. **Filtrado Estricto de Rutas (Path-Based Triggers):**
   Independiente de la plataforma de CI/CD, los disparadores deben aislarse por rutas:
   - **Pipeline de Infra:** `paths: ['iac/**']`
   - **Pipeline de App:** `paths: ['src/**', 'pom.xml', 'k8s/**']`
   Esto erradica ejecuciones redundantes de Terraform cuando solo se modifica lógica de negocio.

2. **Desacoplamiento del Almacenamiento de Artefactos (S3):**
   La compilación de Java y la publicación del JAR en S3 es **autónoma**: no requiere que el clúster EKS esté activo. El pipeline de la aplicación puede compilar y persistir el artefacto en S3 independientemente de si el clúster está en mantenimiento o en proceso de creación.

3. **Pre-Flight Health Gate (Barrera de Preparación en el Pipeline de Aplicación):**
   Para evitar que el pipeline de aplicación colapse al intentar aplicar un manifiesto en un clúster inexistente, el paso de despliegue ejecuta una verificación previa agnóstica:
   ```bash
   # Verifica conectividad al clúster y presencia de los CRDs del Operador de Flink
   kubectl get crd flinkdeployments.flink.apache.org --request-timeout='10s' || {
     echo "ERROR: El Flink Kubernetes Operator no está disponible en este clúster EKS."
     echo "Asegúrese de que el pipeline de infraestructura (Day-0/Day-1) haya finalizado."
     exit 1
   }
   ```

4. **Secuencia de Pase Limpio (Greenfield Deployment Sequence):**
   Cuando se aprovisiona una nueva región o cuenta desde cero (`main`), la orquestación sigue una **cadena secuencial de pipelines** (mediante triggers entre pipelines o stages con dependencias explícitas `needs: [infra-provisioning]`):
   ```text
   Pipeline Infra (IaC) ──► Bootstrap Operador (Helm) ──► Pipeline Aplicación (Kustomize)
   ```
   Una vez completado el Day-0 y Day-1, el pipeline de infraestructura queda en reposo, y el pipeline de la aplicación opera de forma continua y autónoma en Day-2.

##### Consideraciones Avanzadas de Plataforma: Repositorios, Estado y Desacoplamiento

1. **Topología de Repositorios: Polyrepo como Estándar Empresarial:**
   - **Repositorio de Plataforma (`infra-streaming-platform`):** Gestionado exclusivamente por el equipo de **SRE / DevOps**. Contiene los módulos de Terraform (VPC, EKS, MSK, S3) y el recurso `helm_release` del Flink Operator. Sus ciclos de cambio son de semanas o meses.
   - **Repositorio de Aplicación (`poc-apache-flink`):** Gestionado por los **Ingenieros de Software**. Aloja el código Java 21, `pom.xml`, tests de negocio y la carpeta `k8s/` con los manifiestos `FlinkDeployment`. Se despliega múltiples veces al día.
   - **Aislamiento de Seguridad (RBAC):** Esta segregación impide que un desarrollador de aplicaciones posea credenciales con permisos destructivos sobre la infraestructura de red o el clúster de cómputo en AWS.

2. **Gestión de Estado de Helm vs Terraform (`tfstate`):**
   - Helm almacena su propio estado de despliegue dentro del clúster de Kubernetes como **Secrets cifrados** en el namespace del operador (`sh.helm.release.v1.flink-kubernetes-operator.v1`).
   - Cuando Terraform gestiona el chart mediante el recurso `helm_release`, solo registra en su `tfstate` el identificador y la versión del chart.
   - Si se aplican cambios en Terraform sobre otros recursos (ej. modificar un Security Group o agregar un Worker Node EC2), Terraform valida el `helm_release`, detecta que la versión del operador es idéntica y **no ejecuta ninguna reinstalación destructiva** (`Plan: 0 to change`).
   - El operador únicamente se actualiza cuando el equipo de SRE modifica deliberadamente la versión del chart (típicamente una o dos veces al año).

3. **Resolución del Dilema del "Huevo o la Gallina" (La Ceguera Intencional de Terraform):**
   - **El Anti-patrón de ECS:** En AWS ECS, el recurso `aws_ecs_task_definition` de Terraform obliga a declarar la URL de la imagen de contenedor. En un pase limpio (Greenfield), si el registro ECR está vacío, Terraform colapsa a menos que se configure una imagen dummy, introduciendo una discrepancia permanente (*state drift*) entre Terraform y el pipeline de CI/CD.
   - **El Patrón de EKS + Flink (Ceguera Intencional):** En esta arquitectura, **Terraform es 100% ciego a la aplicación Flink**.
     - En el archivo `terraform.tfstate` **NO se registra información sobre el JobManager, los TaskManagers ni el JAR de la aplicación**.
     - Terraform crea únicamente la infraestructura base (el clúster EKS y los buckets S3 vacíos).
     - El pipeline de CI/CD compila el JAR, lo almacena en S3 y aplica el manifiesto `FlinkDeployment` directamente en Kubernetes.
     - Es el **Flink Kubernetes Operator** (dentro de EKS) quien lee el manifiesto, descarga el JAR desde S3 y orquesta los Pods.
     - **Beneficio:** Se erradica por completo la dependencia circular de artefactos, el *state drift* en Terraform y los bloqueos de concurrencia entre infraestructura y código de negocio.

---

## 6. Configuración del Proyecto Java y Screaming Architecture

### 6.1 Screaming Architecture / Vertical Slices

Las arquitecturas convencionales organizan el código por capas técnicas (`controllers`, `services`, `repositories`). En procesamiento de flujos, esto fragmenta la lógica y oscurece la topología del stream.
Este arquetipo implementa **Screaming Architecture**: la estructura de paquetes refleja de inmediato el dominio de negocio y la topología del pipeline:

```text
com.poc.flink/
├── StreamingJobApp.java                 # Composición de topología y configuración del entorno
├── config/
│   └── JobConfig.java                   # Configuración tipada y resolución de variables de entorno
└── transaction/                         # Vertical slice: Dominio de Validación de Transacciones
    ├── function/
    │   ├── TransactionValidationFunction.java # KeyedProcessFunction (reglas de negocio y estado)
    │   └── DeduplicationFilterFunction.java  # Operador de deduplicación del flujo
    ├── model/
    │   ├── TransactionEvent.java        # POJO compatible con Flink PojoSerializer
    │   └── ValidationResult.java        # Modelo enriquecido de salida
    ├── schema/
    │   └── TransactionJsonDeserializer.java # Deserializador Jackson/Avro para el stream
    └── sink/
        └── TransactionDynamoDbSink.java # Sink de almacenamiento idempotente
```

---

### 6.2 Alcance de Dependencias Maven y Estrategias de Shading

Uno de los motivos más comunes de fallos en el despliegue de Flink es el error de `LinkageError` o `ClassNotFoundException` originado por scopes incorrectos en Maven.

#### Reglas Fundamentales del `pom.xml` en Flink
1. **Las Dependencias del Core de Flink DEBEN ser `provided`:**
   La distribución base del clúster de Flink (`/opt/flink/lib/`) ya contiene estas librerías. Empaquetarlas dentro del Fat JAR hace que la JVM intente cargarlas bajo múltiples classloaders, provocando fallos de enlace.
2. **Los Conectores DEBEN ser Sombreados (Shaded / Bundled):**
   Los conectores (como `flink-connector-kafka`, AWS SDK v2 o Jackson) no se incluyen en el runtime base y deben compilarse y empaquetarse dentro del JAR.
3. **El Bytecode Debe Coincidir con el Runtime del Clúster (`--release 21` para Java 21 LTS):**
   Compilar con `<maven.compiler.release>21</maven.compiler.release>` genera bytecode versión `65.0`. Para el análisis a fondo del error `UnsupportedClassVersionError` y la discrepancia con clústeres legados en JVM 17, consulta la [Sección 6.3](#63-cadencia-de-versiones-lts-en-java-y-riesgos-de-java-27).
4. **Fijación Inmutable de Imagen Docker (Prohibición Estricta de `:latest`):**
   El tag `:latest` en Flink apunta históricamente a Java 17 en Docker Hub. Declara siempre la versión inmutable del motor y del JDK (`flink:2.0.2-java21`), según la justificación técnica detallada en la [Sección 6.3](#63-cadencia-de-versiones-lts-en-java-y-riesgos-de-java-27) y el [ADR-004](#adr-004-screaming-architecture-y-serialización-pojo-estricta-en-java-21-lts).

#### Patrón de Configuración en `pom.xml` (Java 21 LTS):
```xml
<properties>
    <maven.compiler.release>21</maven.compiler.release>
    <flink.version>2.0.2</flink.version>
    <kafka.version>3.9.0</kafka.version>
</properties>

<dependencies>
    <!-- Core Flink: DEBE SER PROVIDED EN CLÚSTERES EXTERNOS -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>

    <!-- Conectores: DEBEN SER EMPAQUETADOS EN EL FAT JAR -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka</artifactId>
        <version>4.0.1-2.0</version>
    </dependency>
</dependencies>

<build>
    <plugins>
        <!-- Compilación estricta a bytecode Java 21 -->
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <version>3.13.0</version>
            <configuration>
                <release>21</release>
            </configuration>
        </plugin>

        <!-- Construcción del Fat JAR sombreado -->
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.6.0</version>
            <executions>
                <execution>
                    <phase>package</phase>
                    <goals><goal>shade</goal></goals>
                    <configuration>
                        <artifactSet>
                            <excludes>
                                <exclude>org.apache.flink:force-shading</exclude>
                                <exclude>com.google.code.findbugs:jsr305</exclude>
                                <exclude>org.slf4j:*</exclude>
                                <exclude>org.apache.logging.log4j:*</exclude>
                            </excludes>
                        </artifactSet>
                        <transformers>
                            <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                <mainClass>com.poc.flink.StreamingJobApp</mainClass>
                            </transformer>
                        </transformers>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

---

### 6.3 Cadencia de Versiones LTS en Java y Riesgos de Java 27

Una interrogante habitual en ingeniería empresarial es: *"¿Por qué estandarizar en Java 21 LTS en lugar de adoptar versiones experimentales como Java 27?"* Comprender el modelo de lanzamientos de Java y las características internas de los motores de streaming explica por qué esto responde a estabilidad intencional y no a obsolescencia.

#### La Cadencia Bianual de Versiones LTS:

| Versión de Java | Fecha de Lanzamiento | Tipo de Versión | Estado de Producción Empresarial |
|---|---|---|---|
| **Java 17** | Septiembre 2021 | **LTS** | **Baseline Empresarial Activo** (Soportado universalmente). |
| **Java 21** | Septiembre 2023 | **LTS** | **Estándar Productivo Moderno** (Soporte oficial certificado en Flink 2.x). |
| **Java 25** | Septiembre 2025 | **LTS** | **En Desarrollo** en la comunidad (`FLINK-37719`); previsto para Flink 2.4+. |
| **Java 26** | Marzo 2026 | Non-LTS (Feature) | Versión corta de prueba con 6 meses de soporte; **NO para producción**. |
| **Java 27** | Septiembre 2026 | Non-LTS (Feature) | Versión corta de prueba con 6 meses de soporte; **NO para producción**. |
| **Java 29** | Septiembre 2027 | **LTS** | Próxima versión planificada de Soporte Extendido. |

> [!NOTE]
> **El "LTS Adoption Lag" en Big Data y el Roadmap de Java 25 (`FLINK-37719`):**  
> En plataformas distribuidas (Flink, Kafka, Spark), siempre existe un **retraso de adopción de 12 a 18 meses** tras la publicación de un nuevo LTS. Las librerías de bajo nivel—específicamente los binarios JNI en C++ de RocksDB, los buffers de red off-heap de Netty y los gestores de memoria—deben someterse a pruebas de regresión exhaustivas frente a los cambios internos de la JVM.  
> Actualmente, la comunidad de Apache Flink se encuentra portando y certificando Java 25 bajo el Epic **`FLINK-37719`** (incluyendo subtareas como **`FLINK-39806`** para directivas de acceso nativo como `--sun-misc-unsafe-memory-access=allow`). Se proyecta su disponibilidad general en versiones futuras (ej. Flink 2.4+).  
> **El Veredicto Arquitectónico:** La recomendación firme para producción hoy es **Java 21 LTS** (o Java 17 LTS en infraestructuras legacy). No se debe desplegar Java 25 hasta que la comunidad declare su soporte oficial como GA.

> [!CAUTION]
> **Java 27 NO es una Versión LTS:**  
> Las versiones no-LTS (como Java 26 y Java 27) únicamente reciben parches y actualizaciones de seguridad durante **6 meses**. Desplegar infraestructura de datos con estado sobre versiones no-LTS obliga a migraciones trimestrales y expone la plataforma a vulnerabilidades sin parches.

#### Riesgos Técnicos al Ejecutar Flink sobre Versiones Experimentales (Java 27):

1. **JEP 471 y Restricciones a `sun.misc.Unsafe`:**
   - El administrador de memoria off-heap de Flink (`MemoryManager`), los pools de buffers de red y los serializadores invocan operaciones de bajo nivel mediante `sun.misc.Unsafe`.
   - Las versiones recientes de Java (JEP 403 y JEP 471) encapsulan paquetes internos y restringen el acceso a memoria mediante `Unsafe`. Ejecutar sobre versiones no adaptadas genera excepciones fatales `InaccessibleObjectException` y caídas en el arranque.

2. **Incompatibilidades de Memoria Nativa y JNI en RocksDB:**
   - `EmbeddedRocksDBStateBackend` ejecuta código compilado en C++ a través de JNI.
   - Las versiones experimentales de la JVM modifican con frecuencia el mapeo de memoria y la gestión de `DirectByteBuffer`. Enlaces JNI no certificados con Java 27 provocan fugas de memoria nativa y **Segmentation Faults** que derriban el contenedor sin dejar trazas en los logs de Java.

3. **Confusión sobre Hilos Virtuales (Project Loom):**
   - Aunque los Virtual Threads son beneficiosos para microservicios web bloqueantes (Spring Boot / Tomcat), representan un **anti-patrón para los Task Slots de Flink**.
   - Los slots de Flink procesan bucles cerrados y continuos a millones de eventos por segundo. Cuando un hilo virtual encuentra llamadas JNI hacia RocksDB o bloques de código sincronizados, queda anclado al hilo portador (*carrier thread pinning*), desatando inanición de hilos y backpressure destructivo.

4. **Ausencia de Imágenes Oficiales:**
   - La Apache Software Foundation solo compila y certifica imágenes oficiales de contenedores sobre **Java 17 y Java 21 LTS**. No existen distribuciones mantenidas para Java 27.

---

### 6.4 Eficiencia de Serialización POJO y Trampas de Kryo

Flink gestiona la serialización mediante un sistema interno de extracción de tipos. El rendimiento del shuffle en la red depende de si Flink recurre al serializador genérico **Kryo** o aprovecha su **`PojoSerializer`** optimizado.

```text
Jerarquía de Rendimiento:
1. Serializadores de Tipos Básicos (Int, Long, String)  -> Máxima Velocidad (Cero reflexión)
2. Flink PojoSerializer                                  -> Muy Rápido (Offsets directos, sin nombres de clases en el estado)
3. Serializador Kryo                                     -> 10x-50x MÁS LENTO (Reflexión profunda, almacena metadatos de clases)
```

#### Requisitos para la Conformidad con `PojoSerializer`:
1. La clase debe ser `public` e independiente (o una clase interna estática).
2. Debe poseer un constructor predeterminado sin argumentos `public` (`public MyClass() {}`).
3. Todos los campos no estáticos y no transitorios deben ser `public` o disponer de getters y setters con la convención estándar de Java Beans.
4. Todos los tipos de los campos deben ser soportados por el sistema de tipos de Flink.

#### Forzar la Serialización POJO en Producción:
Incorpora esta instrucción durante la inicialización en `StreamingJobApp.java` para fallar inmediatamente la compilación en caso de que algún modelo recurra a Kryo:
```java
env.getConfig().disableGenericTypes(); // Lanza UnsupportedOperationException si se detecta uso de Kryo
```

---

## 7. Experiencia del Desarrollador Local y Runbook (`poc-apache-flink`)

Históricamente, el desarrollo en streaming padecía de ciclos de retroalimentación lentos: compilar un JAR, subirlo a un clúster remoto y revisar logs distantes. El arquetipo `poc-apache-flink` erradica esta fricción mediante una suite completa de emulación local.

### 7.1 Stack de Arquitectura Local

```text
┌───────────────────────────────────────────────────────────────────────────────┐
│                      Máquina Local del Desarrollador                          │
│                                                                               │
│   ┌─────────────────────────────┐         ┌───────────────────────────────┐   │
│   │  IDE (IntelliJ / VS Code)   │         │    Script deploy.sh           │   │
│   │  MiniCluster Debugger (F5)  │         │    (Empaqueta Fat JAR y sube) │   │
│   └──────────────┬──────────────┘         └───────────────┬───────────────┘   │
└──────────────────┼────────────────────────────────────────┼───────────────────┘
                   │ Conexión Local                         │ docker cp & flink run
                   ▼                                        ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                      Red Docker Compose (poc-flink-net)                       │
│                                                                               │
│   ┌───────────────────────────────┐       ┌───────────────────────────────┐   │
│   │      poc-flink-jobmanager     │◄─────►│     poc-flink-taskmanager     │   │
│   │     (flink:2.0.2-java21)      │       │     (flink:2.0.2-java21)      │   │
│   └───────────────────────────────┘       └───────┬───────────────┬───────┘   │
│                                                   │               │           │
│                           Consumo de Eventos      │               │           │
│                   ┌───────────────────────────────┘               │           │
│                   ▼                                               ▼           │
│   ┌───────────────────────────────┐       ┌───────────────────────────────┐   │
│   │        poc-flink-kafka        │       │        poc-flink-floci        │   │
│   │      (apache/kafka:4.3.1)     │       │    S3: Checkpoints            │   │
│   └───────────────────────────────┘       │    DynamoDB: Ledger Sink      │   │
│                                           └───────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

### 7.2 Pirámide de Testing: Unit, Harness, MiniCluster y Cloud Emulado

```text
             / \
            /   \
           / E2E \       Docker Compose + Floci + Kafka + Clúster Flink (Java 21)
          /───────\
         /  Local  \     Ejecución en MiniCluster embebido en IDE con Breakpoints
        /───────────\
       / Integración \   KeyedOneInputStreamOperatorTestHarness (Verificación de estado)
      /───────────────\
     /    Unitarias    \ Pruebas unitarias de serialización y cumplimiento de POJO
    /───────────────────\
```

1. **Harness Testing:** Utiliza `KeyedOneInputStreamOperatorTestHarness` para adelantar marcas de tiempo, inyectar watermarks sintéticos y verificar transiciones de estado y temporizadores sin necesidad de inicializar un clúster.
2. **MiniCluster en IDE (Depuración Instantánea):** Ejecutar `StreamingJobApp.main()` levanta un clúster embebido en memoria en ~2 segundos, permitiendo fijar breakpoints directamente en `ProcessFunction.processElement()`.

---

### 7.3 Runbook de Verificación Paso a Paso

Sigue este procedimiento para inicializar la infraestructura, aprovisionar recursos y ejecutar el pipeline desde cero.

#### Paso 1: Iniciar la Infraestructura Dockerizada (Java 21 LTS)
```bash
docker compose -f docker/docker-compose.yml up -d
```
Verifica el estado de los servicios:
- Flink Web Dashboard: [http://localhost:8081](http://localhost:8081)
- DynamoDB Admin UI: [http://localhost:8001](http://localhost:8001)

#### Paso 2: Aprovisionar Recursos AWS vía Terraform (Floci)
```bash
cd iac/envs/dev
terraform init
terraform apply -auto-approve
cd ../../..
```
Esto crea:
- Bucket S3: `poc-apache-flink-storage-dev` (para checkpoints y savepoints).
- Tabla DynamoDB: `poc-apache-flink-ledger-dev` (para el ledger del sink idempotente).

#### Paso 3: Depuración Rápida en IDE (Inner-Loop)
1. En IntelliJ IDEA, abre `.run/StreamingJobApp (Local MiniCluster).run.xml` o presiona `F5` en VS Code.
2. Coloca un breakpoint en `TransactionValidationFunction.java`.
3. Flink iniciará en ~2 segundos escuchando eventos en `localhost:9092`.

#### Paso 4: Compilación y Despliegue en el Clúster
Ejecuta el script automatizado:
```bash
chmod +x deploy.sh
./deploy.sh
```
El script:
1. Consulta la API REST del JobManager y cancela jobs previos activos.
2. Compila el Fat JAR con `./mvnw clean package` apuntando a Java 21 (`major version 65`).
3. Copia el binario al JobManager y ejecuta `flink run -d /opt/flink/job.jar`.

#### Paso 5: Emitir Eventos de Prueba y Validar Flujo End-to-End
Publica un evento de transacción en Kafka:
```bash
docker exec -i poc-flink-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic transactions.incoming << 'EOF'
{"transactionId":"tx-9999","accountId":"acc-001","amount":150.00,"timestamp":1728518400000}
EOF
```

Verifica la escritura procesada en DynamoDB:
```bash
aws --endpoint-url=http://localhost:4566 --region us-east-1 dynamodb get-item \
  --table-name poc-apache-flink-ledger-dev \
  --key '{"pk": {"S": "TX#tx-9999"}, "sk": {"S": "ACC#acc-001"}}'
```
Debe retornar el registro confirmado con estado `"status": {"S": "VALIDATED"}`.

---

## 8. Architecture Decision Records (ADRs)

### ADR-001: Segregación Estricta entre Cómputo Flink y Almacenamiento Kafka
- **Estado:** Aceptado
- **Contexto:** Definir si los brokers de Kafka y los TaskManagers de Flink deben cohabitar en el mismo clúster de Kubernetes en producción.
- **Decisión:** Aislar completamente Kafka y Flink en clústeres independientes, utilizando AWS MSK para Kafka y un clúster EKS dedicado para Flink.
- **Consecuencias:** Neutraliza la contención de IOPS en disco y ancho de banda en interfaces ENI; aísla radios de impacto operativo; desacopla ciclos de actualización.
- **Referencia Técnica:** Ver [Sección 5.3](#53-topología-de-clúster-eks-dedicado-vs-eks-compartido-con-kafka).

### ADR-002: EmbeddedRocksDBStateBackend con NVMe Local y Checkpoints en S3
- **Estado:** Aceptado
- **Contexto:** El motor exige un backend de estado capaz de retener gigabytes de estado para deduplicación y recuperación resiliente.
- **Decisión:** Implementar `EmbeddedRocksDBStateBackend` sobre discos locales NVMe de la instancia para operaciones de estado off-heap de baja latencia, y AWS S3 para persistencia asíncrona e incremental de checkpoints.
- **Consecuencias:** Elimina pausas prolongadas de Garbage Collection en la JVM; garantiza durabilidad ante caídas catastróficas; escala el estado más allá de los límites de memoria RAM física.
- **Referencia Técnica:** Ver [Sección 2.4](#24-gestión-de-estado-y-state-backends) y [Sección 5.4](#54-niveles-de-almacenamiento-volúmenes-nvme-scratch-y-almacén-s3).

### ADR-003: Claves de Idempotencia Deterministas para Sinks Downstream
- **Estado:** Aceptado
- **Contexto:** Los sistemas destino (DynamoDB, Pinot, Data Lake) pueden recibir eventos retransmitidos tras la recuperación de un worker de Flink.
- **Decisión:** Calcular claves de registro deterministas (`SHA256` de atributos de negocio inmutables) dentro de Flink y ejecutar operaciones de upsert idempotente en el sink.
- **Consecuencias:** Asegura consistencia de negocio Exactly-Once incluso cuando el almacenamiento final no participe en transacciones distribuidas 2PC.
- **Referencia Técnica:** Ver [Sección 3.5](#35-patrón-de-ingesta-cdc-debezium---kafka---apache-flink) y [Sección 4.3](#43-patrón-a-event-deduplication-con-state-ttl).

### ADR-004: Screaming Architecture y Serialización POJO Estricta en Java 21 LTS
- **Estado:** Aceptado
- **Contexto:** Estructuras multicapa convencionales oscurecen la topología de streaming y provocan caídas de rendimiento por el uso del serializador genérico Kryo.
- **Decisión:** Organizar el repositorio por slices verticales de dominio; compilar estrictamente a Java 21 LTS (`major version 65`); fijar la imagen del clúster a `flink:2.0.2-java21`; prohibir el tag `:latest`; e imponer `env.getConfig().disableGenericTypes()` para forzar el uso exclusivo de `PojoSerializer`.
- **Consecuencias:** Multiplica hasta por 5x el throughput del pipeline eliminando reflexión innecesaria; clarifica el flujo de datos para nuevos desarrolladores; garantiza reproducibilidad inmutable entre el código Java y los contenedores de producción.
- **Referencia Técnica:** Ver [Sección 6.1](#61-principios-rectores-del-diseño-de-código), [Sección 6.3](#63-cadencia-de-versiones-lts-en-java-y-riesgos-de-java-27) y [Sección 6.4](#64-serialización-de-alto-rendimiento-pojos-vs-kryo).

### ADR-005: Broadcast State Pattern para Reglas Dinámicas en Memoria (Cero Redis) vs Primitivas de Transporte
- **Estado:** Aceptado
- **Contexto:** Se requiere aplicar reglas dinámicas y variables de negocio sobre transacciones de alto throughput sin reiniciar el clúster Flink y sin degradar la latencia con llamadas síncronas a una base de datos externa como Redis.
- **Decisión:** Implementar el **Broadcast State Pattern** (`broadcast()`) conectando un stream de control (`config.rules`) al stream de movimientos enriquecidos mediante `BroadcastProcessFunction`.
- **Alternativas Evaluadas y Descartadas:**
  - *Redis / Cache externa en `map()`*: Descartada por incurrir en 1-3 ms de latencia de red por evento, riesgo de agotamiento de sockets y violación de determinismo en repeticiones de checkpoints.
  - *`keyBy(ruleId)`*: Descartada porque aislaría cada regla en un único slot por hash, dejando ciegos a los demás TaskManagers que procesan movimientos con otras llaves.
  - *`rebalance()` / `rescale()`*: Descartadas porque reparten las reglas en modo Round-Robin entre slots, provocando que los workers tengan conjuntos parciales e inconsistentes de reglas.
  - *Nodos Stateless (`map`, `filter`, `flatMap`)*: Descartados por carecer de estado persistente gestionado y temporizadores necesarios para enriquecimiento temporal.
- **Consecuencias:** Logra evaluación de reglas en nanosegundos en memoria RAM local; permite inyección de reglas en tiempo real por Kafka sin downtime; el estado de las reglas queda 100% respaldado en los checkpoints y savepoints en S3.
- **Referencia Técnica:** Ver [Sección 2.2 Subsección 4](#4-por-qué-broadcast-state-y-no-nodos-stateless-ni-otras-aristas-de-red-decisiones-de-diseño) y [Sección 3.6](#36-patrón-empresarial-enriquecimiento-multi-stream-asíncrono-reglas-dinámicas-broadcast-state-y-persistencia-de-estado).

---
*Mantenido por el Equipo de Arquitectura Empresarial y Plataforma de Streaming.*

