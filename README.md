# PoC Apache Flink — Streaming Engine

Un arquetipo de grado de producción para procesamiento distribuido de flujos con estado (*Stateful Stream Processing*) utilizando **Apache Flink 2.0.2**, **Apache Kafka (KRaft)**, e **Infraestructura como Código (Terraform)** emulada localmente con **Floci (AWS)**.

---

## 🏛️ Topología de Streaming & Arquitectura

El pipeline implementa una **Arquitectura Vertical (Screaming Architecture)** con:
1. **Join Asíncrono de 2 Streams** (`movements.incoming` + `movements.currencies`) con temporizadores no bloqueantes en RocksDB.
2. **Side Output a Kafka** (`movements.alerts`) ante expiración de temporizador si nunca llega la moneda.
3. **Reglas Dinámicas en Memoria (Cero Redis)** mediante el patrón **Broadcast State** desde Kafka (`config.rules`).
4. **Persistencia Idempotente** en DynamoDB Ledger y consola de TaskManager.

```text
 [Tabla 1: Movimientos] ──► movements.incoming ──┐
                                                 ├──► keyBy(movementId) ──► [KeyedCoProcessFunction]
 [Tabla 2: Monedas]     ──► movements.currencies ┘                           │ (Memoria RocksDB + Timers)
                                                                             ├──► [Side Output: Timeout] ──► movements.alerts (Kafka DLQ)
                                                                             │
 [Tabla Reglas Negocio] ──► config.rules ────────► broadcast() ──────────────┴──► [BroadcastProcessFunction]
                                                                                   │ (Evaluación RAM sin Redis)
                                                                                   ▼
                                                                     [DynamoDB Ledger + TaskManager Log]
```

### Estructura del Repositorio

```text
poc-apache-flink/
├── docker/
│   └── docker-compose.yml       # Stack: JobManager, TaskManager, Kafka, kafka-init, Floci, DynamoDB Admin
├── iac/
│   └── envs/dev/                # Terraform: S3 (checkpoints) y DynamoDB (ledger) sobre Floci
├── scripts/
│   └── kafka_injector.py        # Inyector Python: Servidor HTTP REST y CLI de pruebas automatizadas
├── src/main/java/com/poc/flink/
│   ├── StreamingJobApp.java     # Entrypoint del Job: composición declarativa del DAG
│   ├── config/
│   │   └── JobConfig.java       # Resolución de variables de entorno y parámetros
│   └── movement/                # Vertical Slice de Negocio
│       ├── function/
│       │   ├── MovementCurrencyCoProcessFunction.java # Join multi-tabla con temporizador y Side Output
│       │   └── DynamicRuleBroadcastProcessFunction.java # Reglas dinámicas en memoria (Broadcast State)
│       ├── model/               # POJOs de dominio (MovementEvent, CurrencyEvent, RuleConfigEvent, etc.)
│       ├── serde/               # Deserializadores y serializadores JSON para Kafka
│       └── sink/                # Sinks para DynamoDB y Side Output a Kafka
├── src/main/resources/
│   └── logback.xml              # Configuración de logs para desarrollo y ejecución
├── deploy.sh                    # Script de compilación y despliegue continuo en 1 comando
├── mvnw / mvnw.cmd              # Maven Wrapper (compatible con Java 21 LTS y Java 27)
└── pom.xml                      # Dependencias, Maven Shade Plugin y empaquetado Fat JAR
```

---

## 📋 Requisitos Previos y Verificación de Entorno

Para clonar y ejecutar este repositorio sin asumir nada en tu máquina, asegúrate de contar con las siguientes herramientas instaladas:

1. **Docker & Docker Compose (v2.0+)**:
   - Requerido para levantar Kafka, Flink (JobManager / TaskManager), Floci (emulador de S3 y DynamoDB), Kafka UI y DynamoDB Admin.
   - *Comando de verificación:* `docker --version && docker compose version`
2. **Java JDK 21 LTS o superior**:
   - Requerido para compilar el proyecto localmente o depurar en tu IDE (IntelliJ IDEA o VS Code).
   - *Comando de verificación:* `java -version`
   - *Nota de Maven:* **No necesitas instalar Maven**. El repositorio incluye el Maven Wrapper oficial (`./mvnw`) que descarga automáticamente la versión compatible.
3. **Terraform CLI (1.5.0+)**:
   - Requerido para aprovisionar el bucket de S3 (checkpoints/savepoints) y la tabla DynamoDB en Floci local.
   - *Comando de verificación:* `terraform -version`
4. **Python 3.10+ y pip**:
   - Requerido para ejecutar el emulador de eventos e inyector de reglas dinámicas (`scripts/kafka_injector.py`).
   - *Comando de verificación:* `python3 --version && python3 -m pip --version`
5. **Puertos Libres en la Máquina Host**:
   - Verifica que los siguientes puertos no estén ocupados por otros servicios locales:
     - `8081`: Flink Web Dashboard
     - `8085`: Kafka UI Dashboard
     - `8001`: DynamoDB Admin Web UI
     - `9092`: Apache Kafka Broker (KRaft)
     - `4566`: Floci AWS Emulator (S3 & DynamoDB)
     - `8080`: Servidor REST del inyector de pruebas (opcional)

---

## 🚀 Guía Paso a Paso: Ejecución Local en Clúster Docker

Sigue estos 4 pasos en orden estricto para tener el pipeline operando de punta a punta:

### Paso 1: Levantar la Infraestructura Local
Levanta Kafka, Floci, la inicialización automática de topics y los nodos de Flink:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Verifica que todos los contenedores estén saludables:
```bash
docker compose -f docker/docker-compose.yml ps
```

| Servicio | URL / Puerto | Rol en la Arquitectura |
|---|---|---|
| **Flink Dashboard** | [http://localhost:8081](http://localhost:8081) | Métricas, DAG interactivo, slots, excepciones y checkpoints |
| **Kafka UI Dashboard** | [http://localhost:8085](http://localhost:8085) | Monitor de topics, inspección de mensajes en vivo y alertas DLQ |
| **DynamoDB Admin UI** | [http://localhost:8001](http://localhost:8001) | Explorador visual de tablas y registros en DynamoDB |
| **Floci (AWS)** | `http://localhost:4566` | Emulador local de S3 (checkpoints) y DynamoDB |
| **Kafka Broker** | `localhost:9092` | Broker KRaft para ingesta y emisión de eventos |

### Paso 2: Aprovisionar Recursos Cloud con Terraform (Floci)
Crea el bucket S3 de almacenamiento de checkpoints y la tabla DynamoDB:

```bash
cd iac/envs/dev
terraform init
terraform apply -auto-approve
cd ../../..
```

### Paso 3: Compilar y Desplegar el Job de Flink

Tienes dos modos de despliegue según el objetivo:

```bash
# Modo A: Despliegue en frío (Cold Start - reinicia estado desde cero)
./deploy.sh

# Modo B: Despliegue con Cero Pérdida de Datos (Zero-Data-Loss con Savepoint a S3)
./deploy.sh --savepoint
```

El script se encarga de:
1. Si usas `--savepoint`: Congela todo el estado (`BroadcastState`, monedas, buffers de movimientos y timers) en **AWS S3** y detiene el job anterior de forma síncrona. Si no, realiza una cancelación limpia.
2. Verificar que los topics de Kafka existan.
3. Compilar el Fat JAR con `./mvnw clean package -DskipTests`.
4. Copiar el JAR al JobManager y someterlo en segundo plano (`-d`), restaurando automáticamente el estado desde el Savepoint en S3 si aplica.
5. El job quedará en estado **`RUNNING`** 24/7 en [http://localhost:8081](http://localhost:8081).

### Paso 4: Ejecutar Pruebas con el Inyector Python
El script emulador simula la llegada de eventos desde Kafka. Instala sus dependencias usando el archivo estándar `requirements.txt`:

```bash
# 1. Crear el entorno virtual en la raíz del proyecto (solo la primera vez)
python3 -m venv .venv

# 2. Activar el entorno virtual e instalar dependencias
source .venv/bin/activate       # En macOS / Linux (o .venv\Scripts\activate en Windows)
pip install -r requirements.txt

# 3. Ejecutar los escenarios de prueba:

# Opción A: Probar Escenario 1 (Moneda llega antes del Movimiento -> Enriquecimiento inmediato)
python scripts/kafka_injector.py --test-currency-first

# Opción B: Probar Escenario 2 (Movimiento llega antes -> Buffering en RocksDB, registro de timer y cancelación)
python scripts/kafka_injector.py --test-movement-first

# Opción C: Probar Escenario 3 (Movimiento huérfano sin moneda -> Vence timer de 30s y desvía a Side Output)
python scripts/kafka_injector.py --test-timeout

# Opción D: Inyectar Regla Dinámica en Caliente (Broadcast State a la RAM de todos los TaskManagers)
python scripts/kafka_injector.py --test-rule
```

---

## 🛠️ Modo Desarrollo Local: Debuggeo con Breakpoints (Inner Loop)

Si estás iterando lógica de negocio y no quieres empaquetar Docker en cada cambio, puedes ejecutar Flink directamente en tu IDE (IntelliJ IDEA o VS Code) usando un **MiniCluster embebido**:

### 1. Variables de Entorno para Ejecución Local (IDE)
Cuando Flink corre dentro de tu IDE, se conecta a los puertos de tu máquina host:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
AWS_ENDPOINT_URL=http://localhost:4566
AWS_REGION=us-east-1
DYNAMODB_TABLE_NAME=poc-apache-flink-ledger-dev
```

### 2. Cómo Iniciar en tu IDE (IntelliJ IDEA o VS Code)
- **En IntelliJ IDEA:**
  El repositorio ya incluye la configuración compartida en [.run/StreamingJobApp.run.xml](.run/StreamingJobApp.run.xml). Simplemente abre el proyecto, selecciona **`StreamingJobApp (Local MiniCluster)`** en el selector superior de ejecución y haz clic en **Debug**.
- **En VS Code:**
  El repositorio ya incluye [.vscode/launch.json](.vscode/launch.json). Ve a la pestaña **Run and Debug** (o presiona `F5`) y selecciona **`StreamingJobApp (Local MiniCluster)`**.
- **Arranque ultra rápido:** El MiniCluster embebido de Flink arranca en ~2 segundos escuchando en `localhost:9092` y guardando en Floci (`localhost:4566`).

### 3. Puntos Clave para Colocar Breakpoints
- **Llegada de Movimiento:** Línea de entrada de `processElement1` en [MovementCurrencyCoProcessFunction.java](src/main/java/com/poc/flink/movement/function/MovementCurrencyCoProcessFunction.java).
- **Llegada de Moneda:** Línea de entrada de `processElement2` en [MovementCurrencyCoProcessFunction.java](src/main/java/com/poc/flink/movement/function/MovementCurrencyCoProcessFunction.java).
- **Expiración del Temporizador:** Método `onTimer()` en [MovementCurrencyCoProcessFunction.java](src/main/java/com/poc/flink/movement/function/MovementCurrencyCoProcessFunction.java).
- **Llegada de Regla de Negocio:** `processBroadcastElement()` en [DynamicRuleBroadcastProcessFunction.java](src/main/java/com/poc/flink/movement/function/DynamicRuleBroadcastProcessFunction.java).
- **Evaluación de la Regla:** `processElement()` en [DynamicRuleBroadcastProcessFunction.java](src/main/java/com/poc/flink/movement/function/DynamicRuleBroadcastProcessFunction.java).

---

## 🧪 Herramienta de Pruebas: Python Kafka Injector (`scripts/kafka_injector.py`)

El script `scripts/kafka_injector.py` actúa como emulador de eventos de negocio tanto vía CLI como mediante una API REST.

### Modo Servidor HTTP REST (para Postman o cURL)
Inicia el servidor HTTP:
```bash
.venv/bin/python scripts/kafka_injector.py --server --port 8080
```

Endpoints disponibles:

1. **Inyectar Regla de Negocio Dinámica (`POST /config`):**
   ```bash
   curl -X POST http://localhost:8080/config \
     -H "Content-Type: application/json" \
     -d '{
       "ruleId": "RULE_STRICT_AML",
       "allowedCurrencies": ["PEN", "USD"],
       "minAmount": 100,
       "maxAmount": 1000000,
       "active": true,
       "description": "Solo PEN y USD permitidos hasta 1M"
     }'
   ```

2. **Inyectar Movimiento Financiero (`POST /movement`):**
   ```bash
   curl -X POST http://localhost:8080/movement \
     -H "Content-Type: application/json" \
     -d '{
       "movement": "MOV-5001",
       "operation": "3747531388",
       "user": "586305",
       "account": "CASH",
       "amount": "-700000",
       "op": "u"
     }'
   ```

3. **Inyectar Moneda Asociada (`POST /currency`):**
   ```bash
   curl -X POST http://localhost:8080/currency \
     -H "Content-Type: application/json" \
     -d '{
       "movement": "MOV-5001",
       "currency": "PEN"
     }'
   ```

4. **Escuchar el Side Output de Alertas en Tiempo Real:**
   ```bash
   .venv/bin/python scripts/kafka_injector.py --consume-alerts
   ```

---

## 🔍 Guía de Debugging & Consideraciones de Operación (Troubleshooting)

Cuando depures un pipeline distribuido en Apache Flink, ten en cuenta las siguientes consideraciones arquitecturales:

### 1. ¿Dónde mirar los Logs? (JobManager vs TaskManager)

> [!IMPORTANT]
> **El código de tus funciones de negocio corre en el TaskManager, NO en el JobManager.**
> Si agregas un `LOG.info(...)` o `System.out.println` dentro de tu `ProcessFunction`, jamás aparecerá en los logs del JobManager.

- **Logs del TaskManager (Procesamiento de datos y Sinks):**
  ```bash
  docker logs -f poc-flink-taskmanager --tail 100
  ```
  *Qué buscar aquí:* Impresiones de eventos (`ENRICHED-MOVEMENT>`), logs de enrichments, registros de timers y evaluaciones de reglas.

- **Logs del JobManager (Coordinación y ciclo de vida):**
  ```bash
  docker logs -f poc-flink-jobmanager --tail 100
  ```
  *Qué buscar aquí:* Errores al arrancar el job, fallos en checkpoints de S3, problemas de asignación de slots o excepciones en el `SourceCoordinator`.

---

### 2. Qué inspeccionar en el Flink Dashboard ([http://localhost:8081](http://localhost:8081))

1. **Job State (`RUNNING` vs `RESTARTING` vs `FAILED`):**
   - Si el job está en `RESTARTING`, significa que una tarea falló y Flink está intentando recuperarse desde el último checkpoint.
2. **Pestaña "Exceptions":**
   - Es el primer lugar donde mirar ante un fallo. Muestra el stacktrace con la causa raíz (`Root Exception`).
3. **Pestaña "Checkpoints":**
   - Verifica que los checkpoints se completen con éxito (`Duration`, `Checkpointed Data Size`).
   - Si los checkpoints fallan continuamente, el JobManager eventualmente cancelará el job.
4. **Pestaña "Backpressure":**
   - Muestra el estado de cada operador (Verde = OK, Rojo = Bloqueado/Saturado).
   - Si el operador de DynamoDB está en rojo, significa que DynamoDB está aplicando rate limiting (throttling) y frenando el pipeline aguas arriba.

---

### 3. Matriz de Gotchas y Problemas Comunes

| Síntoma | Causa Raíz Arquitectural | Solución / Consideración |
|---|---|---|
| `Job State: RESTARTING` con `UnknownTopicOrPartitionException` | Uno o más topics de Kafka suscritos no existen en el broker. | El contenedor `kafka-init` en `docker-compose.yml` los crea automáticamente. Si corres en frío manual, usa `./deploy.sh` o crea los topics antes. |
| El evento se envía pero no aparece en DynamoDB ni en consola | El filtro de negocio descarta eventos que no sean updates (`op != 'u'`), o el evento no encontró moneda y está esperando en memoria. | Verifica que el JSON del movimiento incluya `"op": "u"`. Si no tiene moneda, espera 30s al vencimiento del temporizador. |
| El temporizador no se dispara inmediatamente | Los temporizadores de Flink (`TimerService`) operan por tiempo de procesamiento o tiempo de evento, no son inmediatos. | El timeout por defecto es de 30 segundos (`ENRICHMENT_TIMEOUT_MS=30000`). Deja pasar 30s para observar la emisión al Side Output en `movements.alerts`. |
| Conexión rechazada a Kafka (`Connection refused`) | Confusión entre el host de Kafka en el host vs la red de Docker. | Desde tu Mac / IDE usa `localhost:9092`. Dentro de los contenedores de Docker (JobManager/TaskManager) usa `kafka:19092`. |
| Excepción de serialización Kryo en producción | La clase del evento no cumple con la especificación de Flink POJO. | Asegúrate de que las clases en `model/` tengan constructor público vacío y getters/setters públicos para aprovechar el `PojoSerializer` nativo. |

---

## 🛑 Limpieza y Apagado

Para detener y limpiar todos los contenedores y volúmenes locales:

```bash
docker compose -f docker/docker-compose.yml down -v
```

---
*Para profundizar en los fundamentos distribuidos, revisa la [Guía de Arquitectura Empresarial](docs/apache-flink-kafka-architecture-guide.md).*