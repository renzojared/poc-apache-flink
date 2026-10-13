package com.poc.flink;

import com.poc.flink.config.JobConfig;
import com.poc.flink.movement.function.DynamicRuleBroadcastProcessFunction;
import com.poc.flink.movement.function.MovementCurrencyCoProcessFunction;
import com.poc.flink.movement.model.CurrencyEvent;
import com.poc.flink.movement.model.EnrichedMovementEvent;
import com.poc.flink.movement.model.MovementAlertEvent;
import com.poc.flink.movement.model.MovementEvent;
import com.poc.flink.movement.model.RuleConfigEvent;
import com.poc.flink.movement.serde.CurrencyJsonDeserializationSchema;
import com.poc.flink.movement.serde.MovementJsonDeserializationSchema;
import com.poc.flink.movement.serde.RuleConfigJsonDeserializationSchema;
import com.poc.flink.movement.sink.EnrichedMovementDynamoDbSink;
import com.poc.flink.movement.sink.MovementKafkaAlertSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Main entrypoint for the Apache Flink Streaming Pipeline with:
 * 1. Stream-to-Stream Asynchronous Join (Movements + Currencies) with non-blocking Timers.
 * 2. Side Output routing to Kafka Topic 'movements.alerts' on timeout.
 * 3. Dynamic Business Rules via Broadcast State (Kafka topic 'config.rules') with 0 Redis latency.
 * 4. Sinks to TaskManager Console and DynamoDB Ledger.
 */
public class StreamingJobApp {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJobApp.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Apache Flink PoC Pipeline (Multi-Stream Enrichment + Dynamic Kafka Rules)...");

        JobConfig config = new JobConfig();
        LOG.info("Configuration: Kafka={}, MovementsTopic={}, CurrenciesTopic={}, RulesTopic={}, AlertsTopic={}, Timeout={}ms",
                config.getKafkaBootstrapServers(),
                config.getMovementsTopic(),
                config.getCurrenciesTopic(),
                config.getRulesTopic(),
                config.getAlertsTopic(),
                config.getEnrichmentTimeoutMs());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Configure Exactly-Once Checkpointing
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        // 2. Kafka Source 1: Movements Stream (Table 1)
        KafkaSource<MovementEvent> movementsKafkaSource = KafkaSource.<MovementEvent>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getMovementsTopic())
                .setGroupId(config.getKafkaGroupId() + "-movements")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new MovementJsonDeserializationSchema())
                .build();

        DataStream<MovementEvent> movementsStream = env.fromSource(
                movementsKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "KafkaMovementsSource"
        ).filter(Objects::nonNull).name("FilterNullMovements");

        // 3. Kafka Source 2: Currencies Stream (Table 2)
        KafkaSource<CurrencyEvent> currenciesKafkaSource = KafkaSource.<CurrencyEvent>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getCurrenciesTopic())
                .setGroupId(config.getKafkaGroupId() + "-currencies")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CurrencyJsonDeserializationSchema())
                .build();

        DataStream<CurrencyEvent> currenciesStream = env.fromSource(
                currenciesKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "KafkaCurrenciesSource"
        ).filter(Objects::nonNull).name("FilterNullCurrencies");

        // 4. Kafka Source 3: Dynamic Business Rules Stream
        KafkaSource<RuleConfigEvent> rulesKafkaSource = KafkaSource.<RuleConfigEvent>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getRulesTopic())
                .setGroupId(config.getKafkaGroupId() + "-rules")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new RuleConfigJsonDeserializationSchema())
                .build();

        DataStream<RuleConfigEvent> rulesStream = env.fromSource(
                rulesKafkaSource,
                WatermarkStrategy.noWatermarks(),
                "KafkaRulesSource"
        ).filter(Objects::nonNull).name("FilterNullRules");

        // 5. Dual-Stream Asynchronous Join via KeyedCoProcessFunction
        SingleOutputStreamOperator<EnrichedMovementEvent> enrichedStream = movementsStream
                .keyBy(MovementEvent::getMovementId)
                .connect(currenciesStream.keyBy(CurrencyEvent::getMovementId))
                .process(new MovementCurrencyCoProcessFunction(config.getEnrichmentTimeoutMs()))
                .name("MovementCurrencyEnrichmentFunction");

        // 6. Side Output Branch: Timed-out movements routed to Kafka 'movements.alerts'
        DataStream<MovementAlertEvent> alertsStream = enrichedStream
                .getSideOutput(MovementCurrencyCoProcessFunction.TIMEOUT_ALERTS_TAG);

        alertsStream.print("TIMEOUT-ALERT").name("ConsoleAlertsPrintSink");

        alertsStream.sinkTo(
                MovementKafkaAlertSink.create(config.getKafkaBootstrapServers(), config.getAlertsTopic())
        ).name("KafkaAlertsSink");

        // 7. Dynamic Rules Evaluation via Broadcast State Pattern
        BroadcastStream<RuleConfigEvent> broadcastRules = rulesStream
                .broadcast(DynamicRuleBroadcastProcessFunction.RULES_STATE_DESCRIPTOR);

        SingleOutputStreamOperator<EnrichedMovementEvent> finalStream = enrichedStream
                .connect(broadcastRules)
                .process(new DynamicRuleBroadcastProcessFunction())
                .name("DynamicRuleBroadcastEvaluation");

        // 8. Outputs: Console Log Sink + DynamoDB Ledger Sink
        finalStream.print("ENRICHED-MOVEMENT").name("ConsoleEnrichedPrintSink");

        finalStream.sinkTo(
                EnrichedMovementDynamoDbSink.create(
                        config.getAwsEndpointUrl(),
                        config.getAwsRegion(),
                        "test",
                        "test",
                        config.getDynamoDbTableName()
                )
        ).name("DynamoDbEnrichedLedgerSink");

        LOG.info("Triggering continuous execution DAG...");
        env.execute("PoC-Apache-Flink-MultiStream-Pipeline");
    }
}
