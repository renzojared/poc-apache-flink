package com.poc.flink.movement.sink;

import com.poc.flink.movement.model.MovementAlertEvent;
import com.poc.flink.movement.serde.MovementAlertKafkaSerializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;

/**
 * Factory for creating Kafka Sinks dedicated to routing timed-out movement alerts.
 */
public final class MovementKafkaAlertSink {

    private MovementKafkaAlertSink() {
    }

    public static KafkaSink<MovementAlertEvent> create(String bootstrapServers, String alertsTopic) {
        return KafkaSink.<MovementAlertEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new MovementAlertKafkaSerializer(alertsTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }
}
