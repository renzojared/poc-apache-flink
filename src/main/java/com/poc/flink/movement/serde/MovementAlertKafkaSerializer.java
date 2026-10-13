package com.poc.flink.movement.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.flink.movement.model.MovementAlertEvent;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Serializes MovementAlertEvent domain objects into Kafka ProducerRecords targeting the alerts topic.
 */
public class MovementAlertKafkaSerializer implements KafkaRecordSerializationSchema<MovementAlertEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MovementAlertKafkaSerializer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String topic;

    public MovementAlertKafkaSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            MovementAlertEvent element,
            KafkaSinkContext context,
            Long timestamp) {
        try {
            byte[] key = element.getMovementId() != null
                    ? element.getMovementId().getBytes(StandardCharsets.UTF_8)
                    : null;
            byte[] value = OBJECT_MAPPER.writeValueAsBytes(element);
            return new ProducerRecord<>(topic, key, value);
        } catch (Exception e) {
            LOG.error("Failed to serialize MovementAlertEvent for movementId: {}", element.getMovementId(), e);
            return null;
        }
    }
}
