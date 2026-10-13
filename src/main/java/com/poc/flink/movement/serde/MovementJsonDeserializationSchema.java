package com.poc.flink.movement.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.flink.movement.model.MovementEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes JSON strings from Kafka into MovementEvent domain models.
 */
public class MovementJsonDeserializationSchema implements DeserializationSchema<MovementEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MovementJsonDeserializationSchema.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public MovementEvent deserialize(byte[] message) throws IOException {
        try {
            return OBJECT_MAPPER.readValue(message, MovementEvent.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize movement message: {}", new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(MovementEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<MovementEvent> getProducedType() {
        return TypeInformation.of(MovementEvent.class);
    }
}
