package com.poc.flink.movement.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.flink.movement.model.RuleConfigEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes JSON strings from Kafka into RuleConfigEvent domain models.
 */
public class RuleConfigJsonDeserializationSchema implements DeserializationSchema<RuleConfigEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RuleConfigJsonDeserializationSchema.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public RuleConfigEvent deserialize(byte[] message) throws IOException {
        try {
            return OBJECT_MAPPER.readValue(message, RuleConfigEvent.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize rule configuration message: {}", new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(RuleConfigEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RuleConfigEvent> getProducedType() {
        return TypeInformation.of(RuleConfigEvent.class);
    }
}
