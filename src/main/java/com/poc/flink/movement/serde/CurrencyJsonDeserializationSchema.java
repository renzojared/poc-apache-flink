package com.poc.flink.movement.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.flink.movement.model.CurrencyEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes JSON strings from Kafka into CurrencyEvent domain models.
 */
public class CurrencyJsonDeserializationSchema implements DeserializationSchema<CurrencyEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(CurrencyJsonDeserializationSchema.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public CurrencyEvent deserialize(byte[] message) throws IOException {
        try {
            return OBJECT_MAPPER.readValue(message, CurrencyEvent.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize currency message: {}", new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(CurrencyEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CurrencyEvent> getProducedType() {
        return TypeInformation.of(CurrencyEvent.class);
    }
}
