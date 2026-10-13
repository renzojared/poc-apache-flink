package com.poc.flink.transaction.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.flink.transaction.model.TransactionEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Deserializes JSON strings from Kafka into TransactionEvent domain models.
 */
public class TransactionJsonDeserializationSchema implements DeserializationSchema<TransactionEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TransactionJsonDeserializationSchema.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public TransactionEvent deserialize(byte[] message) throws IOException {
        try {
            return OBJECT_MAPPER.readValue(message, TransactionEvent.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize transaction message: {}", new String(message), e);
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(TransactionEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TransactionEvent> getProducedType() {
        return TypeInformation.of(TransactionEvent.class);
    }
}
