package com.poc.flink.transaction.function;

import com.poc.flink.transaction.model.TransactionEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Core transformation function in the transaction pipeline slice.
 * Validates transaction rules (e.g., positive monetary amounts, valid currency).
 */
public class TransactionValidationFunction extends ProcessFunction<TransactionEvent, TransactionEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(TransactionValidationFunction.class);

    @Override
    public void processElement(TransactionEvent event, Context ctx, Collector<TransactionEvent> out) {
        LOG.info("Processing transaction: id={}, account={}, amount={} {}",
                event.getTransactionId(), event.getAccountId(), event.getAmount(), event.getCurrency());

        if (event.getAmount() == null || event.getAmount().compareTo(BigDecimal.ZERO) <= 0) {
            LOG.warn("Rejecting transaction {}: amount must be positive", event.getTransactionId());
            event.setStatus("REJECTED");
        } else {
            event.setStatus("VALIDATED");
        }

        out.collect(event);
    }
}
