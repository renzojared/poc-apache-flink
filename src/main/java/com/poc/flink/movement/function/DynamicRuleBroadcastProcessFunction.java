package com.poc.flink.movement.function;

import com.poc.flink.movement.model.EnrichedMovementEvent;
import com.poc.flink.movement.model.RuleConfigEvent;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Evaluates dynamically injected business rules on the enriched movements stream.
 * Rules arrive via Kafka and are broadcast to all TaskManagers, storing them in local
 * memory via Broadcast State to achieve nanosecond evaluations without Redis network latency.
 */
public class DynamicRuleBroadcastProcessFunction
        extends BroadcastProcessFunction<EnrichedMovementEvent, RuleConfigEvent, EnrichedMovementEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DynamicRuleBroadcastProcessFunction.class);

    public static final MapStateDescriptor<String, RuleConfigEvent> RULES_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("RulesBroadcastState", Types.STRING, Types.POJO(RuleConfigEvent.class));

    @Override
    public void processBroadcastElement(
            RuleConfigEvent rule,
            Context ctx,
            Collector<EnrichedMovementEvent> out) throws Exception {

        if (rule.getRuleId() == null) {
            LOG.warn("Received rule config without ruleId. Ignoring.");
            return;
        }

        if (rule.isActive()) {
            ctx.getBroadcastState(RULES_STATE_DESCRIPTOR).put(rule.getRuleId(), rule);
            LOG.info("DYNAMIC RULE ACTIVATED in local broadcast state: id='{}', allowedCurrencies={}, min={}, max={}, desc='{}'",
                    rule.getRuleId(), rule.getAllowedCurrencies(), rule.getMinAmount(), rule.getMaxAmount(), rule.getDescription());
        } else {
            ctx.getBroadcastState(RULES_STATE_DESCRIPTOR).remove(rule.getRuleId());
            LOG.info("DYNAMIC RULE DEACTIVATED from local broadcast state: id='{}'", rule.getRuleId());
        }
    }

    @Override
    public void processElement(
            EnrichedMovementEvent event,
            ReadOnlyContext ctx,
            Collector<EnrichedMovementEvent> out) throws Exception {

        ReadOnlyBroadcastState<String, RuleConfigEvent> rules = ctx.getBroadcastState(RULES_STATE_DESCRIPTOR);

        boolean ruleViolated = false;
        String violatingRuleId = null;
        String lastAppliedRuleId = null;

        for (Map.Entry<String, RuleConfigEvent> entry : rules.immutableEntries()) {
            RuleConfigEvent rule = entry.getValue();
            if (!rule.isActive()) {
                continue;
            }

            lastAppliedRuleId = rule.getRuleId();

            // 1. Currency validation
            if (rule.getAllowedCurrencies() != null && !rule.getAllowedCurrencies().isEmpty()) {
                if (event.getCurrency() == null || !rule.getAllowedCurrencies().contains(event.getCurrency())) {
                    LOG.warn("Rule '{}' VIOLATED for movementId={}: currency '{}' not in allowed list {}",
                            rule.getRuleId(), event.getMovementId(), event.getCurrency(), rule.getAllowedCurrencies());
                    ruleViolated = true;
                    violatingRuleId = rule.getRuleId();
                    break;
                }
            }

            // 2. Amount limits validation (evaluated on absolute amount)
            if (event.getAmount() != null) {
                BigDecimal absAmount = event.getAmount().abs();

                if (rule.getMinAmount() != null && absAmount.compareTo(rule.getMinAmount()) < 0) {
                    LOG.warn("Rule '{}' VIOLATED for movementId={}: amount {} below min {}",
                            rule.getRuleId(), event.getMovementId(), absAmount, rule.getMinAmount());
                    ruleViolated = true;
                    violatingRuleId = rule.getRuleId();
                    break;
                }

                if (rule.getMaxAmount() != null && absAmount.compareTo(rule.getMaxAmount()) > 0) {
                    LOG.warn("Rule '{}' VIOLATED for movementId={}: amount {} exceeds max {}",
                            rule.getRuleId(), event.getMovementId(), absAmount, rule.getMaxAmount());
                    ruleViolated = true;
                    violatingRuleId = rule.getRuleId();
                    break;
                }
            }
        }

        if (ruleViolated) {
            event.setStatus("RULE_REJECTED");
            event.setAppliedRuleId(violatingRuleId);
        } else {
            event.setStatus("VALIDATED");
            event.setAppliedRuleId(lastAppliedRuleId);
        }

        LOG.info("Movement evaluated: id={}, status={}, currency={}, amount={}, rule={}",
                event.getMovementId(), event.getStatus(), event.getCurrency(), event.getAmount(), event.getAppliedRuleId());

        out.collect(event);
    }
}
