package com.poc.flink.movement.function;

import com.poc.flink.movement.model.CurrencyEvent;
import com.poc.flink.movement.model.EnrichedMovementEvent;
import com.poc.flink.movement.model.MovementAlertEvent;
import com.poc.flink.movement.model.MovementEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;


/**
 * Asynchronously enriches movements with currency data from two independent Kafka streams.
 * <p>
 * Keyed by movementId. Handles out-of-order arrivals:
 * - If Currency arrives first: stored in currencyState.
 * - If Movement arrives first: buffered in pendingMovementsState and registers a non-blocking timer.
 * - When matched: emits EnrichedMovementEvent and cancels any scheduled timer.
 * - On timer expiration: routes pending movements to TIMEOUT_ALERTS_TAG Side Output.
 */
public class MovementCurrencyCoProcessFunction
        extends KeyedCoProcessFunction<String, MovementEvent, CurrencyEvent, EnrichedMovementEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MovementCurrencyCoProcessFunction.class);

    public static final OutputTag<MovementAlertEvent> TIMEOUT_ALERTS_TAG =
            new OutputTag<MovementAlertEvent>("movement-currency-timeout-alerts") {};

    private final long timeoutDurationMs;

    // Managed Keyed State
    private transient ValueState<CurrencyEvent> currencyState;
    private transient ListState<MovementEvent> pendingMovementsState;
    private transient ValueState<Long> timerState;

    public MovementCurrencyCoProcessFunction(long timeoutDurationMs) {
        this.timeoutDurationMs = timeoutDurationMs;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<CurrencyEvent> currencyDesc =
                new ValueStateDescriptor<>("currencyState", Types.POJO(CurrencyEvent.class));

        // State TTL: Evaporates orphaned currencies after 24h via RocksDB compaction filter
        StateTtlConfig currencyTtl = StateTtlConfig
                .newBuilder(Duration.ofHours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1000)
                .build();
        currencyDesc.enableTimeToLive(currencyTtl);

        currencyState = getRuntimeContext().getState(currencyDesc);

        ListStateDescriptor<MovementEvent> movementDesc =
                new ListStateDescriptor<>("pendingMovementsState", Types.POJO(MovementEvent.class));
        pendingMovementsState = getRuntimeContext().getListState(movementDesc);

        ValueStateDescriptor<Long> timerDesc =
                new ValueStateDescriptor<>("timerState", Types.LONG);
        timerState = getRuntimeContext().getState(timerDesc);
    }


    /**
     * Stream 1: MovementEvent (e.g. from table movements, filtered by op='u').
     */
    @Override
    public void processElement1(
            MovementEvent movement,
            Context ctx,
            Collector<EnrichedMovementEvent> out) throws Exception {

        String key = ctx.getCurrentKey();

        // Filter: only process update events as required by business logic
        if (!movement.isUpdate()) {
            LOG.info("Ignoring movement id={} with non-update op='{}'", movement.getMovementId(), movement.getOp());
            return;
        }

        CurrencyEvent cachedCurrency = currencyState.value();
        if (cachedCurrency != null) {
            // Happy path 1: Currency arrived earlier. Enrich immediately!
            LOG.info("Immediate enrichment for movementId={}: currency '{}' was already in state",
                    key, cachedCurrency.getCurrency());
            out.collect(EnrichedMovementEvent.from(movement, cachedCurrency));
        } else {
            // Path 2: Currency has not arrived yet. Buffer movement and register non-blocking timer.
            LOG.info("Currency missing for movementId={}. Buffering movement and registering timer for {}ms",
                    key, timeoutDurationMs);
            pendingMovementsState.add(movement);

            Long existingTimer = timerState.value();
            if (existingTimer == null) {
                long timerTimestamp = ctx.timerService().currentProcessingTime() + timeoutDurationMs;
                ctx.timerService().registerProcessingTimeTimer(timerTimestamp);
                timerState.update(timerTimestamp);
                LOG.info("Registered processing time timer for movementId={} at timestamp={}", key, timerTimestamp);
            }
        }
    }

    /**
     * Stream 2: CurrencyEvent (e.g. from table currencies).
     */
    @Override
    public void processElement2(
            CurrencyEvent currency,
            Context ctx,
            Collector<EnrichedMovementEvent> out) throws Exception {

        String key = ctx.getCurrentKey();
        LOG.info("Received currency '{}' for movementId={}", currency.getCurrency(), key);

        // Store currency in managed state for this movementId
        currencyState.update(currency);

        // Drain any pending movements that arrived earlier waiting for this currency
        Iterable<MovementEvent> pending = pendingMovementsState.get();
        if (pending != null) {
            List<MovementEvent> movementsToEmit = new ArrayList<>();
            for (MovementEvent m : pending) {
                movementsToEmit.add(m);
            }

            if (!movementsToEmit.isEmpty()) {
                LOG.info("Draining {} buffered movements for movementId={} now that currency has arrived",
                        movementsToEmit.size(), key);
                for (MovementEvent m : movementsToEmit) {
                    out.collect(EnrichedMovementEvent.from(m, currency));
                }
                pendingMovementsState.clear();

                // Cancel the pending timer since the join is now complete
                Long scheduledTimer = timerState.value();
                if (scheduledTimer != null) {
                    ctx.timerService().deleteProcessingTimeTimer(scheduledTimer);
                    timerState.clear();
                    LOG.info("Cancelled processing time timer at {} for movementId={}", scheduledTimer, key);
                }
            }
        }
    }

    /**
     * Flink Timer Callback: Invoked ONLY when processing time reaches the scheduled timestamp.
     * Non-blocking: this does NOT pause or block other keys or worker threads.
     */
    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<EnrichedMovementEvent> out) throws Exception {

        String key = ctx.getCurrentKey();
        LOG.warn("Timer fired at {} for movementId={}. Checking if currency arrived...", timestamp, key);

        CurrencyEvent cachedCurrency = currencyState.value();
        if (cachedCurrency == null) {
            // Currency never arrived! Divert pending movements to Side Output for alerting / dead-letter
            Iterable<MovementEvent> pending = pendingMovementsState.get();
            if (pending != null) {
                for (MovementEvent movement : pending) {
                    LOG.error("TIMEOUT: Currency never arrived for movementId={}. Diverting to side output.", key);
                    MovementAlertEvent alert = MovementAlertEvent.timeout(movement, timeoutDurationMs);
                    ctx.output(TIMEOUT_ALERTS_TAG, alert);
                }
                pendingMovementsState.clear();
            }
        }

        // Clean up timer state
        timerState.clear();
    }
}
