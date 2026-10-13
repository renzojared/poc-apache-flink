package com.poc.flink.movement.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * Domain model representing a fully enriched movement event (movement + currency + rule evaluation).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class EnrichedMovementEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String movementId;
    private String operation;
    private String user;
    private String account;
    private BigDecimal amount;
    private String currency;
    private String op;
    private String status;
    private String appliedRuleId;
    private long enrichmentTimestamp = Instant.now().toEpochMilli();

    public EnrichedMovementEvent() {
    }

    public EnrichedMovementEvent(String movementId, String operation, String user, String account,
                                 BigDecimal amount, String currency, String op, String status,
                                 String appliedRuleId, long enrichmentTimestamp) {
        this.movementId = movementId;
        this.operation = operation;
        this.user = user;
        this.account = account;
        this.amount = amount;
        this.currency = currency;
        this.op = op;
        this.status = status;
        this.appliedRuleId = appliedRuleId;
        this.enrichmentTimestamp = enrichmentTimestamp;
    }

    public static EnrichedMovementEvent from(MovementEvent movement, CurrencyEvent currency) {
        return new EnrichedMovementEvent(
                movement.getMovementId(),
                movement.getOperation(),
                movement.getUser(),
                movement.getAccount(),
                movement.getAmount(),
                currency.getCurrency(),
                movement.getOp(),
                "ENRICHED_PENDING_RULES",
                null,
                Instant.now().toEpochMilli()
        );
    }

    public String getMovementId() {
        return movementId;
    }

    public void setMovementId(String movementId) {
        this.movementId = movementId;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getAppliedRuleId() {
        return appliedRuleId;
    }

    public void setAppliedRuleId(String appliedRuleId) {
        this.appliedRuleId = appliedRuleId;
    }

    public long getEnrichmentTimestamp() {
        return enrichmentTimestamp;
    }

    public void setEnrichmentTimestamp(long enrichmentTimestamp) {
        this.enrichmentTimestamp = enrichmentTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EnrichedMovementEvent)) return false;
        EnrichedMovementEvent that = (EnrichedMovementEvent) o;
        return enrichmentTimestamp == that.enrichmentTimestamp &&
                Objects.equals(movementId, that.movementId) &&
                Objects.equals(operation, that.operation) &&
                Objects.equals(user, that.user) &&
                Objects.equals(account, that.account) &&
                Objects.equals(amount, that.amount) &&
                Objects.equals(currency, that.currency) &&
                Objects.equals(op, that.op) &&
                Objects.equals(status, that.status) &&
                Objects.equals(appliedRuleId, that.appliedRuleId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movementId, operation, user, account, amount, currency, op, status, appliedRuleId, enrichmentTimestamp);
    }

    @Override
    public String toString() {
        return "EnrichedMovementEvent{" +
                "movementId='" + movementId + '\'' +
                ", operation='" + operation + '\'' +
                ", user='" + user + '\'' +
                ", account='" + account + '\'' +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", op='" + op + '\'' +
                ", status='" + status + '\'' +
                ", appliedRuleId='" + appliedRuleId + '\'' +
                ", enrichmentTimestamp=" + enrichmentTimestamp +
                '}';
    }
}
