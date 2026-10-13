package com.poc.flink.movement.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * Domain model representing an alert event emitted via Side Output when a movement
 * timer expires without having received its corresponding currency event.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MovementAlertEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String movementId;
    private String operation;
    private String user;
    private String account;
    private BigDecimal amount;
    private String op;
    private String reason;
    private long timeoutDurationMs;
    private long alertTimestamp = Instant.now().toEpochMilli();

    public MovementAlertEvent() {
    }

    public MovementAlertEvent(String movementId, String operation, String user, String account,
                              BigDecimal amount, String op, String reason, long timeoutDurationMs,
                              long alertTimestamp) {
        this.movementId = movementId;
        this.operation = operation;
        this.user = user;
        this.account = account;
        this.amount = amount;
        this.op = op;
        this.reason = reason;
        this.timeoutDurationMs = timeoutDurationMs;
        this.alertTimestamp = alertTimestamp;
    }

    public static MovementAlertEvent timeout(MovementEvent movement, long timeoutDurationMs) {
        return new MovementAlertEvent(
                movement.getMovementId(),
                movement.getOperation(),
                movement.getUser(),
                movement.getAccount(),
                movement.getAmount(),
                movement.getOp(),
                "CURRENCY_ENRICHMENT_TIMEOUT_EXPIRED",
                timeoutDurationMs,
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

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public long getTimeoutDurationMs() {
        return timeoutDurationMs;
    }

    public void setTimeoutDurationMs(long timeoutDurationMs) {
        this.timeoutDurationMs = timeoutDurationMs;
    }

    public long getAlertTimestamp() {
        return alertTimestamp;
    }

    public void setAlertTimestamp(long alertTimestamp) {
        this.alertTimestamp = alertTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MovementAlertEvent)) return false;
        MovementAlertEvent that = (MovementAlertEvent) o;
        return timeoutDurationMs == that.timeoutDurationMs &&
                alertTimestamp == that.alertTimestamp &&
                Objects.equals(movementId, that.movementId) &&
                Objects.equals(operation, that.operation) &&
                Objects.equals(user, that.user) &&
                Objects.equals(account, that.account) &&
                Objects.equals(amount, that.amount) &&
                Objects.equals(op, that.op) &&
                Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movementId, operation, user, account, amount, op, reason, timeoutDurationMs, alertTimestamp);
    }

    @Override
    public String toString() {
        return "MovementAlertEvent{" +
                "movementId='" + movementId + '\'' +
                ", operation='" + operation + '\'' +
                ", user='" + user + '\'' +
                ", account='" + account + '\'' +
                ", amount=" + amount +
                ", op='" + op + '\'' +
                ", reason='" + reason + '\'' +
                ", timeoutDurationMs=" + timeoutDurationMs +
                ", alertTimestamp=" + alertTimestamp +
                '}';
    }
}
