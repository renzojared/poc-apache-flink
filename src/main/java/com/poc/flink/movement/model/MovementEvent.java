package com.poc.flink.movement.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * Domain model representing a financial movement event from the movements table.
 * Adheres strictly to Flink POJO guidelines (public class, default constructor, public getters/setters).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MovementEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonAlias({"movement", "movement_id"})
    private String movementId;

    @JsonAlias({"operation_id"})
    private String operation;

    @JsonAlias({"user_id"})
    private String user;

    private String account;
    private BigDecimal amount;

    @JsonAlias({"operation_type", "op_type"})
    private String op = "u";

    private long timestamp = Instant.now().toEpochMilli();

    public MovementEvent() {
    }

    public MovementEvent(String movementId, String operation, String user, String account, BigDecimal amount, String op, long timestamp) {
        this.movementId = movementId;
        this.operation = operation;
        this.user = user;
        this.account = account;
        this.amount = amount;
        this.op = op;
        this.timestamp = timestamp;
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isUpdate() {
        return op == null || "u".equalsIgnoreCase(op) || "update".equalsIgnoreCase(op);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MovementEvent)) return false;
        MovementEvent that = (MovementEvent) o;
        return timestamp == that.timestamp &&
                Objects.equals(movementId, that.movementId) &&
                Objects.equals(operation, that.operation) &&
                Objects.equals(user, that.user) &&
                Objects.equals(account, that.account) &&
                Objects.equals(amount, that.amount) &&
                Objects.equals(op, that.op);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movementId, operation, user, account, amount, op, timestamp);
    }

    @Override
    public String toString() {
        return "MovementEvent{" +
                "movementId='" + movementId + '\'' +
                ", operation='" + operation + '\'' +
                ", user='" + user + '\'' +
                ", account='" + account + '\'' +
                ", amount=" + amount +
                ", op='" + op + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
