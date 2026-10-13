package com.poc.flink.transaction.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * Domain model representing a financial transaction event.
 * Follows Flink POJO requirements (public class, default constructor, public getters/setters)
 * to ensure optimal serialization performance with Flink's PojoSerializer.
 */
public class TransactionEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String transactionId;
    private String accountId;
    private BigDecimal amount;
    private String currency;
    private String status;
    private long timestamp;

    public TransactionEvent() {
    }

    public TransactionEvent(String transactionId, String accountId, BigDecimal amount, String currency, String status, long timestamp) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.timestamp = timestamp;
    }

    public static TransactionEvent sample(String id, String account, double amount, String currency) {
        return new TransactionEvent(id, account, BigDecimal.valueOf(amount), currency, "PENDING", Instant.now().toEpochMilli());
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransactionEvent)) return false;
        TransactionEvent that = (TransactionEvent) o;
        return timestamp == that.timestamp &&
                Objects.equals(transactionId, that.transactionId) &&
                Objects.equals(accountId, that.accountId) &&
                Objects.equals(amount, that.amount) &&
                Objects.equals(currency, that.currency) &&
                Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionId, accountId, amount, currency, status, timestamp);
    }

    @Override
    public String toString() {
        return "TransactionEvent{" +
                "transactionId='" + transactionId + '\'' +
                ", accountId='" + accountId + '\'' +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", status='" + status + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
