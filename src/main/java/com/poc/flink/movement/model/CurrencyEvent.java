package com.poc.flink.movement.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Domain model representing a currency mapping event from the secondary table.
 * Joined with MovementEvent using movementId.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CurrencyEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonAlias({"movement", "movement_id"})
    private String movementId;

    @JsonAlias({"moneda", "currency_code"})
    private String currency;

    private long timestamp = Instant.now().toEpochMilli();

    public CurrencyEvent() {
    }

    public CurrencyEvent(String movementId, String currency, long timestamp) {
        this.movementId = movementId;
        this.currency = currency;
        this.timestamp = timestamp;
    }

    public String getMovementId() {
        return movementId;
    }

    public void setMovementId(String movementId) {
        this.movementId = movementId;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
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
        if (!(o instanceof CurrencyEvent)) return false;
        CurrencyEvent that = (CurrencyEvent) o;
        return timestamp == that.timestamp &&
                Objects.equals(movementId, that.movementId) &&
                Objects.equals(currency, that.currency);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movementId, currency, timestamp);
    }

    @Override
    public String toString() {
        return "CurrencyEvent{" +
                "movementId='" + movementId + '\'' +
                ", currency='" + currency + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
