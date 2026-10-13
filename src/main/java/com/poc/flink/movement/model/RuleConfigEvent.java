package com.poc.flink.movement.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Domain model representing dynamic business rules injected via Kafka.
 * Broadcast across all TaskManagers using Flink Broadcast State Pattern,
 * completely replacing external calls to Redis.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RuleConfigEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String ruleId;
    private List<String> allowedCurrencies = new ArrayList<>();
    private BigDecimal minAmount;
    private BigDecimal maxAmount;
    private boolean active = true;
    private String description;

    public RuleConfigEvent() {
    }

    public RuleConfigEvent(String ruleId, List<String> allowedCurrencies, BigDecimal minAmount, BigDecimal maxAmount, boolean active, String description) {
        this.ruleId = ruleId;
        this.allowedCurrencies = allowedCurrencies != null ? allowedCurrencies : new ArrayList<>();
        this.minAmount = minAmount;
        this.maxAmount = maxAmount;
        this.active = active;
        this.description = description;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public List<String> getAllowedCurrencies() {
        return allowedCurrencies;
    }

    public void setAllowedCurrencies(List<String> allowedCurrencies) {
        this.allowedCurrencies = allowedCurrencies;
    }

    public BigDecimal getMinAmount() {
        return minAmount;
    }

    public void setMinAmount(BigDecimal minAmount) {
        this.minAmount = minAmount;
    }

    public BigDecimal getMaxAmount() {
        return maxAmount;
    }

    public void setMaxAmount(BigDecimal maxAmount) {
        this.maxAmount = maxAmount;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RuleConfigEvent)) return false;
        RuleConfigEvent that = (RuleConfigEvent) o;
        return active == that.active &&
                Objects.equals(ruleId, that.ruleId) &&
                Objects.equals(allowedCurrencies, that.allowedCurrencies) &&
                Objects.equals(minAmount, that.minAmount) &&
                Objects.equals(maxAmount, that.maxAmount) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleId, allowedCurrencies, minAmount, maxAmount, active, description);
    }

    @Override
    public String toString() {
        return "RuleConfigEvent{" +
                "ruleId='" + ruleId + '\'' +
                ", allowedCurrencies=" + allowedCurrencies +
                ", minAmount=" + minAmount +
                ", maxAmount=" + maxAmount +
                ", active=" + active +
                ", description='" + description + '\'' +
                '}';
    }
}
