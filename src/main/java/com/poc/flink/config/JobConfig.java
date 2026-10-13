package com.poc.flink.config;

import java.io.Serializable;

public class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final String kafkaGroupId;
    private final String awsEndpointUrl;
    private final String awsRegion;
    private final String dynamoDbTableName;

    // Multi-stream enrichment & dynamic broadcast topics
    private final String movementsTopic;
    private final String currenciesTopic;
    private final String rulesTopic;
    private final String alertsTopic;
    private final long enrichmentTimeoutMs;

    public JobConfig() {
        this.kafkaBootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        this.kafkaTopic = getEnvOrDefault("KAFKA_TOPIC", "transactions.incoming");
        this.kafkaGroupId = getEnvOrDefault("KAFKA_GROUP_ID", "poc-flink-group");
        this.awsEndpointUrl = getEnvOrDefault("AWS_ENDPOINT_URL", "http://localhost:4566");
        this.awsRegion = getEnvOrDefault("AWS_REGION", "us-east-1");
        this.dynamoDbTableName = getEnvOrDefault("DYNAMODB_TABLE_NAME", "poc-apache-flink-ledger-dev");

        this.movementsTopic = getEnvOrDefault("KAFKA_TOPIC_MOVEMENTS", "movements.incoming");
        this.currenciesTopic = getEnvOrDefault("KAFKA_TOPIC_CURRENCIES", "movements.currencies");
        this.rulesTopic = getEnvOrDefault("KAFKA_TOPIC_RULES", "config.rules");
        this.alertsTopic = getEnvOrDefault("KAFKA_TOPIC_ALERTS", "movements.alerts");
        this.enrichmentTimeoutMs = Long.parseLong(getEnvOrDefault("ENRICHMENT_TIMEOUT_MS", "30000"));
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.trim().isEmpty()) ? val.trim() : defaultValue;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public String getAwsEndpointUrl() {
        return awsEndpointUrl;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public String getDynamoDbTableName() {
        return dynamoDbTableName;
    }

    public String getMovementsTopic() {
        return movementsTopic;
    }

    public String getCurrenciesTopic() {
        return currenciesTopic;
    }

    public String getRulesTopic() {
        return rulesTopic;
    }

    public String getAlertsTopic() {
        return alertsTopic;
    }

    public long getEnrichmentTimeoutMs() {
        return enrichmentTimeoutMs;
    }
}
