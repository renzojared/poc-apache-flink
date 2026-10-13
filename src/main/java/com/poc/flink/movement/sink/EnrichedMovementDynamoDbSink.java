package com.poc.flink.movement.sink;

import com.poc.flink.movement.model.EnrichedMovementEvent;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSink;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * DynamoDB sink for persisting enriched and validated movement events into the ledger table.
 */
public final class EnrichedMovementDynamoDbSink {

    private EnrichedMovementDynamoDbSink() {
    }

    public static DynamoDbSink<EnrichedMovementEvent> create(
            String endpoint, String region, String accessKey, String secretKey, String tableName) {
        Properties sinkProperties = new Properties();
        sinkProperties.setProperty(AWSConfigConstants.AWS_REGION, region);
        sinkProperties.setProperty(AWSConfigConstants.AWS_ENDPOINT, endpoint);
        sinkProperties.setProperty(
                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER,
                AWSConfigConstants.CredentialProvider.BASIC.name());
        sinkProperties.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, accessKey);
        sinkProperties.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, secretKey);

        return DynamoDbSink.<EnrichedMovementEvent>builder()
                .setDynamoDbProperties(sinkProperties)
                .setTableName(tableName)
                .setOverwriteByPartitionKeys(List.of("pk"))
                .setElementConverter((event, context) -> toWriteRequest(event))
                .build();
    }

    private static DynamoDbWriteRequest toWriteRequest(EnrichedMovementEvent event) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("pk", AttributeValue.fromS("MOV#" + event.getMovementId()));
        item.put("sk", AttributeValue.fromS("OP#" + (event.getOperation() != null ? event.getOperation() : "DEFAULT")));
        item.put("movement_id", AttributeValue.fromS(event.getMovementId()));
        if (event.getOperation() != null) {
            item.put("operation", AttributeValue.fromS(event.getOperation()));
        }
        if (event.getUser() != null) {
            item.put("user", AttributeValue.fromS(event.getUser()));
        }
        if (event.getAccount() != null) {
            item.put("account", AttributeValue.fromS(event.getAccount()));
        }
        if (event.getAmount() != null) {
            item.put("amount", AttributeValue.fromN(event.getAmount().toPlainString()));
        }
        if (event.getCurrency() != null) {
            item.put("currency", AttributeValue.fromS(event.getCurrency()));
        }
        if (event.getOp() != null) {
            item.put("op", AttributeValue.fromS(event.getOp()));
        }
        if (event.getStatus() != null) {
            item.put("status", AttributeValue.fromS(event.getStatus()));
        }
        if (event.getAppliedRuleId() != null) {
            item.put("applied_rule_id", AttributeValue.fromS(event.getAppliedRuleId()));
        }
        item.put("enrichment_timestamp", AttributeValue.fromN(String.valueOf(event.getEnrichmentTimestamp())));
        item.put("processed_at", AttributeValue.fromN(String.valueOf(System.currentTimeMillis())));

        return DynamoDbWriteRequest.builder()
                .setType(DynamoDbWriteRequestType.PUT)
                .setItem(item)
                .build();
    }
}
