package com.poc.flink.transaction.sink;

import com.poc.flink.transaction.model.TransactionEvent;
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
 * DynamoDB sink for persisting processed transaction events into the ledger table.
 * Uses flink-connector-dynamodb with idempotent PUT operations keyed by 'pk'.
 */
public final class TransactionDynamoDbSink {

    private TransactionDynamoDbSink() {}

    public static DynamoDbSink<TransactionEvent> create(
            String endpoint, String region, String accessKey, String secretKey, String tableName) {
        Properties sinkProperties = new Properties();
        sinkProperties.setProperty(AWSConfigConstants.AWS_REGION, region);
        sinkProperties.setProperty(AWSConfigConstants.AWS_ENDPOINT, endpoint);
        sinkProperties.setProperty(
                AWSConfigConstants.AWS_CREDENTIALS_PROVIDER,
                AWSConfigConstants.CredentialProvider.BASIC.name());
        sinkProperties.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, accessKey);
        sinkProperties.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, secretKey);

        return DynamoDbSink.<TransactionEvent>builder()
                .setDynamoDbProperties(sinkProperties)
                .setTableName(tableName)
                .setOverwriteByPartitionKeys(List.of("pk"))
                .setElementConverter((event, context) -> toWriteRequest(event))
                .build();
    }

    private static DynamoDbWriteRequest toWriteRequest(TransactionEvent event) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("pk", AttributeValue.fromS("TX#" + event.getTransactionId()));
        item.put("sk", AttributeValue.fromS("ACC#" + event.getAccountId()));
        item.put("transaction_id", AttributeValue.fromS(event.getTransactionId()));
        item.put("account_id", AttributeValue.fromS(event.getAccountId()));
        item.put("amount", AttributeValue.fromN(event.getAmount().toPlainString()));
        item.put("currency", AttributeValue.fromS(event.getCurrency()));
        item.put("status", AttributeValue.fromS(event.getStatus()));
        item.put("timestamp", AttributeValue.fromN(String.valueOf(event.getTimestamp())));
        item.put("processed_at", AttributeValue.fromN(String.valueOf(System.currentTimeMillis())));

        return DynamoDbWriteRequest.builder()
                .setType(DynamoDbWriteRequestType.PUT)
                .setItem(item)
                .build();
    }
}
