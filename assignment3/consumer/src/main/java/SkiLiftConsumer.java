import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import lombok.SneakyThrows;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SkiLiftConsumer implements Runnable {

    private final Connection connection;
    private final String queueName;
    private final DynamoDbAsyncClient ddbClient;
    private final String tableName, tablePrimaryKey;
    private final AtomicInteger numOperations;


    public SkiLiftConsumer(Connection connection, String queueName, DynamoDbAsyncClient ddbClient, String tableName, String tablePrimaryKey, AtomicInteger numOperations) {
        this.connection = connection;
        this.queueName = queueName;
        this.ddbClient = ddbClient;
        this.tableName = tableName;
        this.tablePrimaryKey = tablePrimaryKey;
        this.numOperations = numOperations;
    }

    @SneakyThrows
    @Override
    public void run() {
        Channel channel = connection.createChannel();
        channel.queueDeclare(queueName, false, false, false, null);
        System.out.println(" [*] Waiting for messages...");

        channel.basicQos(1);

        DeliverCallback cb = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            System.out.println(" [x] Received '" + message + "'");

            String[] messageParts = message.split("/");
            String skierId = messageParts[0];
            String seasonId = messageParts[1];
            String dayId = messageParts[2];
            String resortId = messageParts[3];
            String liftId = messageParts[4];
            String time = messageParts[5];
            put(ddbClient, skierId, seasonId, dayId, resortId, liftId, time);
            numOperations.getAndIncrement();
        };
        channel.basicConsume(queueName, false, cb, consumerTag -> {});
    }

    public void put(DynamoDbAsyncClient ddbClient, String skierIdValue, String seasonIdValue, String dayIdValue, String resortIdValue, String liftIdValue, String timeValue) {
        HashMap<String, AttributeValue> itemValues = new HashMap<>();

        itemValues.put(tablePrimaryKey, AttributeValue.builder().n(skierIdValue).build());
        itemValues.put("seasonID", AttributeValue.builder().s(seasonIdValue).build());
        itemValues.put("dayID", AttributeValue.builder().s(dayIdValue).build());
        itemValues.put("resortID", AttributeValue.builder().n(resortIdValue).build());
        itemValues.put("liftID", AttributeValue.builder().n(liftIdValue).build());
        itemValues.put("time", AttributeValue.builder().n(timeValue).build());

        PutItemRequest req = PutItemRequest.builder()
                .tableName(tableName)
                .item(itemValues)
                .build();

        try {
            ddbClient.putItem(req);
        } catch (ResourceNotFoundException e1) {
            System.err.println("DynamoDB table not found!");
            System.exit(1);
        } catch (DynamoDbException e2) {
            System.err.println(e2.getMessage());
            System.exit(2);
        }
    }
}
