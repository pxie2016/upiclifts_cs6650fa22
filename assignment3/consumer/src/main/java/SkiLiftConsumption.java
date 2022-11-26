import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.SneakyThrows;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class SkiLiftConsumption {
    private static final String QUEUE_NAME = "test";
    private static final String DYNAMO_TABLE_NAME = "upicResorts";
    private static final String DYNAMO_TABLE_PRIMARY_KEY = "skierID";
    private static final String RABBITMQ_AMQP_ENDPOINT = "amqp://guest:guest@44.227.10.230:5672";
    private static final int CONSUMER_THREADS = 200;

    @SneakyThrows
    public static void main(String[] args) {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        Region region = Region.US_WEST_2;
        DynamoDbAsyncClient ddbClient = DynamoDbAsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(region)
                .build();

        final AtomicInteger numOperations = new AtomicInteger(0);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(RABBITMQ_AMQP_ENDPOINT);
        Connection connection = factory.newConnection();
        ExecutorService consumerPool = Executors.newCachedThreadPool();

        for (int i = 0; i < CONSUMER_THREADS; i++) {
            SkiLiftConsumer consumer = new SkiLiftConsumer(connection, QUEUE_NAME, ddbClient, DYNAMO_TABLE_NAME, DYNAMO_TABLE_PRIMARY_KEY, numOperations);
            System.out.println("Consumer created");
            consumerPool.execute(consumer);
        }
    }
}
