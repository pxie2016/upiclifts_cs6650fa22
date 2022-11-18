import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.SneakyThrows;
import model.LiftRide;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class SkiLiftConsumption {
    private static final String QUEUE_NAME = "test";
    private static final String RABBITMQ_AMQP_ENDPOINT = "amqp://guest:guest@54.244.217.159:5672";
    private static final int CONSUMER_THREADS = 200;

    @SneakyThrows
    public static void main(String[] args) {
        final AtomicInteger numOperations = new AtomicInteger(0);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(RABBITMQ_AMQP_ENDPOINT);
        Connection connection = factory.newConnection();
        ExecutorService consumerPool = Executors.newCachedThreadPool();
        ConcurrentHashMap<Integer, LiftRide> record = new ConcurrentHashMap<>();

        for (int i = 0; i < CONSUMER_THREADS; i++) {
            SkiLiftConsumer consumer = new SkiLiftConsumer(connection, QUEUE_NAME, record, numOperations);
            System.out.println("Consumer created");
            consumerPool.execute(consumer);
        }
    }
}
