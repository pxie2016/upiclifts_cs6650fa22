import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import lombok.SneakyThrows;
import model.LiftRide;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class SkiLiftConsumer implements Runnable {

    private final Connection connection;
    private final String queueName;
    private final ConcurrentHashMap<Integer, LiftRide> map;
    private final AtomicInteger numOperations;

    public SkiLiftConsumer(Connection connection, String queueName, ConcurrentHashMap<Integer, LiftRide> map, AtomicInteger numOperations) {
        this.connection = connection;
        this.queueName = queueName;
        this.map = map;
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
            LiftRide liftRide = new LiftRide();
            liftRide.setLiftId(Short.parseShort(messageParts[1]));
            liftRide.setTime(Short.parseShort(messageParts[2]));
            map.put(Integer.parseInt(messageParts[0]), liftRide);
            numOperations.getAndIncrement();
        };
        channel.basicConsume(queueName, false, cb, consumerTag -> {});
    }
}
