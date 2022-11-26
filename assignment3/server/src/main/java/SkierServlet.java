import channelpool.RMQChannelFactory;
import channelpool.RMQChannelPool;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.SneakyThrows;
import model.LiftRide;
import model.LiftRideWithIds;

import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * An Apache Tomcat servlet to handle API requests from the client.
 */
@WebServlet(name = "SkierServlet", value = "/SkierServlet")
public class SkierServlet extends HttpServlet {

    final String QUEUE_NAME = "test";
    final String RABBITMQ_AMQP_ENDPOINT = "amqp://guest:guest@44.227.10.230:5672";
    final int NO_CHANNELS = 200;
    Connection connection;
    RMQChannelPool channelPool;


    /**
     * Initializes a connection pool for communication with RabbitMQ, based on instructor's RMQChannelFactory class
     */
    @SneakyThrows
    @Override
    public void init() {
        super.init();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(RABBITMQ_AMQP_ENDPOINT);
        connection = factory.newConnection();
        RMQChannelFactory channelFactory = new RMQChannelFactory(connection);
        channelPool = new RMQChannelPool(NO_CHANNELS, channelFactory);
    }

    /**
     * TODO: Handle GET requests from the client
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.getWriter().write("Construction in progress");
    }

    /**
     * Handle POST requests from the client, with full path and payload validation.
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");

        LiftRide liftRide = new Gson().fromJson(request.getReader(), LiftRide.class);

        String urlPath = request.getPathInfo();

        // check if we have a URL
        checkIfUrlEmpty(response, urlPath);
        checkFormatAndSendToRabbit(channelPool, response, urlPath, liftRide);
    }

    /**
     * A helper function that sends a message to RabbitMQ given a channel pool and lift ride information.
     */
    @SneakyThrows
    private void sendToRabbit(RMQChannelPool channelPool, LiftRideWithIds liftRideWithIds) {
        Channel channel = channelPool.borrowObject();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        Integer skierId = liftRideWithIds.getSkierId();
        String seasonId = liftRideWithIds.getSeasonId();
        String dayId = liftRideWithIds.getDayId();
        Integer resortId = liftRideWithIds.getResortId();
        Integer liftId = liftRideWithIds.getLiftId();
        Integer time = liftRideWithIds.getTime();

        String message = skierId + "/" + seasonId + "/" + dayId + "/" + resortId + "/" + liftId + "/" + time;
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        channelPool.returnObject(channel);
    }

    /**
     * A helper function that checks if the API path is empty, and returns an HTTP 404 if so.
     */
    @SneakyThrows
    private void checkIfUrlEmpty(HttpServletResponse response, String urlPath) {
        if (urlPath == null || urlPath.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("The URL supplied is empty or is missing parameters");
        }
    }

    /**
     * A helper function that checks if the API path is improperly formatted, and returns an HTTP 404 if so;
     * otherwise, a message is sent to RabbitMQ and an HTTP 201 will be returned.
     */
    @SneakyThrows
    private void checkFormatAndSendToRabbit(RMQChannelPool channelPool, HttpServletResponse response, String urlPath, LiftRide liftRide) {
        if (!isUrlValid(urlPath)) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            response.getWriter().write("The URL supplied contains one or more errors. Please fix and try again");
        } else {
            LiftRideWithIds liftRideWithIds = new LiftRideWithIds();
            liftRideWithIds.setLiftId((int) liftRide.getLiftId());
            liftRideWithIds.setTime((int) liftRide.getTime());
            String[] urlParts = urlPath.split("/");
            liftRideWithIds.setResortId(Integer.valueOf(urlParts[1]));
            liftRideWithIds.setSeasonId(urlParts[3]);
            liftRideWithIds.setDayId(urlParts[5]);
            liftRideWithIds.setSkierId(Integer.valueOf(urlParts[7]));

            sendToRabbit(channelPool, liftRideWithIds);
            response.setStatus(HttpServletResponse.SC_CREATED);
            response.getWriter().write("POST request successful (and sent to RabbitMQ)");
        }
    }

    /**
     * A helper function that carries out path validation logic, and returns true if URL parameters are valid.
     */
    @SneakyThrows
    private boolean isUrlValid(String urlPath) {
        String[] urlParts = urlPath.split("/");

        // an appropriate urlParts should have a length that's at least 8 - avoiding out-of-bounds exception down the road
        if (urlParts.length < 8) return false;

        final String SEASONS = "seasons", DAYS = "days", SKIERS = "skiers";

        // field names must be exactly "seasons", "days", and "skiers"
        if (!urlParts[2].equals(SEASONS) || !urlParts[4].equals(DAYS) || !urlParts[6].equals(SKIERS)) return false;

        // parameters must conform to formats specified in the Swagger API
        try {
            Integer.parseInt(urlParts[1]);
        } catch (NumberFormatException nfe) {
            return false;
        }

        try {
            int skiDay = Integer.parseInt(urlParts[5]);
            if (skiDay < 1 || skiDay > 366) return false;
        } catch (NumberFormatException nfe) {
            return false;
        }

        try {
            Integer.parseInt(urlParts[7]);
        } catch (NumberFormatException nfe) {
            return false;
        }

        return true;
    }
}
