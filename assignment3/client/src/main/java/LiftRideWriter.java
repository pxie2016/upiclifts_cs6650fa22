import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.model.LiftRide;
import lombok.Setter;
import model.LatencyStats;
import model.LiftRideWithIds;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumer in the producer-consumer model for posting lift rides. Posts
 * lift rides from a shared buffer per assignment specifications.
 */
public class LiftRideWriter implements Runnable {

    private final BlockingQueue<LiftRideWithIds> buffer;
    private final SkiersApi apiInstance = new SkiersApi();
    private final AtomicInteger successes, failures; // thread safe
    private CountDownLatch latch;
    private HashSet<LatencyStats> stats;
    @Setter
    private int runTimes = 1000;

    /**
     * Constructor without the use of CountDownLatch. Initializes an instance of a poster/writer.
     */
    public LiftRideWriter(BlockingQueue<LiftRideWithIds> buffer, AtomicInteger successes, AtomicInteger failures, HashSet<LatencyStats> stats) {
        this.buffer = buffer;
        this.successes = successes;
        this.failures = failures;
        this.stats = stats;
        apiSetup(apiInstance);
    }

    /**
     * Constructor with a CountDownLatch for "Stage 1" of the posting operation, which provides a way for at
     * least 1 of the threads/instances to finish before spawning more threads/instances.
     */
    public LiftRideWriter(BlockingQueue<LiftRideWithIds> buffer, CountDownLatch latch, AtomicInteger successes, AtomicInteger failures, HashSet<LatencyStats> stats) {
        this.buffer = buffer;
        this.latch = latch;
        this.successes = successes;
        this.failures = failures;
        this.stats = stats;
        apiSetup(apiInstance);
    }

    /**
     * Actual work to do - post, with retry up to 5 times, lift rides to the server per API spec
     */
    @Override
    public void run() {
        while (this.runTimes > 0) {
            if (buffer.isEmpty()) break;
            try {
                LiftRideWithIds liftRideWithIds = buffer.take();
                LiftRide liftRide = new LiftRide();
                liftRide.setTime(liftRideWithIds.getTime());
                liftRide.setLiftID(liftRideWithIds.getLiftId());
                int maxTries = 5;
                postWithRetry(liftRide, liftRideWithIds, 0, maxTries);
                this.runTimes--;
            } catch (InterruptedException ie) {
                System.err.println("Lift ride posting interrupted");
            }
        }
        if (latch != null) latch.countDown();
        System.out.println("Thread terminating");
    }

    /**
     * A private helper that sets up client code and APIs.
     */
    private void apiSetup(SkiersApi apiInstance) {
        String BASE_PATH = "http://35.164.201.75:8080/server3";
        ApiClient apiClient = new ApiClient();
        apiClient.setBasePath(BASE_PATH);
        apiInstance.setApiClient(apiClient);
    }

    /**
     * Actual implementation of the post with retry method. Recursively calls itself until max # of tries. Adds
     * latency analytics in Part 2.
     */
    private void postWithRetry(LiftRide liftRide, LiftRideWithIds liftRideWithIds, int trialsMade, int maxTries) {
        if (trialsMade > maxTries) {
            failures.incrementAndGet();
            return;
        }
        try {
            // Timestamp 1
            Instant preHttp = Instant.now();
            ApiResponse<Void> response = apiInstance.writeNewLiftRideWithHttpInfo(liftRide, liftRideWithIds.getResortId(),
                    liftRideWithIds.getSeasonId(), liftRideWithIds.getDayId(), liftRideWithIds.getSkierId());
            // Timestamp 2
            Instant postHttp = Instant.now();
            Duration httpLatency = Duration.between(preHttp, postHttp);
            stats.add(new LatencyStats(preHttp.toEpochMilli(), "POST", (double) httpLatency.toMillis(), response.getStatusCode()));
            if (response.getStatusCode() == 200 || response.getStatusCode() == 201) successes.incrementAndGet();
        } catch (ApiException ae) {
            System.err.println("There is a 4XX or 5XX error! " + trialsMade + " tries were made (max " + maxTries + ")");
            postWithRetry(liftRide, liftRideWithIds, trialsMade + 1, maxTries);
        }
    }
}
