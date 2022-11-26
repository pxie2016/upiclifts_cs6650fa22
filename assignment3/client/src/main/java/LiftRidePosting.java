import com.google.common.math.Stats;
import model.LatencyStats;
import model.LiftRideWithIds;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.math.Quantiles.percentiles;

/**
 * "Main" class that generates 200,000 lift rides and post them to the server.
 */
public class LiftRidePosting {

    public static void main(String[] args) throws InterruptedException {

        // Initializes a BlockingQueue buffer and a CountDownLatch with parameter 1; Uses AtomicIntegers as
        // thread-safe counters for # of successes & failures
        BlockingQueue<LiftRideWithIds> buffer = new LinkedBlockingQueue<>();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger(0), failures = new AtomicInteger(0);
        HashSet<LatencyStats> stats = new HashSet<>();

        Instant start = Instant.now();
        new Thread(new LiftRideGenerator(buffer)).start();

        // CachedThreadPool for thread management
        ExecutorService writerPool = Executors.newCachedThreadPool();

        // Latency estimation (10000 posts with 1 thread/consumer)
        // Comment out when not estimating
        /*
        LiftRideWriter writer = new LiftRideWriter(buffer, successes, failures);
        System.out.println("More writers created");
        writer.setRunTimes(10000);
        writerPool.execute(writer);
         */

        // Stage 1: 32 threads, 1000 posts each
        for (int i = 0; i < 32; i++) {
            LiftRideWriter writer = new LiftRideWriter(buffer, latch, successes, failures, stats);
            System.out.println("Writer created");
            writerPool.execute(writer);
        }

        // Wait until 1 of 32 threads finish
        latch.await();

        // Stage 2: Trial and error/"tunable" # of threads
        int numThreads = 50;
        for (int j = 0; j < numThreads; j++) {
            LiftRideWriter writer = new LiftRideWriter(buffer, successes, failures, stats);
            System.out.println("More writers created");
            writer.setRunTimes((int) Math.ceil(168000. / numThreads));
            writerPool.execute(writer);
        }

        writerPool.shutdown();
        if (writerPool.awaitTermination(25, TimeUnit.MINUTES)) {
            System.out.println("All threads terminated");
        } else {
            System.err.println("There is a timeout. Adjust termination wait time");  // Should almost never reach here
        }
        Instant end = Instant.now();
        System.out.println(successes + " successful posts (should be 200000)");
        System.out.println(failures + " post failures (should be 0)");
        System.out.println(buffer.size() + " lift rides remaining in the buffer (should be 0)");
        long dt = Duration.between(start, end).toMillis();
        System.out.println(dt + " ms to send " + successes.intValue() + " requests, for a throughput of " + successes.doubleValue() / dt * 1000);

        ArrayList<Double> latencies = new ArrayList<>();
        stats.forEach(e -> latencies.add(e.getLatency()));
        System.out.println("Min latency: " + percentiles().index(0).compute(latencies) + " ms");
        System.out.println("Median latency: " + percentiles().index(50).compute(latencies) + " ms");
        System.out.println("Mean latency: " + Stats.meanOf(latencies) + " ms");
        System.out.println("99th percentile latency: " + percentiles().index(99).compute(latencies) + " ms");
        System.out.println("Max latency: " + percentiles().index(100).compute(latencies) + " ms");

    }
}