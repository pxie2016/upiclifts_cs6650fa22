import model.LiftRideWithIds;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Producer in the producer-consumer model for posting lift rides. Generates
 * 200,000 lift rides per assignment specifications.
 */
public class LiftRideGenerator implements Runnable {

    private final BlockingQueue<LiftRideWithIds> buffer;
    private static final Integer COUNT = 200000;

    public LiftRideGenerator(BlockingQueue<LiftRideWithIds> buffer) {
        this.buffer = buffer;
    }

    /**
     * Static method that generates a LiftRideWithIds object for consumption.
     */
    public static LiftRideWithIds generateLiftRideWithIds() {
        LiftRideWithIds liftRideWithIds = new LiftRideWithIds();
        liftRideWithIds.setSkierId(randomIntegerFrom1(100000));
        liftRideWithIds.setResortId(randomIntegerFrom1(10));
        liftRideWithIds.setLiftId(randomIntegerFrom1(40));
        liftRideWithIds.setSeasonId("2022");
        liftRideWithIds.setDayId("1");
        liftRideWithIds.setTime(randomIntegerFrom1(360));
        return liftRideWithIds;
    }

    /**
     * Generates an integer from discrete uniform random distribution with 1 as minimum, inclusive on both ends.
     * @param max maximum possible integer
     */
    private static Integer randomIntegerFrom1(Integer max) {
        // ThreadLocalRandom for lock contention avoidance
        return ThreadLocalRandom.current().nextInt(1, max + 1);
    }

    /**
     * Actual work to do - generates a LiftRideWithId object at a time in a for-loop.
     */
    @Override
    public void run() {
        try {
            for (int i = 0; i < COUNT; i++) {
                buffer.put(generateLiftRideWithIds());
            }
        } catch (InterruptedException ie) {
            System.err.println("Data generation interrupted");
        }
    }
}
