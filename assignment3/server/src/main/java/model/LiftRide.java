package model;

import lombok.Data;

/**
 * A Lombok @Data class that models a lift ride. This includes ID of the lift and time.
 */
@Data
public class LiftRide {
    private short time;
    private short liftId;
}
