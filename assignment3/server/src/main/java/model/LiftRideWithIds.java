package model;

import lombok.Data;

/**
 * A Lombok @Data class that models a lift ride with all fields needed to make a POST request.
 * This includes IDs of the skier, the resort, the particular lift, time, season, and day of season.
 */
@Data
public class LiftRideWithIds {
    Integer skierId, resortId, liftId, time;
    String seasonId, dayId;
}
