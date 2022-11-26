package model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class LatencyStats {
    long startEpochMilli;
    String type;
    double latency;
    int responseCode;
}
