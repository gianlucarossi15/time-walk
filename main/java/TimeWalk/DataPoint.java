package edge2Time;

import java.time.Instant;

public class DataPoint {
    private Instant timestamp;
    private double value;

    public DataPoint(String timestamp, double value) {
        this.timestamp = Instant.parse(timestamp);
        this.value = value;
    }

    public Instant getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
    public double getValue() {
        return value;
    }
    public void setValue(double value) {
        this.value = value;
    }
    @Override
    public String toString() {
        return "(" + timestamp + ", " + value + ")";
    }
}
