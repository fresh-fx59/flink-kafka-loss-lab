package lab;

import java.io.Serializable;

/**
 * One lab event. Wire format is tab-separated so the job needs no JSON dependency:
 *
 *   event_id \t produced_at_ms \t route \t payload
 *
 * The Kafka coordinates are carried alongside because several scenarios need to write
 * them into Postgres (the "resume point lives in the sink store" approach).
 */
public class Event implements Serializable {
    private static final long serialVersionUID = 1L;

    public long eventId;
    public long producedAt;
    public String route;
    public String payload;
    public String srcTopic;
    public int srcPartition;
    public long srcOffset;

    public Event() {}

    public static Event parse(String value, String topic, int partition, long offset) {
        String[] parts = value.split("\t", -1);
        if (parts.length < 4) {
            throw new IllegalArgumentException("malformed event: " + value);
        }
        Event e = new Event();
        e.eventId = Long.parseLong(parts[0]);
        e.producedAt = Long.parseLong(parts[1]);
        e.route = parts[2];
        e.payload = parts[3];
        e.srcTopic = topic;
        e.srcPartition = partition;
        e.srcOffset = offset;
        return e;
    }

    /** Re-serialises to the same wire format, for the router hop into kafka2. */
    public String toWire() {
        return eventId + "\t" + producedAt + "\t" + route + "\t" + payload;
    }

    public String targetTable() {
        switch (route) {
            case "a":    return "t_a";
            case "b":    return "t_b";
            case "rare": return "t_rare";
            default:     return null;
        }
    }

    @Override
    public String toString() {
        return "Event{" + eventId + "," + route + "," + srcTopic + "-" + srcPartition
                + "@" + srcOffset + "}";
    }
}
