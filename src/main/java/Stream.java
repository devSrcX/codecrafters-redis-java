
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stream {

    private static final Logger log = LoggerFactory.getLogger(Stream.class);
    private final List<StreamEntry> entries;
    private long lastSequence;
    private long lastTimestamp;

    public Stream() {
        this.entries = new ArrayList<>();
        this.lastTimestamp = 0;
        this.lastSequence = 0;
    }

    public String addEntry(String id, Map<String, String> fieldValues) {
        log.info("Adding entry with ID: {} and field values: {}", id, fieldValues);
        if ("*".equals(id)) {
            long currentTimestamp = System.currentTimeMillis();
            long sequence = 0;

            if (currentTimestamp == lastTimestamp) {
                sequence = lastSequence + 1;
            } else {
                sequence = 0;
                lastTimestamp = currentTimestamp;
            }

            id = currentTimestamp + "-" + sequence;
            lastSequence = sequence;
        } else {
            String[] parts = id.split("-");
            if (parts.length == 2) {
                long timestamp = Long.parseLong(parts[0]);
                long sequence = Long.parseLong(parts[1]);
                log.info("timestamp: {}, sequence: {}", timestamp, sequence);

                if (timestamp == 0 && sequence == 0) {
                    return "-ERR The ID specified in XADD must be greater than 0-0";
                } else if (timestamp == 0 && sequence == '*') {
                    lastTimestamp = timestamp;
                    lastSequence = 1;
                    log.info("* Case, lastTimestamp: {}, lastSequence: {}", lastTimestamp, lastSequence);
                } else if (timestamp > lastTimestamp) {
                    lastTimestamp = timestamp;
                    lastSequence = sequence;
                } else if (timestamp == lastTimestamp && sequence > lastSequence) {
                    lastSequence = sequence;
                } else {
                    return "-ERR The ID specified in XADD is equal or smaller than the target stream top item";
                }
            }
        }

        StreamEntry entry = new StreamEntry(id, fieldValues);
        entries.add(entry);
        return id;
    }

    public List<StreamEntry> getEntries() {
        return new ArrayList<>(entries);
    }

    public StreamEntry getEntry(String id) {
        for (StreamEntry entry : entries) {
            if (entry.getId().equals(id)) {
                return entry;
            }
        }
        return null;
    }
}
