
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
        if ("*".equals(id)) {
            id = generateAutoId();
        } else {
            String[] parts = id.split("-");
            if (parts.length == 2) {
                String validationError = validateAndUpdateId(parts);
                if (validationError != null) {
                    return validationError;
                }
                id = parts[0] + "-" + parts[1];
            }
        }

        StreamEntry entry = new StreamEntry(id, fieldValues);
        entries.add(entry);
        return id;
    }

    private String generateAutoId() {
        long currentTimestamp = System.currentTimeMillis();
        long sequence = (currentTimestamp == lastTimestamp) ? lastSequence + 1 : 0;
        lastTimestamp = currentTimestamp;
        lastSequence = sequence;
        return currentTimestamp + "-" + sequence;
    }

    private String validateAndUpdateId(String[] parts) {
        boolean hasWildcardSequence = !parts[1].isEmpty() && "*".equals(parts[1]);
        long timestamp = Long.parseLong(parts[0]);
        long sequence = hasWildcardSequence ? 0 : Long.parseLong(parts[1]);

        if (!hasWildcardSequence && timestamp == 0 && sequence == 0) {
            return "-ERR The ID specified in XADD must be greater than 0-0";
        }

        if (timestamp == 0 && hasWildcardSequence) {
            sequence = 1;
        } else if (timestamp > lastTimestamp) {
            lastTimestamp = timestamp;
            lastSequence = sequence;
        } else if (timestamp == lastTimestamp) {
            if (hasWildcardSequence) {
                sequence = lastSequence + 1;
            } else if (sequence <= lastSequence) {
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item";
            }
            lastSequence = sequence;
        } else {
            return "-ERR The ID specified in XADD is equal or smaller than the target stream top item";
        }

        parts[1] = String.valueOf(sequence);
        return null;
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
