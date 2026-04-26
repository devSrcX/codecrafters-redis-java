import java.util.LinkedHashMap;
import java.util.Map;

public class StreamEntry {
    private final String id;
    private final Map<String, String> fieldValues;
    
    public StreamEntry(String id, Map<String, String> fieldValues) {
        this.id = id;
        this.fieldValues = new LinkedHashMap<>(fieldValues);
    }
    
    public String getId() {
        return id;
    }
    
    public Map<String, String> getFieldValues() {
        return fieldValues;
    }
}
