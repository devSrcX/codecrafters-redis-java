
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisCommandHandler {

    private static final String RESPONSE_STRING_TEMPLATE = "$%d\r\n%s\r\n";
    private static final Logger log = LoggerFactory.getLogger(RedisCommandHandler.class);
    private final Map<String, CachedValue> cache = new ConcurrentHashMap<>();
    private final Map<String, List<String>> lists = new ConcurrentHashMap<>();
    private final Map<String, Object> listLocks = new ConcurrentHashMap<>();
    private final Map<String, Stream> streams = new ConcurrentHashMap<>();
    private final Map<String, Object> streamLocks = new ConcurrentHashMap<>();

    public String handle(String redisCommandLiteral) {
        var splitedString = redisCommandLiteral.split("\r\n");
        var commandName = splitedString[2].toUpperCase();
        var command = new Command(commandName, getPayload(commandName, splitedString));
        log.info("executed command: {}, payload: {}, raw value: {}",
                command.name(),
                command.payload(),
                redisCommandLiteral.replace("\r\n", "\\r\\n")
        );
        return command.payload();
    }

    private String getPayload(String commandName, String[] splitCommand) {
        return switch (commandName) {
            case "ECHO" ->
                String.format(RESPONSE_STRING_TEMPLATE, splitCommand[4].length(), splitCommand[4]);
            case "GET" -> {
                var cachedValue = cache.get(splitCommand[4]);
                if (cachedValue == null || cachedValue.isExpired()) {
                    cache.remove(splitCommand[4]);
                    yield "$-1\r\n";
                }
                yield cachedValue.value();
            }
            case "SET" -> {
                var optionalParameter = splitCommand.length > 10 ? splitCommand[8] : "";
                log.info("Executing: {} with optionalParameter: {}", commandName, optionalParameter);
                yield switch (optionalParameter) {
                    case "EX" -> {
                        var seconds = Integer.parseInt(splitCommand[10]);
                        var expirationTimeMs = System.currentTimeMillis() + (seconds * 1000L);
                        log.info("setting key: {} with expiration of {} seconds", splitCommand[4], seconds);
                        var value = String.format(RESPONSE_STRING_TEMPLATE, splitCommand[6].length(), splitCommand[6]);
                        cache.put(splitCommand[4], new CachedValue(value, expirationTimeMs));
                        yield "+OK\r\n";
                    }
                    case "PX" -> {
                        var milliseconds = Integer.parseInt(splitCommand[10]);
                        var expirationTimeMs = System.currentTimeMillis() + milliseconds;
                        log.info("setting key: {} with expiration of {} milliseconds", splitCommand[4], milliseconds);
                        var value = String.format(RESPONSE_STRING_TEMPLATE, splitCommand[6].length(), splitCommand[6]);
                        cache.put(splitCommand[4], new CachedValue(value, expirationTimeMs));
                        yield "+OK\r\n";
                    }
                    default -> {
                        var setCommandResponse = String.format(RESPONSE_STRING_TEMPLATE, splitCommand[6].length(), splitCommand[6]);
                        cache.put(splitCommand[4], new CachedValue(setCommandResponse, -1));
                        yield "+OK\r\n";
                    }
                };
            }
            case "PING" ->
                "+PONG\r\n";
            case "RPUSH" -> {
                var key = splitCommand[4];
                var cachedList = lists.computeIfAbsent(key, k -> new ArrayList<>());
                var lock = listLocks.computeIfAbsent(key, k -> new Object());
                log.info("Adding values to list with key: {}", key);
                synchronized (lock) {
                    for (int i = 6; i < splitCommand.length; i += 2) {
                        if (!splitCommand[i].isEmpty()) {
                            cachedList.add(splitCommand[i]);
                            log.info("Added value: {} to list with key: {}", splitCommand[i], key);
                        }
                    }
                    lock.notifyAll();  // Notify all waiting BLPOP threads
                }
                yield ":" + cachedList.size() + "\r\n";
            }
            case "LRANGE" -> {
                var key = splitCommand[4];
                var cachedList = lists.get(key);
                if (cachedList == null) {
                    yield "*0\r\n";
                }
                var start = Integer.parseInt(splitCommand[6]);
                var end = Integer.parseInt(splitCommand[8]);
                if (start < 0) {
                    start = cachedList.size() + start;
                    if (start < 0) {
                        start = 0;
                    }
                }
                if (end < 0) {
                    end = cachedList.size() + end;
                }
                if (start > end || start >= cachedList.size()) {
                    yield "*0\r\n";
                }
                end = Math.min(end, cachedList.size() - 1);
                var responseBuilder = new StringBuilder();
                responseBuilder.append("*").append(end - start + 1).append("\r\n");
                for (int i = start; i <= end; i++) {
                    String value = cachedList.get(i);
                    responseBuilder.append(String.format(RESPONSE_STRING_TEMPLATE, value.length(), value));
                }
                yield responseBuilder.toString();
            }
            case "LPUSH" -> {
                var key = splitCommand[4];
                var cachedList = lists.computeIfAbsent(key, k -> new ArrayList<>());
                var lock = listLocks.computeIfAbsent(key, k -> new Object());
                log.info("Adding values to list with key: {}", key);
                synchronized (lock) {
                    for (int i = 6; i < splitCommand.length; i += 2) {
                        if (!splitCommand[i].isEmpty()) {
                            cachedList.add(0, splitCommand[i]);
                            log.info("Added value: {} to list with key: {}", splitCommand[i], key);
                        }
                    }
                    lock.notifyAll();  // Notify all waiting BLPOP threads
                }
                yield ":" + cachedList.size() + "\r\n";
            }
            case "LLEN" -> {
                var key = splitCommand[4];
                var cachedList = lists.get(key);
                if (cachedList == null) {
                    yield ":0\r\n";
                }
                yield ":" + cachedList.size() + "\r\n";
            }
            case "LPOP" -> {
                var key = splitCommand[4];
                var cachedList = lists.get(key);
                if (cachedList == null || cachedList.isEmpty()) {
                    yield "$-1\r\n";
                }

                var args = splitCommand.length > 6 ? splitCommand[6] : "1";
                var countToPop = Integer.parseInt(args);
                if (countToPop <= 0) {
                    yield "$-1\r\n";
                } else if (countToPop == 1) {
                    var value = cachedList.remove(0);
                    log.info("Removed value: {} from list with key: {}", value, key);
                    yield String.format(RESPONSE_STRING_TEMPLATE, value.length(), value);
                } else {
                    countToPop = Math.min(countToPop, cachedList.size());
                    var responseBuilder = new StringBuilder();
                    responseBuilder.append("*").append(countToPop).append("\r\n");
                    for (int i = 0; i < countToPop; i++) {
                        var value = cachedList.remove(0);
                        log.info("Removed value: {} from list with key: {}", value, key);
                        responseBuilder.append(String.format(RESPONSE_STRING_TEMPLATE, value.length(), value));
                    }
                    yield responseBuilder.toString();
                }
            }
            case "BLPOP" -> {
                var key = splitCommand[4];
                var blockTimeoutSeconds = Double.parseDouble(splitCommand[6]);
                var timeoutMs = (long) (blockTimeoutSeconds * 1000);

                log.info("Blocking LPOP on key: {} with timeout: {} seconds", key, blockTimeoutSeconds);

                var cachedList = lists.computeIfAbsent(key, k -> new ArrayList<>());
                var lock = listLocks.computeIfAbsent(key, k -> new Object());

                synchronized (lock) {
                    long deadline = timeoutMs > 0 ? System.currentTimeMillis() + timeoutMs : 0;

                    while (cachedList.isEmpty()) {
                        try {
                            if (timeoutMs == 0) {
                                lock.wait();
                            } else {
                                long remainingTime = deadline - System.currentTimeMillis();
                                if (remainingTime <= 0) {
                                    yield "*-1\r\n";
                                }
                                lock.wait(remainingTime);

                                // Check if timeout expired
                                if (System.currentTimeMillis() >= deadline && cachedList.isEmpty()) {
                                    yield "*-1\r\n";
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            yield "*-1\r\n";
                        }
                    }
                    if (!cachedList.isEmpty()) {
                        var value = cachedList.remove(0);
                        log.info("BLPOP removed value: {} from list with key: {}", value, key);

                        var responseBuilder = new StringBuilder();
                        responseBuilder.append("*2\r\n");
                        responseBuilder.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                        responseBuilder.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");
                        yield responseBuilder.toString();
                    }
                }
                yield "*-1\r\n";
            }
            case "TYPE" -> {
                var key = splitCommand[4];
                log.info("Executing TYPE command for key: {}", key);

                var cachedValue = cache.get(key);
                if (cachedValue != null && !cachedValue.isExpired()) {
                    yield "+string\r\n";
                }
                if (cachedValue != null && cachedValue.isExpired()) {
                    cache.remove(key);
                }
                if (lists.containsKey(key)) {
                    yield "+list\r\n";
                }
                if (streams.containsKey(key)) {
                    yield "+stream\r\n";
                }
                yield "+none\r\n";
            }
            case "XADD" -> {
                var key = splitCommand[4];
                var id = splitCommand[6];

                var stream = streams.computeIfAbsent(key, k -> new Stream());
                var lock = streamLocks.computeIfAbsent(key, k -> new Object());

                synchronized (lock) {
                    // Parse field-value pairs from the command
                    Map<String, String> fieldValues = new HashMap<>();
                    for (int i = 8; i < splitCommand.length; i += 2) {
                        if (i + 2 < splitCommand.length && !splitCommand[i].isEmpty()) {
                            String field = splitCommand[i];
                            String value = splitCommand[i + 2];
                            fieldValues.put(field, value);
                            log.info("Added field: {} with value: {} to stream: {}", field, value, key);
                        }
                    }

                    String entryId = stream.addEntry(id, fieldValues);
                    log.info("Added entry with ID: {} to stream: {}", entryId, key);
                    if (!entryId.startsWith("-ERR")) {
                        yield String.format(RESPONSE_STRING_TEMPLATE, entryId.length(), entryId);
                    } else {
                        yield entryId + "\r\n";
                    }
                }
            }
            case "XRANGE" -> {
                var key = splitCommand[4];
                var startId = Long.parseLong(splitCommand[6]);
                var endId = Long.parseLong(splitCommand[8]);

                var cachedstream = streams.get(key);

                if (cachedstream == null) {
                    yield "*0\r\n";
                } else {
                    var entries = cachedstream.getEntriesInRange(startId, endId);

                    var responseBuilder = new StringBuilder();
                    responseBuilder.append("*").append(entries.size()).append("\r\n");
                    for (StreamEntry entry : entries) {
                        responseBuilder.append("*2\r\n");
                        responseBuilder.append("$").append(entry.getId().length()).append("\r\n").append(entry.getId()).append("\r\n");

                        var fieldValues = entry.getFieldValues();
                        responseBuilder.append("*").append(fieldValues.size() * 2).append("\r\n");
                        for (Map.Entry<String, String> fieldValue : fieldValues.entrySet()) {
                            responseBuilder.append(String.format(RESPONSE_STRING_TEMPLATE, fieldValue.getKey().length(), fieldValue.getKey()));
                            responseBuilder.append(String.format(RESPONSE_STRING_TEMPLATE, fieldValue.getValue().length(), fieldValue.getValue()));
                        }
                    }
                    yield responseBuilder.toString();
                }
            }
            default ->
                null;
        };
    }
}
