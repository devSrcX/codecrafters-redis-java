
import java.util.ArrayList;
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
                log.info("Adding values to list with key: {}", key);
                for (int i = 6; i < splitCommand.length; i += 2) {
                    if (!splitCommand[i].isEmpty()) {
                        cachedList.add(splitCommand[i]);
                        log.info("Added value: {} to list with key: {}", splitCommand[i], key);
                    }
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
                log.info("Adding values to list with key: {}", key);
                for (int i = 6; i < splitCommand.length; i += 2) {
                    if (!splitCommand[i].isEmpty()) {
                        cachedList.add(0, splitCommand[i]);
                        log.info("Added value: {} to list with key: {}", splitCommand[i], key);
                    }
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
                var blockTimeoutSeconds = Integer.parseInt(splitCommand[6]);
                var deadline = System.currentTimeMillis() + (blockTimeoutSeconds * 1000L);

                log.info("Blocking LPOP on key: {} with timeout: {} seconds", key, blockTimeoutSeconds);
            
                while (System.currentTimeMillis() < deadline) {
                    var cachedList = lists.get(key);
                    if (cachedList != null && !cachedList.isEmpty()) {
                        var value = cachedList.remove(0);
                        log.info("BLPOP removed value: {} from list with key: {}", value, key);
                        
                        var responseBuilder = new StringBuilder();
                        responseBuilder.append("*2\r\n");
                        responseBuilder.append("$").append(key.length()).append("\r\n").append(key).append("\r\n");
                        responseBuilder.append("$").append(value.length()).append("\r\n").append(value).append("\r\n");
                        yield responseBuilder.toString();
                    }
                    
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                yield "$-1\r\n";
            }
            default ->
                null;
        };
    }
}
