
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
                    yield "$-1\r\n";
                }
                var start = Integer.parseInt(splitCommand[6]);
                var end = Integer.parseInt(splitCommand[8]);
                if (start < 0) {
                    start = cachedList.size() + start;
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
            default ->
                null;
        };
    }
}
