

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisCommandHandler {
    private static final String RESPONSE_STRING_TEMPLATE = "$%d\r\n%s\r\n";
    private static final Logger log = LoggerFactory.getLogger(RedisCommandHandler.class);
    private final Map<String,CachedValue> cache = new ConcurrentHashMap<>();
    private final Map<String,List<String>> lists = new ConcurrentHashMap<>();

    public String handle(String redisCommandLiteral) {
        var splitedString = redisCommandLiteral.split("\r\n");
        var commandName = splitedString[2].toUpperCase();
        var command = new Command(commandName,getPayload(commandName,splitedString));
        log.info("executing command: {}, payload: {}, raw value: {}",
                command.name(),
                command.payload(),
                redisCommandLiteral.replace("\r\n","\\r\\n")
        );
        return command.payload();
    }

    private String getPayload(String commandName,String[] splitCommand){
        return switch(commandName){
            case "ECHO" -> String.format(RESPONSE_STRING_TEMPLATE,splitCommand[4].length(),splitCommand[4]);
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
                log.info("Executing: {} with optionalParameter: {}",commandName,optionalParameter);
                yield switch (optionalParameter) {
                    case "EX" -> {
                        var seconds = Integer.parseInt(splitCommand[10]);
                        var expirationTimeMs = System.currentTimeMillis() + (seconds * 1000L);
                        log.info("setting key: {} with expiration of {} seconds",splitCommand[4],seconds);
                        var value = String.format(RESPONSE_STRING_TEMPLATE,splitCommand[6].length(),splitCommand[6]);
                        cache.put(splitCommand[4], new CachedValue(value, expirationTimeMs));
                        yield "+OK\r\n";
                    }
                    case "PX" -> {
                        var milliseconds = Integer.parseInt(splitCommand[10]);
                        var expirationTimeMs = System.currentTimeMillis() + milliseconds;
                        log.info("setting key: {} with expiration of {} milliseconds",splitCommand[4],milliseconds);
                        var value = String.format(RESPONSE_STRING_TEMPLATE,splitCommand[6].length(),splitCommand[6]);
                        cache.put(splitCommand[4], new CachedValue(value, expirationTimeMs));
                        yield "+OK\r\n";
                    }
                    default -> {
                        var setCommandResponse= String.format(RESPONSE_STRING_TEMPLATE,splitCommand[6].length(),splitCommand[6]);
                        cache.put(splitCommand[4], new CachedValue(setCommandResponse, -1));
                        yield "+OK\r\n";
                    }
                };
            }
            case "PING" -> "+PONG\r\n";
            case "RPUSH" -> {
                var key = splitCommand[4];
                var cachedList = lists.computeIfAbsent(key, k -> new ArrayList<>());
                
                for (int i = 6; i < splitCommand.length; i += 2) {
                    if (!splitCommand[i].isEmpty()) {
                        cachedList.add(splitCommand[i]);
                    }
                }
                yield ":" + cachedList.size() + "\r\n";
            }
            default -> null;
        };
    }
}