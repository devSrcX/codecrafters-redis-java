

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisCommandHandler {
    private static final String RESPONSE_STRING_TEMPLATE = "$%d\r\n%s\r\n";
    private static final Logger log = LoggerFactory.getLogger(RedisCommandHandler.class);
    private final Map<String,String> cache = new ConcurrentHashMap<>();

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
            case "GET" -> cache.getOrDefault(splitCommand[4],"$-1\r\n");
            case "SET" -> {
                var setCommandResponse= String.format(RESPONSE_STRING_TEMPLATE,splitCommand[6].length(),splitCommand[6]);
                cache.put(splitCommand[4],setCommandResponse);
                yield "+OK\r\n";
            }
            case "PING" -> "+PONG\r\n";
            default -> null;
        };
    }
}