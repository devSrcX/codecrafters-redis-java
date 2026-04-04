import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    public static void main(String[] args) throws IOException{
        int port = 6379;
        var commandHandler = new RedisCommandHandler();
        log.info("loading custom redis server");
        try (var serverSocketChannel = ServerSocketChannel.open()){
            serverSocketChannel.bind(new InetSocketAddress("localhost",port));
            log.info("server started");
            while (true){
                var clientSocket = serverSocketChannel.accept();
                executor.submit(()->handlePetition(clientSocket,commandHandler));
            }
        } catch (IOException e) {
            log.error("IOException:",e);
            throw e;
        }
    }

    private static void handlePetition(SocketChannel clientSocketChannel,RedisCommandHandler handler) {
        try(clientSocketChannel){
            var byteBuffer = ByteBuffer.allocate(1024);
            while(clientSocketChannel.read(byteBuffer) > 0){
                byteBuffer.flip();
                log.info("handling petition for ip: {}",clientSocketChannel.getRemoteAddress());
                if(clientSocketChannel.isOpen()){
                    var result = handler.handle(new String(byteBuffer.array(),0,byteBuffer.limit()));
                    clientSocketChannel.write(ByteBuffer.wrap(result.getBytes()));
                }
            }
        } catch (IOException exception){
            log.error("an error occurred while handling petition");
        }
    }
}