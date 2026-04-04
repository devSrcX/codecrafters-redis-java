
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;

public class Main {

    public static void main(String[] args) {
        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while (true) {
                System.out.println("Waiting for clients on port " + port + "...");

                Socket clientSocket = serverSocket.accept();

                System.out.println("Client connected: " + clientSocket.getInetAddress());

                CompletableFuture.runAsync(() -> {
                    handleMulptipleClient(clientSocket);
                });

            }
        } catch (IOException e) {
            System.out.println("Server error: " + e.getMessage());
        }
    }

    private static void handleMulptipleClient(Socket clientSocket) {
        try (InputStream inputStream = clientSocket.getInputStream(); OutputStream outputStream = clientSocket.getOutputStream()) {

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

            String line = bufferedReader.readLine();
            System.out.println("Received line: " + line);
            while (line != null)  {
                System.out.println("Received line: " + line);
                
                if(line.contains("PING")){
                    outputStream.write("+PONG\r\n".getBytes());
                }

                line = bufferedReader.readLine();
            }
        } catch (IOException e) {
            System.out.println("Client error: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.out.println("Error closing client socket: " + e.getMessage());
            }
        }
    }
}
