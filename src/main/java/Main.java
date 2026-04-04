
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    public static void main(String[] args) {
        int port = 6379;
        ExecutorService executorService = Executors.newCachedThreadPool();
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            while (true) {
                System.out.println("Waiting for clients on port " + port + "...");

                Socket clientSocket = serverSocket.accept();

                System.out.println("Client connected: " + clientSocket.getInetAddress());

                executorService.submit(() -> {
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
            while (line != null) {
                System.out.println("Received line: " + line);
                if (line.startsWith("*")) {
                    System.out.println("Received array length line: " + line);
                    int arrayLength = Integer.parseInt(line.substring(1));
                    String command = bufferedReader.readLine();
                    System.out.println("Received command: " + command);
                    int commandLength = Integer.parseInt(command.substring(1));
                    String commandStr = bufferedReader.readLine();
                    System.out.println("Received command name: " + commandStr);

                    if (commandStr.equalsIgnoreCase("PING")) {
                        outputStream.write("+PONG\r\n".getBytes());
                        outputStream.flush();
                    } else if (commandStr.equalsIgnoreCase("ECHO")) {
                        String argLenLine = bufferedReader.readLine();
                        System.out.println("Received line: " + argLenLine);
                        String arg = bufferedReader.readLine();
                        System.out.println("Received line: " + arg);
                        outputStream.write(("$" + arg.length() + "\r\n" + arg + "\r\n").getBytes());
                        outputStream.flush();
                    }
                } else {
                    if (line.contains("PING")) {
                        outputStream.write("+PONG\r\n".getBytes());
                        outputStream.flush();
                    }
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
