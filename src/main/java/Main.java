import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
  public static void main(String[] args){
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    System.out.println("Logs from your program will appear here!");

    //  Uncomment the code below to pass the first stage
       ServerSocket serverSocket = null;
       Socket clientSocket = null;
       int port = 6379;
       try {
         serverSocket = new ServerSocket(port);
         // Since the tester restarts your program quite often, setting SO_REUSEADDR
         // ensures that we don't run into 'Address already in use' errors
         serverSocket.setReuseAddress(true);
         // Wait for connection from client.
         clientSocket = serverSocket.accept();

         while (true) {
           // Read data from client and print it to console.
           byte[] buffer = new byte[1024];
           int bytesRead = clientSocket.getInputStream().read(buffer);
           if (bytesRead == -1) {
             break; // Client closed the connection
           }
           String request = new String(buffer, 0, bytesRead);
           System.out.println("Received from client: " + request.trim());

           // Respond with "+PONG\r\n" to the client.
           if(request.contains("PING")){
            clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
           }
         }
       } catch (IOException e) {
         System.out.println("IOException: " + e.getMessage());
       } finally {
         try {
           if (clientSocket != null) {
             clientSocket.close();
           }
         } catch (IOException e) {
           System.out.println("IOException: " + e.getMessage());
         }
       }
  }
}
