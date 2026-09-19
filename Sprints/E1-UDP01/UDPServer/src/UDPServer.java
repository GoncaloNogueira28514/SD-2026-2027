import java.io.*;
import java.net.*;

public class UDPServer {

  public static void main(String args[]) {
    DatagramSocket aSocket = null;
    int L = 0;

    try {
      aSocket = new DatagramSocket(6789);
      byte[] buffer = new byte[1000];

      while (true) {
        DatagramPacket request = new DatagramPacket(buffer, buffer.length);
        aSocket.receive(request);

        L = handleRequest(L, aSocket, request);
      }
    } catch (SocketException e) {
      System.out.println("Socket: " + e.getMessage());
    } catch (IOException e) {
      System.out.println("IO: " + e.getMessage());
    } finally {
      if (aSocket != null)
        aSocket.close();
    }
  }

  private static int handleRequest(int L, DatagramSocket aSocket, DatagramPacket request) throws IOException {
    String message = new String(request.getData(), 0, request.getLength());
    Integer requestNumber;

    if (!message.contains(",")) {
      System.out.println("Invalid message format: " + message);
      return L;
    }

    try {
      requestNumber = Integer.parseInt(message.split(",")[0]);
    } catch (NumberFormatException e) {
      System.out.println("Invalid request number: " + message);
      return L;
    }

    if (requestNumber == L + 1) {
      DatagramPacket reply = new DatagramPacket(request.getData(),
          request.getLength(), request.getAddress(), request.getPort());

      aSocket.send(reply);
      L++;
    } else {
      String waitingMessage = "waitingfor," + (L + 1);
      byte[] waitingBytes = waitingMessage.getBytes();

      DatagramPacket waitingReply = new DatagramPacket(waitingBytes, waitingBytes.length, request.getAddress(),
          request.getPort());
      aSocket.send(waitingReply);
    }

    return L;
  }
}