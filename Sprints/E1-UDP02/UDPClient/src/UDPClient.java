import java.io.*;
import java.net.*;
import java.util.Scanner;

public class UDPClient {

  public static void main(String args[]) {
    DatagramSocket aSocket = null;
    Scanner s = new Scanner(System.in);
    String message = writeMessage(s);

    try {
      aSocket = new DatagramSocket();
    } catch (Exception e) {
      System.out.println("Socket: " + e.getMessage());
    }

    while (!message.equals("exit")) {
      try {
        sendMessage(aSocket, message);
      } catch (IOException e) {
        System.out.println("IO: " + e.getMessage());
      }
      message = writeMessage(s);
    }

    try {
      if (aSocket != null) aSocket.close();
    } catch (Exception e) {
      System.out.println("Socket: " + e.getMessage());
    }
  }

  private static String writeMessage(Scanner s) {
    System.out.println("Digite a mensagem que deseja enviar ao servidor: ");
    return s.nextLine();
  }

  private static void sendMessage(DatagramSocket aSocket, String message) throws IOException {
    try {
      byte[] m = message.getBytes();
      InetAddress aHost = InetAddress.getByName("localhost");
      int serverPort = 6789;

      DatagramPacket request = new DatagramPacket(m, m.length, aHost, serverPort);

      aSocket.send(request);

      handleReply(aSocket);

    } catch (SocketException e) {
      System.out.println("Socket: " + e.getMessage());
    } catch (IOException e) {
      System.out.println("IO: " + e.getMessage());
    }
  }

  private static void handleReply(DatagramSocket aSocket) throws IOException {
    byte[] buffer = new byte[1000];
    DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
    aSocket.receive(reply);

    String replyMessage = new String(reply.getData(), 0, reply.getLength());

    if (replyMessage.startsWith("waitingfor")) {
      System.out.println("Server is waiting for message: " + replyMessage.replaceAll("waitingfor,", "") + ". Please send the message again.");
      return;
    }

    System.out.println("Reply: " + new String(reply.getData(), 0, reply.getLength()));
  }
}