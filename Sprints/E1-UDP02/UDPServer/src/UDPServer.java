import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.ArrayList;

class ClientState {
    int L = 0;
    HashMap<Integer, String> bufferTemporario = new HashMap<>();
    ArrayList<String> listaEntregues = new ArrayList<>();
}

public class UDPServer {

    static final int MAX_WINDOW_SIZE = 100;
    static HashMap<SocketAddress, ClientState> clients = new HashMap<>();
    static SocketAddress currentClientAddress;

    public static void main(String args[]) {
        DatagramSocket aSocket = null;

        try {
            aSocket = new DatagramSocket(6789);
            byte[] buffer = new byte[1000];

            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request);
                handleRequest(aSocket, request);
            }
        } catch (SocketException e) {
            System.out.println(e.getMessage());
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            if (aSocket != null) aSocket.close();
        }
    }

    private static void handleRequest(DatagramSocket aSocket, DatagramPacket request) throws IOException {
        String message = new String(request.getData(), 0, request.getLength());

        if (!message.contains(",")) return;

        int requestNumber;
        try {
            String[] parts = message.split(",", 2); 
            requestNumber = Integer.parseInt(parts[0]);
        } catch (NumberFormatException e) {
            return;
        }

        currentClientAddress = request.getSocketAddress();
        clients.putIfAbsent(currentClientAddress, new ClientState());
        ClientState client = clients.get(currentClientAddress);

        if (requestNumber <= client.L) {
            DatagramPacket reply = new DatagramPacket(
                request.getData(), request.getLength(), 
                request.getAddress(), request.getPort()
            );
            aSocket.send(reply);
            return;
        }

        client.L = processDeliveredMessages(client.L, requestNumber, message);

        if (requestNumber <= client.L) {
            DatagramPacket reply = new DatagramPacket(
                request.getData(), request.getLength(), 
                request.getAddress(), request.getPort()
            );
            aSocket.send(reply);
        } else {
            enviarResposta(aSocket, request, "waitingfor," + (client.L + 1));
        }
    }

    public static int processDeliveredMessages(int nLastMessageInOrder, int nCurrentMessage, String currentMessage) {
        ClientState client = clients.get(currentClientAddress);

        if (client.bufferTemporario.containsKey(nCurrentMessage)) {
            return nLastMessageInOrder;
        }

        if (nCurrentMessage > nLastMessageInOrder + MAX_WINDOW_SIZE) {
            return nLastMessageInOrder;
        }

        if (nCurrentMessage == nLastMessageInOrder + 1) {
            client.listaEntregues.add(currentMessage);
            nLastMessageInOrder++;

            while (client.bufferTemporario.containsKey(nLastMessageInOrder + 1)) {
                String msgGuardada = client.bufferTemporario.remove(nLastMessageInOrder + 1);
                System.out.println("Mensagem " + (nLastMessageInOrder + 1) + " entregue: " + msgGuardada);
                client.listaEntregues.add(msgGuardada);
                nLastMessageInOrder++;
            }
            
        } else if (nCurrentMessage > nLastMessageInOrder + 1) {
            client.bufferTemporario.put(nCurrentMessage, currentMessage);
        }

        return nLastMessageInOrder;
    }

    private static void enviarResposta(DatagramSocket aSocket, DatagramPacket request, String resposta) throws IOException {
        byte[] bytesResposta = resposta.getBytes();
        DatagramPacket reply = new DatagramPacket(
            bytesResposta, bytesResposta.length, 
            request.getAddress(), request.getPort()
        );
        aSocket.send(reply);
    }
}