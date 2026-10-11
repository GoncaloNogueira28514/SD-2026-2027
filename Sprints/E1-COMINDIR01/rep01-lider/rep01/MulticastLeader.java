package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        TreeMap<Long, SensorRecord> pending = new TreeMap<>();

        // Métricas da réplica
        long totalRecebidos = 0;
        long totalEscritos = 0;
        long maxRetidos = 0;
        long tPrimeira = 0;
        long tUltima = 0;

        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            long ultimo = file.lastSeq();
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group); // passa a receber o que é enviado ao grupo
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);
            byte[] buffer = new byte[1000];

            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                socket.receive(p); // bloqueia até chegar um datagrama
                long agora = System.currentTimeMillis();

                String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);
                
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(line);
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                    continue;
                }

                // Atualizar métrica de receção
                totalRecebidos++;
                if (totalRecebidos == 1) {
                    tPrimeira = agora;
                }
                tUltima = agora;

                if (r.getSeq() <= ultimo) {
                    System.out.println("DUPLICADO: " + r.getSeq());
                    continue;
                } else if (r.getSeq() > ultimo + 1) {
                    System.out.println("EM ESPERA: " + r.getSeq() + " (falta o registo " + (ultimo + 1) + ")");
                    if (pending.containsKey(r.getSeq())) {
                        System.out.println("DUPLICADO: " + r.getSeq());
                    } else {
                        pending.put(r.getSeq(), r);
                        if (pending.size() > maxRetidos) {
                            maxRetidos = pending.size();
                        }
                    }
                    continue;
                }

                file.append(r);
                totalEscritos++;
                ultimo = r.getSeq();

                while (pending.containsKey(ultimo + 1)) {
                    SensorRecord next = pending.remove(ultimo + 1);
                    file.append(next);
                    totalEscritos++;
                    ultimo = next.getSeq();
                    System.out.println("Aplicado: " + next.toLine());
                }

                System.out.println("Aplicado: " + r.toLine());
                
                // Mostrar métricas atuais periodicamente ou a cada pacote (opcional, ou podes consultar)
                System.out.println(String.format("[Métricas] Recebidos: %d | Escritos: %d | Máx. Retidos: %d | 1ª Receção: %d | Últ. Receção: %d",
                        totalRecebidos, totalEscritos, maxRetidos, tPrimeira, tUltima));
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}