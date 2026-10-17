package rep01;

import java.io.*;
import java.time.Instant;

public class PubSubLeader {

    static final String TOPIC = "rep01-temperaturas";
    static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) throws Exception {
        String apiUrl = args.length > 0 ? args[0] : DEFAULT_API;
        RecordFile file = new RecordFile("lider.txt");
        long ultimo = file.lastSeq();

        IpfsPubSub ipfs = new IpfsPubSub(apiUrl);
        System.out.println("Lider iniciado. A aguardar input (ex: A 20.0 ou rajada 3)...");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                // Comando rajada: gera N registos automáticos
                if (line.startsWith("rajada")) {
                    try {
                        String[] parts = line.split("\\s+");
                        if (parts.length < 2) {
                            System.out.println("Uso correto: rajada <N>");
                            continue;
                        }
                        int n = Integer.parseInt(parts[1]);
                        for (int i = 0; i < n; i++) {
                            long seq = ++ultimo;
                            String sensor = "SIM-" + (i + 1);
                            double temp = 15.0 + Math.random() * 15.0; // Temperatura aleatória entre 15 e 30 ºC
                            String timestamp = Instant.now().toString();

                            SensorRecord r = new SensorRecord(seq, sensor, temp, timestamp);
                            file.append(r);
                            ipfs.publish(TOPIC, r.toLine());
                            System.out.println("Rajada - Publicado: " + r.toLine());
                        }
                    } catch (Exception e) {
                        System.out.println("Erro ao executar a rajada: " + e.getMessage());
                    }
                    continue;
                }

                // Input normal: formato <sensor> <temperatura> (ex: A 20.0)
                String[] p = line.split("\\s+");
                if (p.length < 2) {
                    System.out.println("Formato inválido. Use: <sensor> <temperatura> (ex: A 20.0)");
                    continue;
                }

                String sensor = p[0];
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }

                long seq = ++ultimo;
                String timestamp = Instant.now().toString();
                SensorRecord r = new SensorRecord(seq, sensor, temp, timestamp);

                file.append(r);
                ipfs.publish(TOPIC, r.toLine());
                System.out.println("Publicado: " + r.toLine());
            }
        }
    }
}