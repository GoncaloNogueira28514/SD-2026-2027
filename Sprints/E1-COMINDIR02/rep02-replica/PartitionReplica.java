package rep02;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class PartitionReplica {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: java rep02.PartitionReplica <id> <partições> [portoApi]");
            System.exit(1);
        }

        int id = Integer.parseInt(args[0]);
        String[] particoes = args[1].split(","); // Na Parte A, recebemos "0"
        String apiPort = args.length > 2 ? args[2] : "5001";
        String api = "http://127.0.0.1:" + apiPort + "/api/v0";

        // Ler as propriedades de sistema para simulação de perdas
        double perda = 0.0;
        long descartar = -1;

        String propPerda = System.getProperty("perda");
        if (propPerda != null) {
            perda = Double.parseDouble(propPerda);
        }

        String propDescartar = System.getProperty("descartar");
        if (propDescartar != null) {
            descartar = Long.parseLong(propDescartar);
        }

        IpfsPubSub ps = new IpfsPubSub(api);
        System.out.println("Réplica " + id + " iniciada. (API=" + api + ")");

        for (String p : particoes) {
            int k = Integer.parseInt(p);
            iniciarParticao(id, k, ps, perda, descartar);
        }
    }

    private static void iniciarParticao(int id, int k, IpfsPubSub ps, double perda, long descartar) throws Exception {
        RecordFile file = new RecordFile("replica-" + id + "-p" + k + ".txt");

        final long[] ultimo = { file.lastSeq() };
        final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        final String[] liderHost = { null };
        final int[] liderPort = { -1 };
        final long[] descartarTarget = { descartar };

        System.out.println("Partição " + k + " -> Último seq: " + ultimo[0]);

        Thread sub = new Thread(() -> {
            try {
                ps.subscribe(Wire.topicData(k), msg -> {
                    try {
                        if (!Wire.type(msg).equals("REG")) {
                            throw new IllegalArgumentException("Mensagem não é REG");
                        }

                        String[] partes = Wire.regParts(msg);
                        liderHost[0] = partes[0];
                        liderPort[0] = Integer.parseInt(partes[1]);
                        SensorRecord r = SensorRecord.fromLine(partes[2]);

                        if (descartarTarget[0] == r.getSeq()) {
                            System.out.println("DESCARTADO (simulação): " + r.getSeq());
                            descartarTarget[0] = -1;
                            return;
                        }
                        if (perda > 0 && Math.random() < perda) {
                            System.out.println("DESCARTADO (simulação): " + r.getSeq());
                            return;
                        }

                        long seq = r.getSeq();

                        if (seq == ultimo[0] + 1) {
                            file.append(r);
                            ultimo[0] = seq;
                            processarRetidos(file, retidos, ultimo);

                        } else if (seq <= ultimo[0] || retidos.containsKey(seq)) {
                            System.out.println("DUPLICADO: " + seq);

                        } else if (seq > ultimo[0] + 1) {
                            retidos.put(seq, r);
                            System.out.println("EM ESPERA: " + seq);

                            if (retidos.size() > Wire.LIMITE_RETIDOS) {
                                recuperar(k, liderHost[0], liderPort[0], file, retidos, ultimo);
                            }
                        }

                    } catch (Exception e) {
                        System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
                    }
                });
            } catch (Exception e) {
                System.out.println("Erro na subscrição p" + k + ": " + e.getMessage());
            }
        });
        sub.start();
    }

    private static void processarRetidos(RecordFile file, TreeMap<Long, SensorRecord> retidos, long[] ultimo)
            throws Exception {
        while (retidos.containsKey(ultimo[0] + 1)) {
            SensorRecord r = retidos.remove(ultimo[0] + 1);
            file.append(r);
            ultimo[0] = r.getSeq();
        }
    }

    private static void recuperar(int k, String host, int port, RecordFile file, TreeMap<Long, SensorRecord> retidos,
            long[] ultimo) {
        if (host == null || port == -1)
            return;

        long de = ultimo[0] + 1;
        long ate = retidos.firstKey() - 1;

        System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " pedida a " + host + ":" + port);

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), Wire.T_TCP_MS);
            s.setSoTimeout(Wire.T_TCP_MS);

            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8),
                    true);

            out.println("PEDIDO " + de + " " + ate);

            String linha;
            boolean recebidoFim = false;

            while ((linha = in.readLine()) != null) {
                if (linha.equals("FIM")) {
                    recebidoFim = true;
                    break;
                }
                if (linha.startsWith("ERRO")) {
                    System.out.println("RECUPERAÇÃO FALHOU: " + linha);
                    return;
                }

                try {
                    SensorRecord r = SensorRecord.fromLine(linha);
                    if (r.getSeq() == ultimo[0] + 1) {
                        file.append(r);
                        ultimo[0] = r.getSeq();
                        System.out.println("RECUPERADO: " + r.getSeq());
                    } else {
                        System.out.println("RECUPERAÇÃO FALHOU: Sequência TCP inesperada. Recebido " + r.getSeq()
                                + ", esperado " + (ultimo[0] + 1));
                        return;
                    }
                } catch (Exception e) {
                    System.out.println("RECUPERAÇÃO FALHOU: Erro ao ler registo - " + e.getMessage());
                    return;
                }
            }

            if (recebidoFim) {
                processarRetidos(file, retidos, ultimo);
            } else {
                System.out.println("RECUPERAÇÃO FALHOU: Resposta interrompida antes de FIM");
            }

        } catch (Exception e) {
            System.out.println("RECUPERAÇÃO FALHOU: " + e.getMessage());
        }
    }
}