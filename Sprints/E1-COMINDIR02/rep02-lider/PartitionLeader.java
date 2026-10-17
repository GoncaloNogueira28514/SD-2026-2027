package rep02;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PartitionLeader {
    // Estado partilhado entre a thread de entrada e a thread TCP
    private static long ultimoSeq = 0;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: java rep02.PartitionLeader <k> <N> [endereço] [api]");
            System.exit(1);
        }

        int k = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);
        String endereco = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        int porto = Wire.PORTO_BASE + k;
        RecordFile file = new RecordFile("lider-p" + k + ".txt");
        IpfsPubSub ps = new IpfsPubSub(api);

        // Criação do Partitioner com N
        Partitioner partitioner = new Partitioner(n);

        ultimoSeq = file.lastSeq();
        System.out.println("Líder p" + k + " iniciado. Último seq: " + ultimoSeq);

        // 1. ATIVIDADE DE ENTRADA (PubSub)
        Thread entrada = new Thread(() -> {
            try {
                ps.subscribe(Wire.topicIn(k), msg -> {
                    try {
                        if (!Wire.type(msg).equals("IN")) {
                            throw new IllegalArgumentException("A mensagem não é do tipo IN");
                        }

                        String[] partes = Wire.body(msg).split(";");
                        if (partes.length != 2)
                            throw new IllegalArgumentException("Corpo inválido");

                        String sensor = partes[0];
                        double temp = Double.parseDouble(partes[1]);

                        // VALIDAÇÃO DA PARTIÇÃO NO LÍDER
                        int j = partitioner.of(sensor);
                        if (j != k) {
                            System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                            return; // Rejeita a mensagem antes de incrementar o seq ou escrever no ficheiro
                        }

                        SensorRecord r;
                        // SECÇÃO SINCRONIZADA
                        synchronized (file) {
                            ultimoSeq++;
                            r = new SensorRecord(ultimoSeq, sensor, temp, String.valueOf(System.currentTimeMillis()));
                            file.append(r);
                        }

                        // Publicar o registo oficial para as réplicas
                        ps.publish(Wire.topicData(k), Wire.reg(endereco, porto, r));

                    } catch (Exception e) {
                        System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
                    }
                });
            } catch (Exception e) {
                System.err.println("Erro na subscrição: " + e.getMessage());
            }
        });
        entrada.start();

        // 2. ATIVIDADE DE RECUPERAÇÃO (Servidor TCP)
        try (ServerSocket server = new ServerSocket(porto)) {
            System.out.println("À escuta para recuperação no porto " + porto + "...");

            while (true) {
                try (Socket s = server.accept();
                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                        PrintWriter out = new PrintWriter(
                                new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

                    s.setSoTimeout(Wire.T_TCP_MS);
                    String pedido = in.readLine();

                    if (pedido == null || !pedido.startsWith("PEDIDO ")) {
                        out.println("ERRO pedido mal formado");
                        continue;
                    }

                    String[] partes = pedido.split(" ");
                    if (partes.length != 3) {
                        out.println("ERRO formato deve ser PEDIDO <de> <até>");
                        continue;
                    }

                    long de;
                    long ate;
                    try {
                        de = Long.parseLong(partes[1]);
                        ate = Long.parseLong(partes[2]);
                    } catch (NumberFormatException e) {
                        out.println("ERRO valores numericos invalidos");
                        continue;
                    }

                    // Estado partilhado protegido
                    long maxSeqAtual;
                    synchronized (file) {
                        maxSeqAtual = ultimoSeq;
                    }

                    if (de > ate) {
                        out.println("ERRO <de> maior que <até>");
                        continue;
                    }

                    long limiteAte = Math.min(ate, maxSeqAtual);
                    List<SensorRecord> registos = file.range(de, limiteAte);

                    for (SensorRecord r : registos) {
                        out.println(r.toLine());
                    }
                    out.println("FIM");

                    System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " para " +
                            s.getRemoteSocketAddress() + " (" + registos.size() + " registos)");

                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }
}