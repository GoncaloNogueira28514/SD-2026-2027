package rep04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionLeader {
    // Estado partilhado entre a thread de entrada, a thread do heartbeat e a thread TCP,
    // protegido pelo monitor de file
    private static long ultimoSeq = 0;
    private static long lamport = 0;
    private static VectorClock vectorClock;
    private static List<SensorRecord> pendentes = new ArrayList<>();
    private static long hb = 0;

    // incarnação: instante de arranque, fixo durante toda a execução
    private static long inc;

    // Falha deliberada do ponto 9 (0 = código correto), -Drep04.falha=<1..4>:
    // 1 só publica com pendentes; 2 heartbeat altera L e V; 3 ultimoSeq lido fora da secção;
    // 4 ultimoSeq = 0 e descarta um em cada dez lotes
    private static final int FALHA = Integer.getInteger("rep04.falha", 0);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: java rep04.PartitionLeader <k> <N> [endereço] [api]");
            System.exit(1);
        }

        int k = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);
        vectorClock = new VectorClock(n);
        String endereco = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        int porto = Wire.PORTO_BASE + k;
        RecordFile file = new RecordFile("lider-p" + k + ".txt", n);
        Path hbFile = Paths.get("hb-p" + k + ".txt");
        IpfsPubSub ps = new IpfsPubSub(api);

        // Criação do Partitioner com N
        Partitioner partitioner = new Partitioner(n);

        inc = System.currentTimeMillis();
        hb = 0;
        ultimoSeq = file.lastSeq();
        SensorRecord ultimo = file.last();
        if (ultimo != null) {
            lamport = ultimo.getLamport();
            vectorClock = ultimo.getVector();
        }
        System.out.println("Líder p" + k + " iniciado. inc=" + inc + " Último seq: " + ultimoSeq
                + " (Δ=" + Wire.PERIODO_HB_MS + " ms" + (FALHA != 0 ? ", FALHA " + FALHA : "") + ")");

        // 1. ATIVIDADE DE ENTRADA (PubSub)
        Thread entrada = new Thread(() -> {
            try {
                ps.subscribe(Wire.topicIn(k), msg -> {
                    try {
                        if (!Wire.type(msg).equals("IN")) {
                            throw new IllegalArgumentException("A mensagem não é do tipo IN");
                        }

                        String[] partes = Wire.body(msg).split(";");
                        if (partes.length != 4)
                            throw new IllegalArgumentException("Corpo inválido");

                        String sensor = partes[0];
                        double temp = Double.parseDouble(partes[1]);
                        long incomingLamport = Long.parseLong(partes[2]);
                        VectorClock incomingVector = VectorClock.parse(partes[3], n);

                        // VALIDAÇÃO DA PARTIÇÃO NO LÍDER
                        int j = partitioner.of(sensor);
                        if (j != k) {
                            System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                            return; // Rejeita a mensagem antes de incrementar o seq ou escrever no ficheiro
                        }

                        // SECÇÃO SINCRONIZADA
                        synchronized (file) {
                            ultimoSeq++;
                            lamport = Math.max(lamport, incomingLamport) + 1;
                            vectorClock.merge(incomingVector);
                            vectorClock.tick(k);
                            if (vectorClock.get(k) != ultimoSeq) {
                                throw new IllegalStateException(
                                        "Relógio vetorial inconsistente: V[" + k + "]=" + vectorClock.get(k)
                                                + ", seq=" + ultimoSeq);
                            }
                            SensorRecord r = SensorRecord.now(ultimoSeq, sensor, temp, lamport, vectorClock);
                            file.append(r);
                            // fica à espera do próximo lote; deixa de haver um REG por registo
                            pendentes.add(r);
                            System.out.println("EVENTO: seq=" + r.getSeq()
                                    + " L=" + r.getLamport()
                                    + " V=" + r.getVector());
                        }

                    } catch (Exception e) {
                        System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
                    }
                });
            } catch (Exception e) {
                System.err.println("Erro na subscrição: " + e.getMessage());
            }
        });
        entrada.start();

        // O servidor TCP é aberto antes do primeiro lote: um participante que veja ultimoSeq
        // acima do que aplicou pede logo a recuperação
        try (ServerSocket server = new ServerSocket(porto)) {
            // 2. ATIVIDADE DE HEARTBEAT (um lote no arranque e depois a cada Δ)
            ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor();
            heartbeat.scheduleAtFixedRate(() -> enviarLote(k, endereco, porto, file, hbFile, ps),
                    0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);

            // 3. ATIVIDADE DE RECUPERAÇÃO (Servidor TCP)
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

    // fecha o lote do intervalo e publica-o; serve também de heartbeat
    private static void enviarLote(int k, String endereco, int porto, RecordFile file, Path hbFile,
            IpfsPubSub ps) {
        List<SensorRecord> lote;
        long hbAtual;
        long seqAtual;
        // mesmo monitor da atribuição de seq: nenhum registo fica entre o lote e o ultimoSeq
        synchronized (file) {
            if (FALHA == 1 && pendentes.isEmpty()) {
                return;
            }
            lote = pendentes;
            pendentes = new ArrayList<>();
            seqAtual = ultimoSeq;
            hb++;
            hbAtual = hb;
            if (FALHA == 2) {
                lamport++;
                vectorClock.tick(k);
            }
        }
        if (FALHA == 3) {
            seqAtual = ultimoSeq;
        }
        if (FALHA == 4) {
            seqAtual = 0;
            if (hbAtual % 10 == 0) {
                System.out.println("DESCARTADO (falha 4): hb=" + hbAtual + " n=" + lote.size());
                return;
            }
        }

        // fora da secção sincronizada: a publicação não bloqueia a entrada nem a recuperação
        try {
            String msg = new Batch(endereco, porto, inc, hbAtual, seqAtual, lote).encode();
            int bytes = msg.getBytes(StandardCharsets.UTF_8).length;
            long instante = System.currentTimeMillis();
            ps.publish(Wire.topicData(k), msg);

            Files.writeString(hbFile,
                    inc + ";" + hbAtual + ";" + instante + ";" + seqAtual + ";" + lote.size() + ";" + bytes
                            + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            System.out.println("HEARTBEAT: hb=" + hbAtual + " n=" + lote.size() + " ultimoSeq=" + seqAtual);
        } catch (Exception e) {
            // não se republica: os participantes recuperam pelo ultimoSeq do lote seguinte
            System.out.println("ERRO na publicação do lote hb=" + hbAtual + " (" + lote.size()
                    + " registos): " + e.getMessage());
        }
    }
}
