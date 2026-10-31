package rep04;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionReplica {
    private static Path monitorFile;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: java rep04.PartitionReplica <id> <partições> [portoApi] [N]");
            System.exit(1);
        }

        int id = Integer.parseInt(args[0]);
        String[] particoes = args[1].split(","); // ex.: "0,1"
        String apiPort = args.length > 2 ? args[2] : "5001";
        String api = "http://127.0.0.1:" + apiPort + "/api/v0";
        // dimensão dos relógios vetoriais: número total de partições, não o das que a réplica guarda
        int n = args.length > 3 ? Integer.parseInt(args[3]) : 3;

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

        monitorFile = Paths.get("monitor-replica" + id + ".txt");
        IpfsPubSub ps = new IpfsPubSub(api);
        System.out.println("Réplica " + id + " iniciada. (API=" + api + ", N=" + n
                + ", Δ=" + Wire.PERIODO_HB_MS + " ms, fator=" + Wire.FATOR_TIMEOUT + ")");

        List<HeartbeatMonitor> monitores = new ArrayList<>();
        for (String p : particoes) {
            int k = Integer.parseInt(p.trim());
            monitores.add(iniciarParticao(id, k, n, ps, perda, descartar));
        }

        // thread de verificação dos detetores
        ScheduledExecutorService verificacao = Executors.newSingleThreadScheduledExecutor();
        verificacao.scheduleAtFixedRate(() -> monitores.forEach(HeartbeatMonitor::check),
                Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    // instante;evento;p<k>;detalhe, na consola e em monitor-replica<r>.txt
    private static synchronized void evento(String linha) {
        String l = System.currentTimeMillis() + ";" + linha;
        System.out.println(l);
        try {
            Files.writeString(monitorFile, l + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception e) {
            System.out.println("Erro ao escrever " + monitorFile + ": " + e.getMessage());
        }
    }

    private static HeartbeatMonitor iniciarParticao(int id, int k, int n, IpfsPubSub ps, double perda,
            long descartar) throws Exception {
        RecordFile file = new RecordFile("replica-" + id + "-p" + k + ".txt", n);
        HeartbeatMonitor monitor = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                PartitionReplica::evento);

        final long[] ultimo = { file.lastSeq() };
        final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        final long[] descartarTarget = { descartar };

        System.out.println("Partição " + k + " -> Último seq: " + ultimo[0]);

        Thread sub = new Thread(() -> {
            try {
                ps.subscribe(Wire.topicData(k), msg -> {
                    try {
                        if (!Wire.type(msg).equals("LOTE")) {
                            throw new IllegalArgumentException("Mensagem não é LOTE");
                        }

                        Batch lote = Batch.decode(msg, n);
                        monitor.onHeartbeat(lote.getInc(), lote.getHb());
                        evento("RECEBIDO;p" + k + ";inc=" + lote.getInc() + ";hb=" + lote.getHb()
                                + ";ultimoSeq=" + lote.getLastSeq() + ";n=" + lote.size()
                                + ";seq=" + lote.seqRange());

                        // os registos aplicam-se mesmo que o lote seja de uma incarnação antiga:
                        // a regra dos duplicados trata os que já existem
                        for (SensorRecord r : lote.getRecords()) {
                            if (descartarTarget[0] == r.getSeq()) {
                                System.out.println("DESCARTADO (simulação): " + r.getSeq());
                                descartarTarget[0] = -1;
                                continue;
                            }
                            if (perda > 0 && Math.random() < perda) {
                                System.out.println("DESCARTADO (simulação): " + r.getSeq());
                                continue;
                            }
                            aplicar(k, n, lote, r, file, retidos, ultimo);
                        }

                        // faltam registos que o líder já numerou (ex.: o último antes de uma falha)
                        if (ultimo[0] < lote.getLastSeq()) {
                            recuperar(k, n, lote.getAddress(), lote.getPort(), ultimo[0] + 1, lote.getLastSeq(),
                                    file, retidos, ultimo);
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
        return monitor;
    }

    // regra da COMINDIR02: aplica em ordem, ignora duplicados, retém os que chegam adiantados
    private static void aplicar(int k, int n, Batch lote, SensorRecord r, RecordFile file,
            TreeMap<Long, SensorRecord> retidos, long[] ultimo) throws Exception {
        long seq = r.getSeq();

        if (seq == ultimo[0] + 1) {
            file.append(r);
            ultimo[0] = seq;
            processarRetidos(file, retidos, ultimo);

        } else if (seq <= ultimo[0] || retidos.containsKey(seq)) {
            System.out.println("DUPLICADO: " + seq);

        } else {
            retidos.put(seq, r);
            System.out.println("EM ESPERA: " + seq);

            if (retidos.size() > Wire.LIMITE_RETIDOS) {
                recuperar(k, n, lote.getAddress(), lote.getPort(), ultimo[0] + 1, retidos.firstKey() - 1,
                        file, retidos, ultimo);
            }
        }
    }

    private static void processarRetidos(RecordFile file, TreeMap<Long, SensorRecord> retidos, long[] ultimo)
            throws Exception {
        while (retidos.containsKey(ultimo[0] + 1)) {
            SensorRecord r = retidos.remove(ultimo[0] + 1);
            file.append(r);
            ultimo[0] = r.getSeq();
        }
    }

    private static void recuperar(int k, int n, String host, int port, long de, long ate, RecordFile file,
            TreeMap<Long, SensorRecord> retidos, long[] ultimo) {
        evento("RECUPERACAO;p" + k + ";de=" + de + ";ate=" + ate);

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
                    SensorRecord r = SensorRecord.fromLine(linha, n);
                    if (r.getSeq() == ultimo[0] + 1) {
                        file.append(r);
                        ultimo[0] = r.getSeq();
                        System.out.println("RECUPERADO: " + r.getSeq());
                    } else if (r.getSeq() > ultimo[0] + 1) {
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
                // os retidos já cobertos pela recuperação passam a duplicados
                retidos.headMap(ultimo[0], true).clear();
                processarRetidos(file, retidos, ultimo);
            } else {
                System.out.println("RECUPERAÇÃO FALHOU: Resposta interrompida antes de FIM");
            }

        } catch (Exception e) {
            System.out.println("RECUPERAÇÃO FALHOU: " + e.getMessage());
        }
    }
}
