package rep04;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AlarmProcessor {
    private static final Path MONITOR_FILE = Paths.get("monitor-processador.txt");

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Uso: java rep04.AlarmProcessor <N> [api]");
            System.exit(1);
        }

        int n;
        try {
            n = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Erro: N deve ser um inteiro válido.");
            return;
        }
        if (n < 1) {
            System.err.println("Erro: N deve ser maior ou igual a 1.");
            return;
        }

        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ps = new IpfsPubSub(api);
        VectorClock commonClock = new VectorClock(n);
        long[] commonLamport = { 0 };
        Object monitor = new Object();
        Set<String> seen = new HashSet<>();

        HeartbeatMonitor[] detetores = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            detetores[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, AlarmProcessor::evento);
        }

        System.out.println("Processador de alarmes iniciado (N = " + n + ", API = " + api
                + ", Δ=" + Wire.PERIODO_HB_MS + " ms, fator=" + Wire.FATOR_TIMEOUT + ")");

        for (int k = 0; k < n; k++) {
            final int partition = k;
            Thread subscription = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(partition), message -> {
                        try {
                            if (!Wire.type(message).equals("LOTE")) {
                                throw new IllegalArgumentException("Mensagem não é LOTE");
                            }
                            Batch lote = Batch.decode(message, n);
                            detetores[partition].onHeartbeat(lote.getInc(), lote.getHb());
                            evento("RECEBIDO;p" + partition + ";inc=" + lote.getInc() + ";hb=" + lote.getHb()
                                    + ";ultimoSeq=" + lote.getLastSeq() + ";n=" + lote.size()
                                    + ";seq=" + lote.seqRange());

                            for (SensorRecord record : lote.getRecords()) {
                                tratar(partition, record, partitioner, ps, commonClock, commonLamport, monitor,
                                        seen, detetores);
                            }
                        } catch (Exception e) {
                            System.out.println("REJEITADO: " + message + " (" + e.getMessage() + ")");
                        }
                    });
                } catch (Exception e) {
                    System.err.println("Erro na subscrição p" + partition + ": " + e.getMessage());
                }
            }, "alarm-processor-p" + partition);
            subscription.start();
        }

        // thread de verificação dos detetores
        ScheduledExecutorService verificacao = Executors.newSingleThreadScheduledExecutor();
        verificacao.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor d : detetores) {
                d.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    // regras da COMINDIR03, aplicadas a cada registo do lote
    private static void tratar(int partition, SensorRecord record, Partitioner partitioner, IpfsPubSub ps,
            VectorClock commonClock, long[] commonLamport, Object monitor, Set<String> seen,
            HeartbeatMonitor[] detetores) throws Exception {
        String duplicateKey = partition + ":" + record.getSeq();

        boolean alarm;
        VectorClock publicationClock = null;
        long publicationLamport = 0;
        synchronized (monitor) {
            if (!seen.add(duplicateKey)) {
                System.out.println("DUPLICADO: p" + partition + " seq=" + record.getSeq());
                return;
            }

            commonLamport[0] = Math.max(commonLamport[0], record.getLamport());
            commonClock.merge(record.getVector());
            alarm = record.getTemperature() >= Wire.LIMIAR_ALARME
                    && !record.getSensorId().startsWith("ALARM-");
            if (alarm) {
                publicationLamport = commonLamport[0];
                publicationClock = commonClock.copy();
            }
        }

        if (alarm) {
            String alarmSensor = "ALARM-" + record.getSensorId();
            int destination = partitioner.of(alarmSensor);
            if (detetores[destination].state() == HeartbeatMonitor.State.SUSPEITO) {
                System.out.println("AVISO: líder p" + destination + " suspeito; alarme " + alarmSensor
                        + " pode perder-se");
            }
            String alarmMessage = Wire.in(alarmSensor, record.getTemperature(),
                    publicationLamport, publicationClock);
            ps.publish(Wire.topicIn(destination), alarmMessage);
            System.out.println("ALARME: " + alarmSensor + " -> p" + destination
                    + " V=" + publicationClock);
        }
    }

    // instante;evento;p<k>;detalhe, na consola e em monitor-processador.txt
    private static synchronized void evento(String linha) {
        String l = System.currentTimeMillis() + ";" + linha;
        System.out.println(l);
        try {
            Files.writeString(MONITOR_FILE, l + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception e) {
            System.out.println("Erro ao escrever " + MONITOR_FILE + ": " + e.getMessage());
        }
    }
}
