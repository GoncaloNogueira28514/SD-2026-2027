package rep03;

import java.util.HashSet;
import java.util.Set;

public class AlarmProcessor {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Uso: java rep03.AlarmProcessor <N> [api]");
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

        System.out.println("Processador de alarmes iniciado (N = " + n + ", API = " + api + ")");

        for (int k = 0; k < n; k++) {
            final int partition = k;
            Thread subscription = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(partition), message -> {
                        try {
                            String[] parts = Wire.regParts(message);
                            SensorRecord record = SensorRecord.fromLine(parts[2], n);
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
                                String alarmMessage = Wire.in(alarmSensor, record.getTemperature(),
                                        publicationLamport, publicationClock);
                                ps.publish(Wire.topicIn(destination), alarmMessage);
                                System.out.println("ALARME: " + alarmSensor + " -> p" + destination
                                        + " V=" + publicationClock);
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
    }
}
