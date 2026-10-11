package rep01;

import java.io.IOException;
import java.util.TreeMap;

public class PubSubReplica {

    static final String TOPIC = "rep01-temperaturas";
    static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) throws Exception {
        String id = args.length > 0 ? args[0] : "1";
        String api = args.length > 1 ? args[1] : DEFAULT_API;

        RecordFile file = new RecordFile("replica-" + id + ".txt");
        TreeMap<Long, SensorRecord> pending = new TreeMap<>();

        IpfsPubSub ipfs = new IpfsPubSub(api);

        System.out.println("Réplica PubSub " + id + " pronta. Tópico: " + TOPIC);

        ipfs.subscribe(TOPIC, line -> {
            try {
                long ultimo = file.lastSeq();
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(line);
                } catch (IllegalArgumentException e) {
                    System.out.println("REJEITADO: " + line);
                    return;
                }

                if (r.getSeq() <= ultimo) {
                    System.out.println("DUPLICADO: " + r.getSeq());
                    return;
                } else if (r.getSeq() > ultimo + 1) {
                    System.out.println("EM ESPERA: " + r.getSeq() + " (falta o registo " + (ultimo + 1) + ")");
                    if (pending.containsKey(r.getSeq())) {
                        System.out.println("DUPLICADO: " + r.getSeq());
                    } else {
                        pending.put(r.getSeq(), r);
                    }
                    return;
                }

                file.append(r);
                ultimo = r.getSeq();
                System.out.println("Aplicado: " + r.toLine());

                while (pending.containsKey(ultimo + 1)) {
                    SensorRecord next = pending.remove(ultimo + 1);
                    file.append(next);
                    ultimo = next.getSeq();
                    System.out.println("Aplicado: " + next.toLine());
                }

            } catch (IOException e) {
                System.out.println("IO: " + e.getMessage());
            }
        });
    }
}
