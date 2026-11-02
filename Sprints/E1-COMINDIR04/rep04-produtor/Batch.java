package rep04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// LOTE;<endereço>;<porto>;<inc>;<hb>;<ultimoSeq>;<n>
// <seq>;<sensor>;<temperatura>;<instante>;<L>;<V>
// ...
public final class Batch {
    private final String address;
    private final int port;
    private final long inc;
    private final long hb;
    private final long lastSeq;
    private final int size;                     // n do cabeçalho
    private final List<SensorRecord> records;   // vazia quando só se interpretou o cabeçalho

    public Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records) {
        this(address, port, inc, hb, lastSeq, records.size(), records);
    }

    private Batch(String address, int port, long inc, long hb, long lastSeq, int size, List<SensorRecord> records) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.size = size;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(address).append(';').append(port).append(';')
                .append(inc).append(';').append(hb).append(';')
                .append(lastSeq).append(';').append(records.size());
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    // interpreta um LOTE completo; n é a dimensão dos relógios vetoriais
    public static Batch decode(String msg, int n) {
        String[] lines = msg.trim().split("\\r?\\n");
        Batch header = decodeHeader(lines[0]);

        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            if (!lines[i].isBlank()) {
                records.add(SensorRecord.fromLine(lines[i], n));
            }
        }
        if (records.size() != header.size) {
            throw new IllegalArgumentException("LOTE com " + records.size()
                    + " registos, cabeçalho indica " + header.size);
        }
        return new Batch(header.address, header.port, header.inc, header.hb, header.lastSeq, records);
    }

    // interpreta apenas a primeira linha (usado pelo produtor)
    public static Batch decodeHeader(String msg) {
        String first = msg.trim().split("\\r?\\n", 2)[0];
        String[] f = first.split(";");
        if (f.length != 7 || !f[0].equals("LOTE")) {
            throw new IllegalArgumentException("LOTE mal formado: " + first);
        }
        int size = Integer.parseInt(f[6]);
        if (size < 0) {
            throw new IllegalArgumentException("Número de registos negativo: " + first);
        }
        return new Batch(f[1], Integer.parseInt(f[2]), Long.parseLong(f[3]), Long.parseLong(f[4]),
                Long.parseLong(f[5]), size, Collections.emptyList());
    }

    // intervalo de seq transportado, "a..b", ou "-" se o lote não tiver registos
    public String seqRange() {
        if (records.isEmpty()) {
            return "-";
        }
        return records.get(0).getSeq() + ".." + records.get(records.size() - 1).getSeq();
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public long getInc() {
        return inc;
    }

    public long getHb() {
        return hb;
    }

    public long getLastSeq() {
        return lastSeq;
    }

    public int size() {
        return size;
    }

    public List<SensorRecord> getRecords() {
        return records;
    }
}
