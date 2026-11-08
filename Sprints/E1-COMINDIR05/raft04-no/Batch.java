package raft04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// LOTE;<termo>;<lider>;<endereço>;<porto>;<hb>;<ultimoSeq>;<ultimoTermo>;<n>
// <seq>;<termo>;<sensor>;<temperatura>;<instante>;<L>;<V>
// ...
public final class Batch {
    private final long term;                    // termo do líder que publicou o lote
    private final int leader;                   // identificador i do líder no grupo
    private final String address;
    private final int port;
    private final long hb;
    private final long lastSeq;
    private final long lastTerm;                // termo do registo lastSeq (0 se não houver registos)
    private final int size;                     // n do cabeçalho
    private final List<SensorRecord> records;   // vazia quando só se interpretou o cabeçalho

    public Batch(long term, int leader, String address, int port, long hb, long lastSeq, long lastTerm,
            List<SensorRecord> records) {
        this(term, leader, address, port, hb, lastSeq, lastTerm, records.size(), records);
    }

    private Batch(long term, int leader, String address, int port, long hb, long lastSeq, long lastTerm, int size,
            List<SensorRecord> records) {
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.size = size;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(term).append(';').append(leader).append(';')
                .append(address).append(';').append(port).append(';')
                .append(hb).append(';').append(lastSeq).append(';').append(lastTerm).append(';')
                .append(records.size());
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
        return new Batch(header.term, header.leader, header.address, header.port, header.hb, header.lastSeq,
                header.lastTerm, records);
    }

    // interpreta apenas a primeira linha (usado pelo produtor)
    public static Batch decodeHeader(String msg) {
        String first = msg.trim().split("\\r?\\n", 2)[0];
        String[] f = first.split(";");
        if (f.length != 9 || !f[0].equals("LOTE")) {
            throw new IllegalArgumentException("LOTE mal formado: " + first);
        }
        int size = Integer.parseInt(f[8]);
        if (size < 0) {
            throw new IllegalArgumentException("Número de registos negativo: " + first);
        }
        return new Batch(Long.parseLong(f[1]), Integer.parseInt(f[2]), f[3], Integer.parseInt(f[4]),
                Long.parseLong(f[5]), Long.parseLong(f[6]), Long.parseLong(f[7]), size, Collections.emptyList());
    }

    // intervalo de seq transportado, "a..b", ou "-" se o lote não tiver registos
    public String seqRange() {
        if (records.isEmpty()) {
            return "-";
        }
        return records.get(0).getSeq() + ".." + records.get(records.size() - 1).getSeq();
    }

    public long getTerm() {
        return term;
    }

    public int getLeader() {
        return leader;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public long getHb() {
        return hb;
    }

    public long getLastSeq() {
        return lastSeq;
    }

    public long getLastTerm() {
        return lastTerm;
    }

    public int size() {
        return size;
    }

    public List<SensorRecord> getRecords() {
        return records;
    }
}
