package raft04;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

// <seq>;<termo>;<sensor>;<temperatura>;<instante>;<L>;<V>
public class SensorRecord {
    private static final DateTimeFormatter INSTANTE = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private final long seq;
    private final long term;          // termo em que o líder criou o registo
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, long term, String sensorId, double temperature, String timestamp, long lamport,
            VectorClock vector) {
        this.seq = seq;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        // Cópia defensiva na entrada para garantir imutabilidade
        this.vector = vector.copy();
    }

    // instante com precisão ao milissegundo
    public static SensorRecord now(long seq, long term, String sensorId, double temperature, long lamport,
            VectorClock vector) {
        LocalDateTime t = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        return new SensorRecord(seq, term, sensorId, temperature, t.format(INSTANTE), lamport, vector);
    }

    public String toLine() {
        return seq + ";" + term + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";"
                + vector.toString();
    }

    public static SensorRecord fromLine(String line, int n) {
        String[] f = line.trim().split(";");
        if (f.length != 7) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(Long.parseLong(f[0]), Long.parseLong(f[1]), f[2], Double.parseDouble(f[3]), f[4],
                Long.parseLong(f[5]), VectorClock.parse(f[6], n));
    }

    // dois registos com o mesmo seq e o mesmo termo são o mesmo registo
    public boolean sameEntry(SensorRecord other) {
        return seq == other.seq && term == other.term;
    }

    // identifica a leitura independentemente do seq e do termo que lhe foram atribuídos
    public String reading() {
        return sensorId + "@" + temperature;
    }

    public long getSeq() {
        return seq;
    }

    public long getTerm() {
        return term;
    }

    public String getSensorId() {
        return sensorId;
    }

    public double getTemperature() {
        return temperature;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public long getLamport() {
        return lamport;
    }

    public VectorClock getVector() {
        // Cópia defensiva na saída para proteger o carimbo do registo
        return vector.copy();
    }
}
