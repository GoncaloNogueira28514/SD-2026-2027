package rep04;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class SensorRecord {
    private static final DateTimeFormatter INSTANTE = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, String sensorId, double temperature, String timestamp, long lamport, VectorClock vector) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        // Cópia defensiva na entrada para garantir imutabilidade
        this.vector = vector.copy(); 
    }

    // instante com precisão ao milissegundo, para que HeartbeatCheck possa medir a latência de lote
    public static SensorRecord now(long seq, String sensorId, double temperature, long lamport, VectorClock vector) {
        LocalDateTime t = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
        return new SensorRecord(seq, sensorId, temperature, t.format(INSTANTE), lamport, vector);
    }

    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector.toString();
    }

    public static SensorRecord fromLine(String line, int n) {
        String[] f = line.trim().split(";");
        if (f.length != 6) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(Long.parseLong(f[0]), f[1], Double.parseDouble(f[2]), f[3], Long.parseLong(f[4]), VectorClock.parse(f[5], n));
    }

    public long getSeq() {
        return seq;
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