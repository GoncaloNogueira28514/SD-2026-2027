package rep03;

import java.time.LocalDateTime;

public class SensorRecord {
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

    public static SensorRecord now(long seq, String sensorId, double temperature, long lamport, VectorClock vector) {
        return new SensorRecord(seq, sensorId, temperature, LocalDateTime.now().withNano(0).toString(), lamport, vector);
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