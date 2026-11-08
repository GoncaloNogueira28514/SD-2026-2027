package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

// Ficheiro de registos de um nó: no-p<k>-n<i>.txt, uma linha <seq>;<termo>;<sensor>;… por registo
public class RecordFile {
    private final Path path;
    private final int n; // dimensão dos relógios vetoriais

    public RecordFile(String fileName, int n) {
        this.path = Paths.get(fileName);
        this.n = n;
    }

    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // todos os registos, pela ordem do ficheiro
    public synchronized List<SensorRecord> readAll() throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        if (!Files.exists(path)) {
            return out;
        }
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                out.add(SensorRecord.fromLine(line, n));
            }
        }
        return out;
    }

    public synchronized SensorRecord last() throws IOException {
        List<SensorRecord> all = readAll();
        return all.isEmpty() ? null : all.get(all.size() - 1);
    }

    public synchronized long lastSeq() throws IOException {
        SensorRecord lastRecord = last();
        return lastRecord == null ? 0 : lastRecord.getSeq();
    }

    // registos com from <= seq <= to (servidos na recuperação por TCP)
    public synchronized List<SensorRecord> range(long from, long to) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        for (SensorRecord r : readAll()) {
            if (r.getSeq() >= from && r.getSeq() <= to) {
                out.add(r);
            }
        }
        return out;
    }

    // Reescreve o ficheiro com os registos indicados, de forma atómica: escreve um ficheiro temporário e
    // substitui o original com ATOMIC_MOVE. Quem ler o ficheiro vê a versão antiga ou a nova, nunca metade.
    // Usado na truncagem que resolve uma divergência.
    public synchronized void rewrite(List<SensorRecord> records) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (SensorRecord r : records) {
            sb.append(r.toLine()).append(System.lineSeparator());
        }
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8);
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
