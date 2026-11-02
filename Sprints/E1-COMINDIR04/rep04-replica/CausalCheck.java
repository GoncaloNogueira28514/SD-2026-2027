package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CausalCheck {
    private static final Pattern LEADER_FILE = Pattern.compile("lider-p(\\d+)\\.txt");

    private enum Relation {
        A_BEFORE_B,
        B_BEFORE_A,
        CONCURRENT,
        EQUAL
    }

    private static final class PartitionedRecord {
        private final int partition;
        private final SensorRecord record;

        private PartitionedRecord(int partition, SensorRecord record) {
            this.partition = partition;
            this.record = record;
        }
    }

    private CausalCheck() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: java rep04.CausalCheck <N> <pasta>");
            return;
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

        Path folder = Paths.get(args[1]);
        if (!Files.isDirectory(folder)) {
            System.err.println("Erro: a pasta não existe ou não é uma pasta: " + folder);
            return;
        }

        List<PartitionedRecord> records = readRecords(folder, n);
        Map<Relation, Long> counts = new EnumMap<>(Relation.class);
        for (Relation relation : Relation.values()) {
            counts.put(relation, 0L);
        }

        int vectorViolations = 0;
        int lamportViolations = 0;
        long concurrentWithDifferentLamport = 0;
        long concurrentPairs = 0;

        for (PartitionedRecord record : records) {
            if (record.record.getVector().get(record.partition) != record.record.getSeq()) {
                vectorViolations++;
                System.out.println("VIOLAÇÃO V[k]=seq: p" + record.partition
                        + " seq=" + record.record.getSeq()
                        + " V[" + record.partition + "]="
                        + record.record.getVector().get(record.partition));
            }
        }

        for (int i = 0; i < records.size(); i++) {
            for (int j = i + 1; j < records.size(); j++) {
                PartitionedRecord a = records.get(i);
                PartitionedRecord b = records.get(j);
                if (a.partition == b.partition) {
                    continue;
                }

                Relation relation = classify(a.record, b.record);
                counts.put(relation, counts.get(relation) + 1);

                if (relation == Relation.A_BEFORE_B
                        && a.record.getLamport() >= b.record.getLamport()) {
                    lamportViolations++;
                    System.out.println("VIOLAÇÃO Lamport: a -> b mas L(a)="
                            + a.record.getLamport() + " e L(b)=" + b.record.getLamport());
                } else if (relation == Relation.B_BEFORE_A
                        && b.record.getLamport() >= a.record.getLamport()) {
                    lamportViolations++;
                    System.out.println("VIOLAÇÃO Lamport: b -> a mas L(b)="
                            + b.record.getLamport() + " e L(a)=" + a.record.getLamport());
                } else if (relation == Relation.CONCURRENT) {
                    concurrentPairs++;
                    if (a.record.getLamport() != b.record.getLamport()) {
                        concurrentWithDifferentLamport++;
                    }
                }
            }
        }

        double proportion = concurrentPairs == 0
                ? 0.0
                : (double) concurrentWithDifferentLamport / concurrentPairs;

        System.out.println("Registos: " + records.size());
        System.out.println("Violações V[k]=seq: " + vectorViolations);
        System.out.println("a -> b: " + counts.get(Relation.A_BEFORE_B));
        System.out.println("b -> a: " + counts.get(Relation.B_BEFORE_A));
        System.out.println("Concorrentes: " + concurrentPairs);
        System.out.println("Pares concorrentes com L distinto: "
                + concurrentWithDifferentLamport);
        System.out.println("Proporção: " + proportion);
        System.out.println("Violações da condição de Lamport: " + lamportViolations);
        if (counts.get(Relation.EQUAL) > 0) {
            System.out.println("Vetores iguais: " + counts.get(Relation.EQUAL));
        }

        writeLamportView(folder, records);
    }

    private static List<PartitionedRecord> readRecords(Path folder, int n) throws IOException {
        List<PartitionedRecord> records = new ArrayList<>();
        try (Stream<Path> files = Files.list(folder)) {
            List<Path> leaderFiles = files
                    .filter(Files::isRegularFile)
                    .filter(path -> LEADER_FILE.matcher(path.getFileName().toString()).matches())
                    .sorted(Comparator.comparing(path -> partitionOf(path.getFileName().toString())))
                    .collect(Collectors.toList());

            for (Path file : leaderFiles) {
                int partition = partitionOf(file.getFileName().toString());
                if (partition < 0 || partition >= n) {
                    System.out.println("IGNORADO: partição fora de [0," + n + "): " + file.getFileName());
                    continue;
                }

                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (!line.trim().isEmpty()) {
                        records.add(new PartitionedRecord(
                                partition,
                                SensorRecord.fromLine(line, n)));
                    }
                }
            }
        }
        return records;
    }

    private static int partitionOf(String fileName) {
        Matcher matcher = LEADER_FILE.matcher(fileName);
        if (!matcher.matches()) {
            return -1;
        }
        return Integer.parseInt(matcher.group(1));
    }

    private static Relation classify(SensorRecord a, SensorRecord b) {
        VectorClock.Order order = a.getVector().compare(b.getVector());
        switch (order) {
            case BEFORE:
                return Relation.A_BEFORE_B;
            case AFTER:
                return Relation.B_BEFORE_A;
            case CONCURRENT:
                return Relation.CONCURRENT;
            case EQUAL:
                return Relation.EQUAL;
            default:
                throw new IllegalStateException("Relação vetorial desconhecida: " + order);
        }
    }

    private static void writeLamportView(Path folder, List<PartitionedRecord> records) throws IOException {
        List<PartitionedRecord> sorted = new ArrayList<>(records);
        sorted.sort(Comparator
                .comparingLong((PartitionedRecord item) -> item.record.getLamport())
                .thenComparingInt(item -> item.partition)
                .thenComparingLong(item -> item.record.getSeq()));

        List<String> lines = new ArrayList<>();
        for (PartitionedRecord item : sorted) {
            lines.add("p" + item.partition + ";" + item.record.toLine());
        }
        Files.write(folder.resolve("vista-lamport.txt"), lines, StandardCharsets.UTF_8);
    }
}
