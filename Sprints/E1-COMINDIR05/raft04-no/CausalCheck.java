package raft04;

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
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// CausalCheck <N> <pasta>
// 1. Verifica V[k] = seq em todos os registos de todos os nós (no-p<k>-n<i>.txt), incluindo os criados
//    por líderes de termos diferentes: confirma que o invariante se mantém através das mudanças de líder.
// 2. Para a análise causal entre partições usa, de cada partição, o registo mais atualizado (maior par
//    (ultimoTermo, ultimoSeq), que é o do líder pela restrição de voto) e escreve vista-lamport.txt.
public final class CausalCheck {
    private static final Pattern NODE_FILE = Pattern.compile("no-p(\\d+)-n(\\d+)\\.txt");

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
            System.err.println("Uso: java raft04.CausalCheck <N> <pasta>");
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

        // partição -> nó -> registos
        Map<Integer, Map<Integer, List<SensorRecord>>> logs = readLogs(folder, n);

        // 1. V[k] = seq em todos os nós
        int vectorViolations = 0;
        int checked = 0;
        for (Map.Entry<Integer, Map<Integer, List<SensorRecord>>> p : logs.entrySet()) {
            for (Map.Entry<Integer, List<SensorRecord>> node : p.getValue().entrySet()) {
                for (SensorRecord r : node.getValue()) {
                    checked++;
                    if (r.getVector().get(p.getKey()) != r.getSeq()) {
                        vectorViolations++;
                        if (vectorViolations <= 20) {
                            System.out.println("VIOLAÇÃO V[k]=seq: p" + p.getKey() + " n" + node.getKey()
                                    + " seq=" + r.getSeq() + " termo=" + r.getTerm()
                                    + " V[" + p.getKey() + "]=" + r.getVector().get(p.getKey()));
                        }
                    }
                }
            }
        }

        // 2. registo de referência de cada partição
        List<PartitionedRecord> records = new ArrayList<>();
        for (Map.Entry<Integer, Map<Integer, List<SensorRecord>>> p : logs.entrySet()) {
            int best = -1;
            List<SensorRecord> bestLog = null;
            for (Map.Entry<Integer, List<SensorRecord>> node : p.getValue().entrySet()) {
                if (bestLog == null || moreUpToDate(node.getValue(), bestLog)) {
                    best = node.getKey();
                    bestLog = node.getValue();
                }
            }
            if (bestLog != null) {
                System.out.println("Referência de p" + p.getKey() + ": n" + best + " (" + bestLog.size()
                        + " registos, termos " + termsOf(bestLog) + ")");
                for (SensorRecord r : bestLog) {
                    records.add(new PartitionedRecord(p.getKey(), r));
                }
            }
        }

        Map<Relation, Long> counts = new EnumMap<>(Relation.class);
        for (Relation relation : Relation.values()) {
            counts.put(relation, 0L);
        }
        int lamportViolations = 0;
        long concurrentWithDifferentLamport = 0;
        long concurrentPairs = 0;

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

        System.out.println("Registos verificados (todos os nós): " + checked);
        System.out.println("Violações V[k]=seq: " + vectorViolations);
        System.out.println("Registos na análise causal: " + records.size());
        System.out.println("a -> b: " + counts.get(Relation.A_BEFORE_B));
        System.out.println("b -> a: " + counts.get(Relation.B_BEFORE_A));
        System.out.println("Concorrentes: " + concurrentPairs);
        System.out.println("Pares concorrentes com L distinto: " + concurrentWithDifferentLamport);
        System.out.println("Proporção: " + proportion);
        System.out.println("Violações da condição de Lamport: " + lamportViolations);
        if (counts.get(Relation.EQUAL) > 0) {
            System.out.println("Vetores iguais: " + counts.get(Relation.EQUAL));
        }

        writeLamportView(folder, records);
    }

    // (ultimoTermo, ultimoSeq) de a é maior do que o de b
    private static boolean moreUpToDate(List<SensorRecord> a, List<SensorRecord> b) {
        long ta = a.isEmpty() ? 0 : a.get(a.size() - 1).getTerm();
        long tb = b.isEmpty() ? 0 : b.get(b.size() - 1).getTerm();
        return ta > tb || (ta == tb && a.size() > b.size());
    }

    private static String termsOf(List<SensorRecord> log) {
        return log.stream().map(r -> String.valueOf(r.getTerm())).distinct().collect(Collectors.joining(","));
    }

    private static Map<Integer, Map<Integer, List<SensorRecord>>> readLogs(Path folder, int n) throws IOException {
        Map<Integer, Map<Integer, List<SensorRecord>>> logs = new TreeMap<>();
        try (Stream<Path> files = Files.list(folder)) {
            List<Path> nodeFiles = files
                    .filter(Files::isRegularFile)
                    .filter(path -> NODE_FILE.matcher(path.getFileName().toString()).matches())
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .collect(Collectors.toList());

            for (Path file : nodeFiles) {
                Matcher m = NODE_FILE.matcher(file.getFileName().toString());
                m.matches();
                int partition = Integer.parseInt(m.group(1));
                int node = Integer.parseInt(m.group(2));
                if (partition < 0 || partition >= n) {
                    System.out.println("IGNORADO: partição fora de [0," + n + "): " + file.getFileName());
                    continue;
                }
                List<SensorRecord> log = new ArrayList<>();
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (!line.trim().isEmpty()) {
                        log.add(SensorRecord.fromLine(line, n));
                    }
                }
                logs.computeIfAbsent(partition, x -> new TreeMap<>()).put(node, log);
            }
        }
        return logs;
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
