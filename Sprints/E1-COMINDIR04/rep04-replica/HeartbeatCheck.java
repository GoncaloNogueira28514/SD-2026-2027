package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// HeartbeatCheck <N> <pasta> [falhas.txt]
// Lê hb-p*.txt, lider-p*.txt e monitor-*.txt (na pasta e nas subpastas) e escreve um relatório
// na consola e em <pasta>/relatorio-heartbeat.txt
public final class HeartbeatCheck {
    private static final Pattern HB_FILE = Pattern.compile("hb-p(\\d+)\\.txt");
    private static final Pattern LEADER_FILE = Pattern.compile("lider-p(\\d+)\\.txt");
    private static final Pattern MONITOR_FILE = Pattern.compile("monitor-(.+)\\.txt");
    private static final Pattern FAILURE_LINE = Pattern.compile("p(\\d+);(\\d+)");

    // inc;hb;instante;ultimoSeq;n;bytes
    private static final class Heartbeat {
        final int partition;
        final long inc, hb, instant, lastSeq;
        final int n, bytes;

        Heartbeat(int partition, String line) {
            String[] f = line.trim().split(";");
            if (f.length != 6) {
                throw new IllegalArgumentException("Linha de heartbeat mal formada: " + line);
            }
            this.partition = partition;
            this.inc = Long.parseLong(f[0]);
            this.hb = Long.parseLong(f[1]);
            this.instant = Long.parseLong(f[2]);
            this.lastSeq = Long.parseLong(f[3]);
            this.n = Integer.parseInt(f[4]);
            this.bytes = Integer.parseInt(f[5]);
        }
    }

    // instante;evento;p<k>;detalhe
    private static final class Event {
        final String participant;
        final long instant;
        final String type;
        final int partition;
        final Map<String, String> detail = new HashMap<>();

        Event(String participant, String line) {
            String[] f = line.trim().split(";");
            if (f.length < 3 || !f[2].startsWith("p")) {
                throw new IllegalArgumentException("Evento mal formado: " + line);
            }
            this.participant = participant;
            this.instant = Long.parseLong(f[0]);
            this.type = f[1];
            this.partition = Integer.parseInt(f[2].substring(1));
            for (int i = 3; i < f.length; i++) {
                int eq = f[i].indexOf('=');
                if (eq > 0) {
                    detail.put(f[i].substring(0, eq), f[i].substring(eq + 1));
                }
            }
        }

        long num(String key) {
            return Long.parseLong(detail.get(key));
        }
    }

    private static final class Stats {
        long count, sum, max = Long.MIN_VALUE, min = Long.MAX_VALUE;

        void add(long v) {
            count++;
            sum += v;
            max = Math.max(max, v);
            min = Math.min(min, v);
        }

        String avg() {
            return count == 0 ? "-" : String.format("%.1f", (double) sum / count);
        }

        String max() {
            return count == 0 ? "-" : String.valueOf(max);
        }

        String min() {
            return count == 0 ? "-" : String.valueOf(min);
        }
    }

    private static final List<String> out = new ArrayList<>();

    private HeartbeatCheck() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: java rep04.HeartbeatCheck <N> <pasta> [falhas.txt]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        Path folder = Paths.get(args[1]);
        if (!Files.isDirectory(folder)) {
            System.err.println("Erro: a pasta não existe ou não é uma pasta: " + folder);
            return;
        }

        List<Path> files;
        try (Stream<Path> walk = Files.walk(folder, 3)) {
            files = walk.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
        }

        List<Heartbeat> heartbeats = new ArrayList<>();
        Map<Integer, TreeMap<Long, SensorRecord>> leaderRecords = new TreeMap<>();
        List<Event> events = new ArrayList<>();
        for (Path file : files) {
            String name = file.getFileName().toString();
            Matcher m;
            if ((m = HB_FILE.matcher(name)).matches()) {
                int k = Integer.parseInt(m.group(1));
                for (String line : lines(file)) {
                    heartbeats.add(new Heartbeat(k, line));
                }
            } else if ((m = LEADER_FILE.matcher(name)).matches()) {
                int k = Integer.parseInt(m.group(1));
                TreeMap<Long, SensorRecord> map = leaderRecords.computeIfAbsent(k, x -> new TreeMap<>());
                for (String line : lines(file)) {
                    SensorRecord r = SensorRecord.fromLine(line, n);
                    map.put(r.getSeq(), r);
                }
            } else if ((m = MONITOR_FILE.matcher(name)).matches()) {
                for (String line : lines(file)) {
                    try {
                        events.add(new Event(m.group(1), line));
                    } catch (RuntimeException e) {
                        System.out.println("IGNORADO (" + name + "): " + e.getMessage());
                    }
                }
            }
        }
        heartbeats.sort(Comparator.comparingInt((Heartbeat h) -> h.partition)
                .thenComparingLong(h -> h.instant));
        events.sort(Comparator.comparingLong(e -> e.instant));

        List<long[]> failures = new ArrayList<>(); // { partição, instante }
        if (args.length == 3) {
            for (String line : lines(Paths.get(args[2]))) {
                Matcher m = FAILURE_LINE.matcher(line.trim());
                if (m.matches()) {
                    failures.add(new long[] { Long.parseLong(m.group(1)), Long.parseLong(m.group(2)) });
                } else {
                    System.out.println("IGNORADO (falhas): " + line);
                }
            }
        }

        long periodo = Wire.PERIODO_HB_MS;
        long timeout = periodo * Wire.FATOR_TIMEOUT;
        print("HeartbeatCheck · pasta " + folder.toAbsolutePath());
        print("Δ = " + periodo + " ms, TIMEOUT = " + timeout + " ms, verificação = " + Wire.VERIFICACAO_MS
                + " ms → tempo de deteção previsto em ]" + (timeout - periodo) + ", "
                + (timeout + Wire.VERIFICACAO_MS) + "] ms");
        print("Ficheiros: " + heartbeats.size() + " lotes, "
                + leaderRecords.values().stream().mapToInt(Map::size).sum() + " registos de líder, "
                + events.size() + " eventos, " + failures.size() + " falhas anotadas");

        reportHeartbeats(heartbeats);
        reportSuspicions(events, heartbeats, failures);
        reportBatchLatency(events, leaderRecords);
        reportCounters(events);
        reportBytes(heartbeats, leaderRecords);

        Files.write(folder.resolve("relatorio-heartbeat.txt"), out, StandardCharsets.UTF_8);
        System.out.println();
        System.out.println("Relatório escrito em " + folder.resolve("relatorio-heartbeat.txt"));
    }

    // 1. regularidade dos heartbeats e tamanho dos lotes, por partição e incarnação
    private static void reportHeartbeats(List<Heartbeat> heartbeats) {
        section("Lotes por partição e incarnação");
        Map<Integer, Map<Long, List<Heartbeat>>> byPartition = new TreeMap<>();
        for (Heartbeat h : heartbeats) {
            byPartition.computeIfAbsent(h.partition, x -> new LinkedHashMap<>())
                    .computeIfAbsent(h.inc, x -> new ArrayList<>()).add(h);
        }
        for (Map.Entry<Integer, Map<Long, List<Heartbeat>>> p : byPartition.entrySet()) {
            int total = p.getValue().values().stream().mapToInt(List::size).sum();
            print("p" + p.getKey() + ": " + total + " lotes publicados em " + p.getValue().size() + " incarnação(ões)");
            for (Map.Entry<Long, List<Heartbeat>> e : p.getValue().entrySet()) {
                List<Heartbeat> list = e.getValue();
                list.sort(Comparator.comparingLong(h -> h.hb));
                Stats size = new Stats();
                Stats interval = new Stats();
                int empty = 0;
                for (int i = 0; i < list.size(); i++) {
                    Heartbeat h = list.get(i);
                    size.add(h.n);
                    if (h.n == 0) {
                        empty++;
                    }
                    if (i > 0) {
                        interval.add(h.instant - list.get(i - 1).instant);
                    }
                }
                long span = list.get(list.size() - 1).instant - list.get(0).instant;
                String perMinute = span == 0 ? "-"
                        : String.format("%.1f", (list.size() - 1) * 60000.0 / span);
                print(String.format("  inc=%d: %d lotes, vazios %d (%.1f%%), registos/lote média %s máx %s,"
                        + " intervalo média %s ms máx %s ms, lotes/min %s",
                        e.getKey(), list.size(), empty, 100.0 * empty / list.size(),
                        size.avg(), size.max(), interval.avg(), interval.max(), perMinute));
            }
        }
    }

    // 2. e 3. suspeitas (falha real vs falsa suspeita) e tempo de deteção
    private static void reportSuspicions(List<Event> events, List<Heartbeat> heartbeats, List<long[]> failures) {
        section("Suspeitas por participante e partição");
        Map<String, Long> lastInc = new HashMap<>();
        Map<String, Stats> detection = new TreeMap<>();
        Stats detectionAll = new Stats();
        int real = 0, falsas = 0;

        for (int i = 0; i < events.size(); i++) {
            Event e = events.get(i);
            String key = e.participant + " p" + e.partition;
            if ((e.type.equals("RECEBIDO") || e.type.equals("RETOMA")) && e.detail.containsKey("inc")) {
                long inc = e.num("inc");
                if (inc >= lastInc.getOrDefault(key, -1L)) {
                    lastInc.put(key, inc);
                }
            }
            if (!e.type.equals("SUSPEITA")) {
                continue;
            }

            long inc = lastInc.getOrDefault(key, -1L);
            Event retoma = null;
            for (int j = i + 1; j < events.size(); j++) {
                Event x = events.get(j);
                if (x.type.equals("RETOMA") && x.participant.equals(e.participant) && x.partition == e.partition) {
                    retoma = x;
                    break;
                }
            }
            boolean falsa;
            if (heartbeats.stream().anyMatch(h -> h.partition == e.partition)) {
                // houve novo heartbeat da mesma incarnação depois da suspeita?
                falsa = heartbeats.stream().anyMatch(h -> h.partition == e.partition && h.inc == inc
                        && h.instant > e.instant);
            } else {
                falsa = retoma != null && retoma.num("inc") == inc;
            }

            long lastPublished = heartbeats.stream()
                    .filter(h -> h.partition == e.partition && h.instant <= e.instant)
                    .mapToLong(h -> h.instant).max().orElse(-1);
            String silence = lastPublished < 0 ? "-" : (e.instant - lastPublished) + " ms";

            StringBuilder sb = new StringBuilder();
            sb.append("  ").append(key).append(" @").append(e.instant).append(": ")
                    .append(inc < 0 ? "SEM HEARTBEATS RECEBIDOS" : falsa ? "FALSA SUSPEITA" : "FALHA REAL")
                    .append(" (inc=").append(inc).append(", silêncio desde o último publicado ").append(silence)
                    .append(", detetor ").append(e.detail.getOrDefault("silencio_ms", "-")).append(" ms");
            if (retoma != null) {
                sb.append(", RETOMA após ").append(retoma.instant - e.instant).append(" ms com inc=")
                        .append(retoma.detail.get("inc"));
            }
            sb.append(')');

            if (falsa) {
                falsas++;
            } else if (inc >= 0) {
                real++;
                long failure = failures.stream()
                        .filter(f -> f[0] == e.partition && f[1] <= e.instant)
                        .mapToLong(f -> f[1]).max().orElse(-1);
                if (failure >= 0) {
                    long t = e.instant - failure;
                    sb.append(" · tempo de deteção ").append(t).append(" ms");
                    detection.computeIfAbsent(key, x -> new Stats()).add(t);
                    detectionAll.add(t);
                }
            }
            print(sb.toString());
        }
        print("Falhas reais: " + real + " · falsas suspeitas: " + falsas);

        if (!failures.isEmpty()) {
            section("Tempo de deteção das falhas reais (desde o instante anotado em falhas.txt)");
            for (Map.Entry<String, Stats> d : detection.entrySet()) {
                Stats s = d.getValue();
                print("  " + d.getKey() + ": " + s.count + " falha(s), mín " + s.min() + " ms, média " + s.avg()
                        + " ms, máx " + s.max() + " ms");
            }
            print("  Global: mín " + detectionAll.min() + " ms, média " + detectionAll.avg() + " ms, máx "
                    + detectionAll.max() + " ms");
        }
    }

    // 4. latência de lote: instante do RECEBIDO que contém o registo menos o instante do registo
    private static void reportBatchLatency(List<Event> events, Map<Integer, TreeMap<Long, SensorRecord>> leader) {
        section("Latência de lote (RECEBIDO − instante do registo)");
        Map<Integer, Stats> byPartition = new TreeMap<>();
        for (Event e : events) {
            String range = e.detail.get("seq");
            if (!e.type.equals("RECEBIDO") || range == null || !range.contains("..")) {
                continue;
            }
            TreeMap<Long, SensorRecord> records = leader.get(e.partition);
            if (records == null) {
                continue;
            }
            String[] ab = range.split("\\.\\.");
            for (SensorRecord r : records.subMap(Long.parseLong(ab[0]), true, Long.parseLong(ab[1]), true).values()) {
                byPartition.computeIfAbsent(e.partition, x -> new Stats()).add(e.instant - epochMillis(r));
            }
        }
        if (byPartition.isEmpty()) {
            print("  (sem RECEBIDO com registos ou sem lider-p*.txt)");
        }
        for (Map.Entry<Integer, Stats> p : byPartition.entrySet()) {
            print("  p" + p.getKey() + ": " + p.getValue().count + " receções, média " + p.getValue().avg()
                    + " ms, máx " + p.getValue().max() + " ms");
        }
    }

    // 5. heartbeats perdidos e recuperações
    private static void reportCounters(List<Event> events) {
        section("HB_PERDIDO e RECUPERACAO");
        Map<String, int[]> counts = new TreeMap<>();
        for (Event e : events) {
            int[] c = counts.computeIfAbsent(e.participant + " p" + e.partition, x -> new int[2]);
            if (e.type.equals("HB_PERDIDO")) {
                c[0]++;
            } else if (e.type.equals("RECUPERACAO")) {
                c[1]++;
            }
        }
        int hbLost = 0, recov = 0;
        for (Map.Entry<String, int[]> c : counts.entrySet()) {
            if (c.getValue()[0] + c.getValue()[1] > 0) {
                print("  " + c.getKey() + ": HB_PERDIDO " + c.getValue()[0] + ", RECUPERACAO " + c.getValue()[1]);
            }
            hbLost += c.getValue()[0];
            recov += c.getValue()[1];
        }
        print("  Total: HB_PERDIDO " + hbLost + ", RECUPERACAO " + recov);
    }

    // 6. bytes por registo: em LOTE (com a parte do cabeçalho) vs REG individual da COMINDIR03
    private static void reportBytes(List<Heartbeat> heartbeats, Map<Integer, TreeMap<Long, SensorRecord>> leader) {
        section("Bytes por registo e mensagens no tópico de dados");
        long batchBytes = 0, batchRecords = 0;
        for (Heartbeat h : heartbeats) {
            if (h.n > 0) {
                batchBytes += h.bytes;
                batchRecords += h.n;
            }
        }
        long regBytes = 0, regCount = 0;
        for (Map.Entry<Integer, TreeMap<Long, SensorRecord>> p : leader.entrySet()) {
            // REG;<endereço>;<porto>;<registo>, com o endereço e o porto por omissão do líder
            String prefix = "REG;127.0.0.1;" + (Wire.PORTO_BASE + p.getKey()) + ";";
            for (SensorRecord r : p.getValue().values()) {
                regBytes += (prefix + r.toLine()).getBytes(StandardCharsets.UTF_8).length;
                regCount++;
            }
        }
        print("  Em LOTE (lotes não vazios): " + (batchRecords == 0 ? "-"
                : String.format("%.1f", (double) batchBytes / batchRecords)) + " bytes/registo");
        print("  REG individual equivalente: " + (regCount == 0 ? "-"
                : String.format("%.1f", (double) regBytes / regCount)) + " bytes/registo");
        long batchesWithLeader = heartbeats.stream().filter(h -> leader.containsKey(h.partition)).count();
        print("  Mensagens no tópico de dados: COMINDIR03 (um REG por registo) " + regCount
                + " / COMINDIR04 (lotes) " + batchesWithLeader);
        TreeSet<Integer> partitions = new TreeSet<>(leader.keySet());
        for (int k : partitions) {
            long batches = heartbeats.stream().filter(h -> h.partition == k).count();
            print("    p" + k + ": " + leader.get(k).size() + " registos, " + batches + " lotes");
        }
    }

    private static long epochMillis(SensorRecord r) {
        return LocalDateTime.parse(r.getTimestamp()).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    private static List<String> lines(Path file) throws IOException {
        return Files.readAllLines(file, StandardCharsets.UTF_8).stream()
                .filter(l -> !l.isBlank())
                .collect(Collectors.toList());
    }

    private static void section(String title) {
        print("");
        print("== " + title + " ==");
    }

    private static void print(String line) {
        System.out.println(line);
        out.add(line);
    }
}
