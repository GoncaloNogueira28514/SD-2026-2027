package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// RaftCheck <N> <pasta> [falhas.txt]
//
// Lê no-*, estado-*, hb-*, eventos-*, monitor-* e produtor.txt de uma pasta e escreve um relatório na
// consola e em <pasta>/relatorio-raft.txt: termos e eleições, segurança da eleição, tempo até ao primeiro
// líder, lideranças simultâneas, indisponibilidade de cada falha, leituras perdidas, truncagens e
// coincidência final dos registos. Use uma pasta limpa por ensaio (ver o documento da tarefa).
public final class RaftCheck {
    private static final Pattern NODE_FILE = Pattern.compile("no-p(\\d+)-n(\\d+)\\.txt");
    private static final Pattern STATE_FILE = Pattern.compile("estado-p(\\d+)-n(\\d+)\\.txt");
    private static final Pattern HB_FILE = Pattern.compile("hb-p(\\d+)-n(\\d+)\\.txt");
    private static final Pattern EVENT_FILE = Pattern.compile("eventos-p(\\d+)-n(\\d+)\\.txt");
    private static final Pattern MONITOR_FILE = Pattern.compile("monitor-(.+)\\.txt");
    private static final Pattern PARTITION = Pattern.compile("p(\\d+)");
    private static final Pattern NODE = Pattern.compile("n(\\d+)");

    // instante;evento;p<k>;detalhe
    private static final class Ev {
        final long t;
        final String type;
        final int p;
        final int node;               // 0 nos ficheiros de monitorização
        final String who;             // n<i> ou o id do monitor
        final List<String> flags = new ArrayList<>();
        final Map<String, String> kv = new HashMap<>();
        final long order;             // ordem de leitura, para desempatar instantes iguais

        Ev(String line, int node, String who, long order) {
            String[] f = line.trim().split(";");
            if (f.length < 3) {
                throw new IllegalArgumentException("Evento mal formado: " + line);
            }
            this.t = Long.parseLong(f[0]);
            this.type = f[1];
            Matcher m = PARTITION.matcher(f[2]);
            this.p = m.matches() ? Integer.parseInt(m.group(1)) : -1;
            this.node = node;
            this.who = who;
            this.order = order;
            for (int i = 3; i < f.length; i++) {
                int eq = f[i].indexOf('=');
                if (eq > 0) {
                    kv.put(f[i].substring(0, eq), f[i].substring(eq + 1));
                } else {
                    flags.add(f[i]);
                }
            }
        }

        long num(String key) {
            String v = kv.get(key);
            return v == null ? -1 : Long.parseLong(v);
        }

        int nodeRef(String key) {
            String v = kv.get(key);
            if (v == null) return 0;
            Matcher m = NODE.matcher(v);
            return m.matches() ? Integer.parseInt(m.group(1)) : 0;
        }
    }

    // termo;hb;instante;ultimoSeq;ultimoTermo;n;bytes
    private static final class Hb {
        final int p, node;
        final long term, hb, t, lastSeq, lastTerm;
        final int n, bytes;

        Hb(int p, int node, String line) {
            String[] f = line.trim().split(";");
            if (f.length != 7) {
                throw new IllegalArgumentException("Linha de heartbeat mal formada: " + line);
            }
            this.p = p;
            this.node = node;
            this.term = Long.parseLong(f[0]);
            this.hb = Long.parseLong(f[1]);
            this.t = Long.parseLong(f[2]);
            this.lastSeq = Long.parseLong(f[3]);
            this.lastTerm = Long.parseLong(f[4]);
            this.n = Integer.parseInt(f[5]);
            this.bytes = Integer.parseInt(f[6]);
        }
    }

    // instante;p<k>;sensor;temperatura
    private static final class Sent {
        final long t;
        final int p;
        final String reading;

        Sent(String line) {
            String[] f = line.trim().split(";");
            if (f.length != 4) {
                throw new IllegalArgumentException("Linha de produtor.txt mal formada: " + line);
            }
            this.t = Long.parseLong(f[0]);
            Matcher m = PARTITION.matcher(f[1]);
            this.p = m.matches() ? Integer.parseInt(m.group(1)) : -1;
            this.reading = f[2] + "@" + Double.parseDouble(f[3]);
        }
    }

    // p<k>;n<i>;instante
    private static final class Failure {
        final int p, node;
        final long t;

        Failure(String line) {
            String[] f = line.trim().split(";");
            Matcher mp = PARTITION.matcher(f[0]);
            if (!mp.matches()) {
                throw new IllegalArgumentException("Linha de falhas mal formada: " + line);
            }
            this.p = Integer.parseInt(mp.group(1));
            if (f.length >= 3) {
                Matcher mn = NODE.matcher(f[1]);
                this.node = mn.matches() ? Integer.parseInt(mn.group(1)) : 0;
                this.t = Long.parseLong(f[2]);
            } else {
                this.node = 0;
                this.t = Long.parseLong(f[1]);
            }
        }
    }

    // liderança de um nó num termo
    private static final class Leadership {
        final int p, node;
        final long term, start;
        long end;
        String endReason;

        Leadership(int p, int node, long term, long start) {
            this.p = p;
            this.node = node;
            this.term = term;
            this.start = start;
        }
    }

    private static final class TermInfo {
        final Set<Integer> candidates = new TreeSet<>();
        final Map<Integer, Set<Integer>> granted = new TreeMap<>();
        final Map<Integer, Set<Integer>> refused = new TreeMap<>();
        final Set<Integer> leaders = new TreeSet<>();
        long firstCandidate = Long.MAX_VALUE;
        long leaderAt = Long.MAX_VALUE;
    }

    private final int n;
    private final Path folder;
    private final StringBuilder out = new StringBuilder();
    private long t0 = Long.MAX_VALUE;

    private final Map<Integer, Map<Integer, List<SensorRecord>>> logs = new TreeMap<>();
    private final Map<Integer, Map<Integer, long[]>> states = new TreeMap<>();
    private final List<Hb> hbs = new ArrayList<>();
    private final List<Ev> nodeEvents = new ArrayList<>();
    private final Map<String, List<Ev>> monitorEvents = new TreeMap<>();
    private final List<Sent> sent = new ArrayList<>();
    private final List<Failure> failures = new ArrayList<>();
    private final Set<Integer> partitions = new TreeSet<>();

    private RaftCheck(int n, Path folder) {
        this.n = n;
        this.folder = folder;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: java raft04.RaftCheck <N> <pasta> [falhas.txt]");
            System.exit(1);
        }
        int n = Integer.parseInt(args[0]);
        Path folder = Paths.get(args[1]);
        if (!Files.isDirectory(folder)) {
            System.err.println("Erro: a pasta não existe: " + folder);
            System.exit(1);
        }
        RaftCheck c = new RaftCheck(n, folder);
        c.read();
        if (args.length > 2) {
            Path f = Paths.get(args[2]);
            if (!Files.exists(f)) {
                f = folder.resolve(args[2]);
            }
            c.readFailures(f);
        }
        c.report();
        Files.writeString(folder.resolve("relatorio-raft.txt"), c.out.toString(), StandardCharsets.UTF_8);
        System.out.println("(relatório escrito em " + folder.resolve("relatorio-raft.txt") + ")");
    }

    // =====================================================================================
    // Leitura
    // =====================================================================================

    private void read() throws IOException {
        List<Path> files;
        try (Stream<Path> s = Files.list(folder)) {
            files = s.filter(Files::isRegularFile).sorted().collect(Collectors.toList());
        }
        long order = 0;
        for (Path f : files) {
            String name = f.getFileName().toString();
            Matcher m;
            if ((m = NODE_FILE.matcher(name)).matches()) {
                int p = Integer.parseInt(m.group(1)), node = Integer.parseInt(m.group(2));
                List<SensorRecord> log = new ArrayList<>();
                for (String line : lines(f)) {
                    log.add(SensorRecord.fromLine(line, n));
                }
                logs.computeIfAbsent(p, x -> new TreeMap<>()).put(node, log);
                partitions.add(p);
            } else if ((m = STATE_FILE.matcher(name)).matches()) {
                int p = Integer.parseInt(m.group(1)), node = Integer.parseInt(m.group(2));
                List<String> l = lines(f);
                if (!l.isEmpty()) {
                    String[] x = l.get(0).split(";");
                    states.computeIfAbsent(p, y -> new TreeMap<>())
                            .put(node, new long[] { Long.parseLong(x[0]), Long.parseLong(x[1]) });
                }
                partitions.add(p);
            } else if ((m = HB_FILE.matcher(name)).matches()) {
                int p = Integer.parseInt(m.group(1)), node = Integer.parseInt(m.group(2));
                for (String line : lines(f)) {
                    hbs.add(new Hb(p, node, line));
                }
                partitions.add(p);
            } else if ((m = EVENT_FILE.matcher(name)).matches()) {
                int p = Integer.parseInt(m.group(1)), node = Integer.parseInt(m.group(2));
                for (String line : lines(f)) {
                    Ev e = new Ev(line, node, "n" + node, order++);
                    nodeEvents.add(e);
                    t0 = Math.min(t0, e.t);
                }
                partitions.add(p);
            } else if ((m = MONITOR_FILE.matcher(name)).matches()) {
                List<Ev> l = new ArrayList<>();
                for (String line : lines(f)) {
                    try {
                        l.add(new Ev(line, 0, m.group(1), order++));
                    } catch (RuntimeException e) {
                        // linha de outro formato: ignorada
                    }
                }
                monitorEvents.put(m.group(1), l);
            } else if (name.equals("produtor.txt")) {
                for (String line : lines(f)) {
                    sent.add(new Sent(line));
                }
            }
        }
        nodeEvents.sort(Comparator.comparingLong((Ev e) -> e.t).thenComparingLong(e -> e.order));
        if (t0 == Long.MAX_VALUE) {
            t0 = 0;
        }
    }

    private void readFailures(Path f) throws IOException {
        for (String line : lines(f)) {
            failures.add(new Failure(line));
        }
        failures.sort(Comparator.comparingLong(x -> x.t));
    }

    private static List<String> lines(Path f) throws IOException {
        return Files.readAllLines(f, StandardCharsets.UTF_8).stream()
                .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    // =====================================================================================
    // Relatório
    // =====================================================================================

    private void report() {
        line("RaftCheck · pasta " + folder.toAbsolutePath() + " · N=" + n);
        line("Instantes relativos a t0 = " + t0 + " (primeiro evento de um nó)");
        for (int p : partitions) {
            Map<Integer, List<SensorRecord>> l = logs.getOrDefault(p, Collections.emptyMap());
            Set<Integer> nodes = new TreeSet<>(l.keySet());
            nodeEvents.stream().filter(e -> e.p == p).forEach(e -> nodes.add(e.node));
            line("  p" + p + ": nós " + nodes.stream().map(i -> "n" + i).collect(Collectors.joining(" ")));
        }

        Map<Integer, TreeMap<Long, TermInfo>> terms = termsByPartition();
        List<Leadership> leaderships = leaderships();

        sectionTerms(terms);
        sectionSafety(terms);
        sectionFirstLeader();
        sectionLeaderships(leaderships);
        sectionFailures(leaderships);
        sectionLost();
        sectionTruncated(terms);
        sectionCoincidence();
        sectionMonitors();
    }

    private Map<Integer, TreeMap<Long, TermInfo>> termsByPartition() {
        Map<Integer, TreeMap<Long, TermInfo>> terms = new TreeMap<>();
        for (Ev e : nodeEvents) {
            TreeMap<Long, TermInfo> tp = terms.computeIfAbsent(e.p, x -> new TreeMap<>());
            switch (e.type) {
                case "CANDIDATO": {
                    TermInfo ti = tp.computeIfAbsent(e.num("termo"), x -> new TermInfo());
                    ti.candidates.add(e.node);
                    ti.firstCandidate = Math.min(ti.firstCandidate, e.t);
                    break;
                }
                case "VOTO": {
                    TermInfo ti = tp.computeIfAbsent(e.num("termo"), x -> new TermInfo());
                    int cand = e.nodeRef("candidato");
                    boolean ok = e.flags.contains("concedido");
                    (ok ? ti.granted : ti.refused).computeIfAbsent(cand, x -> new TreeSet<>()).add(e.node);
                    break;
                }
                case "LIDER": {
                    TermInfo ti = tp.computeIfAbsent(e.num("termo"), x -> new TermInfo());
                    ti.leaders.add(e.node);
                    ti.leaderAt = Math.min(ti.leaderAt, e.t);
                    break;
                }
                default:
                    break;
            }
        }
        return terms;
    }

    // 1 · termos, candidatos, votos e líder
    private void sectionTerms(Map<Integer, TreeMap<Long, TermInfo>> terms) {
        header("1 · Termos e eleições por partição");
        for (Map.Entry<Integer, TreeMap<Long, TermInfo>> tp : terms.entrySet()) {
            line("p" + tp.getKey() + ":");
            int split = 0;
            List<Long> durations = new ArrayList<>();
            for (Map.Entry<Long, TermInfo> te : tp.getValue().entrySet()) {
                TermInfo ti = te.getValue();
                StringBuilder sb = new StringBuilder("  termo " + te.getKey() + ": candidatos "
                        + nodes(ti.candidates));
                for (int c : ti.candidates) {
                    Set<Integer> votes = new TreeSet<>(ti.granted.getOrDefault(c, Collections.emptySet()));
                    votes.add(c);
                    sb.append("; votos de n").append(c).append(" = ").append(nodes(votes));
                    Set<Integer> ref = ti.refused.getOrDefault(c, Collections.emptySet());
                    if (!ref.isEmpty()) {
                        sb.append(" (recusados por ").append(nodes(ref)).append(")");
                    }
                }
                if (!ti.leaders.isEmpty()) {
                    sb.append("; líder ").append(nodes(ti.leaders));
                    if (ti.firstCandidate != Long.MAX_VALUE) {
                        long d = ti.leaderAt - ti.firstCandidate;
                        durations.add(d);
                        sb.append("; eleição ").append(d).append(" ms (CANDIDATO em ").append(rel(ti.firstCandidate))
                                .append(", LIDER em ").append(rel(ti.leaderAt)).append(")");
                    }
                } else if (ti.candidates.size() >= 2) {
                    split++;
                    sb.append("; sem líder: VOTO DIVIDIDO");
                } else if (!ti.candidates.isEmpty()) {
                    sb.append("; sem líder: candidato sem maioria (isolado, nós em falha ou votos recusados)");
                } else {
                    sb.append("; termo só observado (sem CANDIDATO registado)");
                }
                line(sb.toString());
            }
            line("  -> termos: " + tp.getValue().size() + "; votos divididos: " + split
                    + "; duração das eleições (ms) " + stats(durations));
        }
    }

    // 2 · segurança da eleição
    private void sectionSafety(Map<Integer, TreeMap<Long, TermInfo>> terms) {
        header("2 · Segurança da eleição");
        int multi = 0;
        for (Map.Entry<Integer, TreeMap<Long, TermInfo>> tp : terms.entrySet()) {
            for (Map.Entry<Long, TermInfo> te : tp.getValue().entrySet()) {
                if (te.getValue().leaders.size() > 1) {
                    multi++;
                    line("  VIOLAÇÃO: p" + tp.getKey() + " termo " + te.getKey() + " com líderes "
                            + nodes(te.getValue().leaders));
                }
            }
        }
        line("Termos com mais de um LIDER: " + multi + (multi == 0 ? "  (OK)" : "  (FALHA DE SEGURANÇA)"));

        // votos duplos: o mesmo nó vota em dois candidatos no mesmo termo (o voto em si próprio conta)
        Map<String, Set<Integer>> votesBy = new LinkedHashMap<>();
        for (Ev e : nodeEvents) {
            if (e.type.equals("CANDIDATO")) {
                votesBy.computeIfAbsent("p" + e.p + " n" + e.node + " termo " + e.num("termo"), x -> new TreeSet<>())
                        .add(e.node);
            } else if (e.type.equals("VOTO") && e.flags.contains("concedido")) {
                votesBy.computeIfAbsent("p" + e.p + " n" + e.node + " termo " + e.num("termo"), x -> new TreeSet<>())
                        .add(e.nodeRef("candidato"));
            }
        }
        int doubles = 0;
        for (Map.Entry<String, Set<Integer>> v : votesBy.entrySet()) {
            if (v.getValue().size() > 1) {
                doubles++;
                line("  VIOLAÇÃO: " + v.getKey() + " votou em " + nodes(v.getValue()));
            }
        }
        line("Nós que votaram em dois candidatos no mesmo termo: " + doubles + (doubles == 0 ? "  (OK)" : ""));

        // o termo de cada nó nunca diminui; lotes de termo inferior aplicados
        int decreases = 0;
        int staleApplied = 0;
        Map<String, Long> current = new HashMap<>();
        for (Ev e : nodeEvents) {
            String key = "p" + e.p + " n" + e.node;
            long cur = current.getOrDefault(key, -1L);
            long value;
            switch (e.type) {
                case "ARRANQUE":
                case "CANDIDATO":
                case "VOTO":
                case "LIDER":
                case "NOVO_LIDER":
                case "DEMISSAO":
                    value = e.num("termo");
                    break;
                case "TERMO":
                    value = e.flags.isEmpty() ? -1 : Long.parseLong(e.flags.get(0).split("->")[1]);
                    break;
                case "RECEBIDO":
                    if (cur >= 0 && e.num("termo") < cur) {
                        staleApplied++;
                        if (staleApplied <= 10) {
                            line("  LOTE de termo inferior aplicado: " + key + " em " + rel(e.t) + " (termo do lote "
                                    + e.num("termo") + ", termo do nó " + cur + ")");
                        }
                    }
                    continue;
                default:
                    continue;
            }
            if (value < 0) {
                continue;
            }
            if (cur >= 0 && value < cur) {
                decreases++;
                line("  VIOLAÇÃO: termo de " + key + " diminuiu de " + cur + " para " + value + " em " + rel(e.t)
                        + " (" + e.type + (e.type.equals("ARRANQUE") ? ": reinício sem estado persistente?" : "")
                        + ")");
            }
            current.put(key, value);
        }
        line("Diminuições do termo de um nó: " + decreases + (decreases == 0 ? "  (OK: o termo nunca diminui)" : ""));
        line("Lotes de termo inferior aplicados por seguidores: " + staleApplied + (staleApplied == 0 ? "  (OK)" : ""));

        long errs = nodeEvents.stream().filter(e -> e.type.equals("ERRO")).count();
        long twoLeaders = nodeEvents.stream().filter(e -> e.type.equals("ERRO")
                && String.join(";", e.flags).contains("dois lideres")).count();
        long subs = nodeEvents.stream().filter(e -> e.type.equals("ERRO")
                && String.join(";", e.flags).contains("subscricao")).count();
        line("Eventos ERRO: " + errs + " (dois lideres no termo: " + twoLeaders
                + "; subscrição terminada, p.ex. IPFS parado no fim: " + subs
                + "; outros, p.ex. recuperação TCP falhada: " + (errs - twoLeaders - subs) + ")");

        for (Map.Entry<Integer, Map<Integer, long[]>> sp : states.entrySet()) {
            for (Map.Entry<Integer, long[]> s : sp.getValue().entrySet()) {
                String key = "p" + sp.getKey() + " n" + s.getKey();
                long last = current.getOrDefault(key, -1L);
                line("  estado-" + "p" + sp.getKey() + "-n" + s.getKey() + ".txt: termo=" + s.getValue()[0]
                        + " votouEm=" + s.getValue()[1]
                        + (last > s.getValue()[0] ? "  (ATENÇÃO: inferior ao último termo registado, " + last + ")" : ""));
            }
        }
    }

    // 3 · tempo até ao primeiro líder, desde o arranque do primeiro nó
    private void sectionFirstLeader() {
        header("3 · Tempo até ao primeiro líder (desde o arranque do primeiro nó)");
        for (int p : partitions) {
            Ev pending = null;
            int count = 0;
            for (Ev e : nodeEvents) {
                if (e.p != p) continue;
                if (e.type.equals("ARRANQUE") && e.num("termo") == 0 && pending == null) {
                    pending = e;
                } else if (e.type.equals("LIDER") && pending != null) {
                    count++;
                    long terms = e.num("termo") - pending.num("termo");
                    line("  p" + p + ": arranque de n" + pending.node + " em " + rel(pending.t) + " -> LIDER n" + e.node
                            + " termo " + e.num("termo") + " em " + rel(e.t) + ": " + secs(e.t - pending.t)
                            + " s; termos até haver líder: " + terms);
                    pending = null;
                }
            }
            if (pending != null) {
                line("  p" + p + ": arranque de n" + pending.node + " em " + rel(pending.t) + " e nenhum líder até ao fim");
            } else if (count == 0) {
                line("  p" + p + ": sem arranque a frio (termo 0) registado");
            }
        }
    }

    private List<Leadership> leaderships() {
        List<Leadership> result = new ArrayList<>();
        for (Ev e : nodeEvents) {
            if (!e.type.equals("LIDER")) continue;
            Leadership l = new Leadership(e.p, e.node, e.num("termo"), e.t);
            long end = Long.MAX_VALUE;
            String reason = null;
            for (Ev x : nodeEvents) {
                if (x.p == e.p && x.node == e.node && x.t > e.t && x.type.equals("DEMISSAO")) {
                    end = x.t;
                    reason = "DEMISSAO";
                    break;
                }
            }
            for (Failure f : failures) {
                if (f.p == e.p && f.node == e.node && f.t > e.t && f.t < end) {
                    end = f.t;
                    reason = "falha provocada";
                    break;
                }
            }
            for (Ev x : nodeEvents) {
                if (x.p == e.p && x.node == e.node && x.t > e.t && x.type.equals("ARRANQUE")) {
                    if (x.t < end) {
                        end = lastHb(e.p, e.node, l.term, x.t, e.t);
                        reason = "falhou antes do reinício (último lote)";
                    }
                    break;
                }
            }
            if (end == Long.MAX_VALUE) {
                end = lastHb(e.p, e.node, l.term, Long.MAX_VALUE, e.t);
                reason = "ativo até ao fim (último lote)";
            }
            l.end = end;
            l.endReason = reason;
            result.add(l);
        }
        return result;
    }

    private long lastHb(int p, int node, long term, long before, long dflt) {
        long last = dflt;
        for (Hb h : hbs) {
            if (h.p == p && h.node == node && h.term == term && h.t < before) {
                last = Math.max(last, h.t);
            }
        }
        return last;
    }

    // 4 · lideranças e líderes simultâneos
    private void sectionLeaderships(List<Leadership> ls) {
        header("4 · Lideranças e líderes simultâneos");
        for (int p : partitions) {
            List<Leadership> lp = ls.stream().filter(l -> l.p == p).collect(Collectors.toList());
            line("p" + p + ":");
            for (Leadership l : lp) {
                long batches = hbs.stream().filter(h -> h.p == p && h.node == l.node && h.term == l.term).count();
                long recs = hbs.stream().filter(h -> h.p == p && h.node == l.node && h.term == l.term)
                        .mapToLong(h -> h.n).sum();
                line("  n" + l.node + " termo " + l.term + ": " + rel(l.start) + " .. " + rel(l.end) + " ("
                        + l.endReason + "); lotes publicados " + batches + ", registos publicados " + recs);
            }
            int overlaps = 0;
            for (int i = 0; i < lp.size(); i++) {
                for (int j = i + 1; j < lp.size(); j++) {
                    Leadership a = lp.get(i), b = lp.get(j);
                    if (a.node == b.node) continue;
                    long s = Math.max(a.start, b.start), e = Math.min(a.end, b.end);
                    if (s < e) {
                        overlaps++;
                        line("  LÍDERES SIMULTÂNEOS: n" + a.node + " (termo " + a.term + ") e n" + b.node + " (termo "
                                + b.term + ") entre " + rel(s) + " e " + rel(e) + " (" + secs(e - s) + " s)");
                    }
                }
            }
            if (overlaps == 0) {
                line("  sem líderes simultâneos");
            }
        }
    }

    // 5 · indisponibilidade de cada falha provocada
    private void sectionFailures(List<Leadership> ls) {
        header("5 · Indisponibilidade após cada falha (falhas.txt)");
        if (failures.isEmpty()) {
            line("  (sem falhas.txt)");
            return;
        }
        List<Long> unav = new ArrayList<>();
        for (Failure f : failures) {
            Leadership was = null;
            for (Leadership l : ls) {
                if (l.p == f.p && (f.node == 0 || l.node == f.node) && l.start <= f.t && f.t <= l.end + Wire.PERIODO_HB_MS) {
                    if (was == null || l.term > was.term) was = l;
                }
            }
            String who = "p" + f.p + (f.node > 0 ? " n" + f.node : "");
            if (was == null) {
                Leadership cur = null;
                for (Leadership l : ls) {
                    if (l.p == f.p && l.start <= f.t && f.t <= l.end) cur = l;
                }
                line("  falha de " + who + " em " + rel(f.t) + ": não era líder"
                        + (cur != null ? " (líder n" + cur.node + " do termo " + cur.term + " continuou)" : "")
                        + " -> sem indisponibilidade de escrita");
                continue;
            }
            final long term = was.term;
            Hb next = hbs.stream().filter(h -> h.p == f.p && h.term > term && h.t >= f.t)
                    .min(Comparator.comparingLong(h -> h.t)).orElse(null);
            if (next == null) {
                line("  falha de " + who + " em " + rel(f.t) + " (líder do termo " + term
                        + "): nenhum LOTE de termo superior até ao fim do ensaio");
                continue;
            }
            long d = next.t - f.t;
            unav.add(d);
            List<Sent> during = sent.stream().filter(s -> s.p == f.p && s.t >= f.t && s.t <= next.t)
                    .collect(Collectors.toList());
            Map<String, Integer> present = presentReadings(f.p);
            long lost = during.stream().filter(s -> !present.containsKey(s.reading)).count();
            line("  falha de " + who + " em " + rel(f.t) + " (líder do termo " + term + "): primeiro LOTE de termo superior: n"
                    + next.node + " termo " + next.term + " hb=" + next.hb + " em " + rel(next.t)
                    + " -> indisponibilidade " + secs(d) + " s; leituras enviadas nesse intervalo: " + during.size()
                    + " (perdidas: " + lost + ")");
            during.stream().filter(s -> !present.containsKey(s.reading)).limit(10)
                    .forEach(s -> line("      perdida: " + s.reading + " enviada em " + rel(s.t)));
        }
        line("  -> indisponibilidade (s) " + statsSecs(unav) + "; intervalo previsto ]"
                + secs(Wire.ELEICAO_MIN_MS - Wire.PERIODO_HB_MS) + "; "
                + secs(Wire.ELEICAO_MAX_MS + Wire.VERIFICACAO_MS) + "]");
    }

    // leitura -> número máximo de ocorrências no registo final de um nó da partição
    private Map<String, Integer> presentReadings(int p) {
        Map<String, Integer> present = new HashMap<>();
        for (List<SensorRecord> log : logs.getOrDefault(p, Collections.emptyMap()).values()) {
            Map<String, Integer> c = new HashMap<>();
            for (SensorRecord r : log) {
                c.merge(r.reading(), 1, Integer::sum);
            }
            c.forEach((k, v) -> present.merge(k, v, Math::max));
        }
        return present;
    }

    // 6 · leituras perdidas
    private void sectionLost() {
        header("6 · Leituras perdidas (produtor.txt sem correspondência no registo final de nenhum nó)");
        if (sent.isEmpty()) {
            line("  (sem produtor.txt)");
            return;
        }
        int total = 0;
        for (int p : partitions) {
            List<Sent> sp = sent.stream().filter(s -> s.p == p).collect(Collectors.toList());
            Map<String, Integer> avail = new HashMap<>(presentReadings(p));
            List<Sent> lost = new ArrayList<>();
            for (Sent s : sp) {
                int a = avail.getOrDefault(s.reading, 0);
                if (a > 0) {
                    avail.put(s.reading, a - 1);
                } else {
                    lost.add(s);
                }
            }
            total += lost.size();
            line("  p" + p + ": enviadas " + sp.size() + ", perdidas " + lost.size());
            lost.stream().limit(15).forEach(s -> line("      " + s.reading + " enviada em " + rel(s.t)));
            if (lost.size() > 15) line("      ...");
        }
        line("  -> total de leituras perdidas: " + total);
    }

    // 7 · registos truncados
    private void sectionTruncated(Map<Integer, TreeMap<Long, TermInfo>> terms) {
        header("7 · Registos truncados (TRUNCADO)");
        int events = 0;
        long discarded = 0;
        for (Ev e : nodeEvents) {
            if (!e.type.equals("TRUNCADO")) continue;
            events++;
            long de = e.num("de");
            long count = e.num("descartados");
            discarded += count;
            int leader = finalLeader(e.p, terms);
            List<SensorRecord> leaderLog = logs.getOrDefault(e.p, Collections.emptyMap())
                    .getOrDefault(leader, Collections.emptyList());
            String[] readings = e.kv.getOrDefault("leituras", "").isEmpty() ? new String[0]
                    : e.kv.get("leituras").split(",");
            int otherSeq = 0, sameSeq = 0, lost = 0;
            StringBuilder detail = new StringBuilder();
            for (int j = 0; j < readings.length; j++) {
                long origSeq = de + j;
                String r = readings[j];
                List<Long> at = new ArrayList<>();
                for (SensorRecord x : leaderLog) {
                    if (x.reading().equals(r)) at.add(x.getSeq());
                }
                if (at.isEmpty()) {
                    lost++;
                    detail.append("\n      seq ").append(origSeq).append(" ").append(r).append(": perdida");
                } else if (at.contains(origSeq)) {
                    sameSeq++;
                    detail.append("\n      seq ").append(origSeq).append(" ").append(r).append(": mesmo seq, outro termo");
                } else {
                    otherSeq++;
                    detail.append("\n      seq ").append(origSeq).append(" ").append(r).append(": existe com seq ").append(at);
                }
            }
            line("  p" + e.p + " n" + e.node + " em " + rel(e.t) + ": de=" + de + " descartados=" + count + " termos="
                    + e.kv.getOrDefault("termos", "?") + " -> no registo final do líder (n" + leader + "): "
                    + otherSeq + " com outro seq, " + sameSeq + " com o mesmo seq, " + lost + " perdidas"
                    + (readings.length <= 20 ? detail.toString() : ""));
        }
        line("  -> eventos TRUNCADO: " + events + "; registos descartados: " + discarded);
    }

    // nó que foi líder do maior termo da partição
    private int finalLeader(int p, Map<Integer, TreeMap<Long, TermInfo>> terms) {
        TreeMap<Long, TermInfo> tp = terms.getOrDefault(p, new TreeMap<>());
        for (Long t : tp.descendingKeySet()) {
            if (!tp.get(t).leaders.isEmpty()) return tp.get(t).leaders.iterator().next();
        }
        return 0;
    }

    // 8 · coincidência final dos registos
    private void sectionCoincidence() {
        header("8 · Coincidência final dos registos dos nós");
        for (int p : partitions) {
            Map<Integer, List<SensorRecord>> lp = logs.getOrDefault(p, Collections.emptyMap());
            if (lp.isEmpty()) {
                line("  p" + p + ": sem ficheiros no-*");
                continue;
            }
            long min = lp.values().stream().mapToLong(List::size).min().orElse(0);
            StringBuilder sizes = new StringBuilder();
            lp.forEach((node, l) -> sizes.append(" n").append(node).append("=").append(l.size())
                    .append(l.isEmpty() ? "" : "(t" + l.get(l.size() - 1).getTerm() + ")"));
            List<Long> diff = new ArrayList<>();
            for (int s = 1; s <= min; s++) {
                String ref = null;
                boolean same = true;
                for (List<SensorRecord> l : lp.values()) {
                    String x = l.get(s - 1).toLine();
                    if (ref == null) ref = x;
                    else if (!ref.equals(x)) same = false;
                }
                if (!same) diff.add((long) s);
            }
            line("  p" + p + ": ultimoSeq" + sizes + "; seq divergentes até " + min + ": " + diff.size()
                    + (diff.isEmpty() ? "  (OK)" : " " + diff.subList(0, Math.min(20, diff.size()))));
        }
    }

    // 9 · o que observaram o produtor e o processador
    private void sectionMonitors() {
        header("9 · Observado pelo produtor e pelo processador (monitor-*.txt)");
        if (monitorEvents.isEmpty()) {
            line("  (sem ficheiros monitor-*)");
            return;
        }
        String[] types = { "NOVO_LIDER", "SUSPEITA", "RETOMA", "HB_PERDIDO", "IGNORADO", "AVISO" };
        for (Map.Entry<String, List<Ev>> m : monitorEvents.entrySet()) {
            line("monitor-" + m.getKey() + ":");
            for (int p : partitions) {
                StringBuilder sb = new StringBuilder("  p" + p + ":");
                for (String t : types) {
                    long c = m.getValue().stream().filter(e -> e.p == p && e.type.equals(t)).count();
                    sb.append(" ").append(t).append("=").append(c);
                }
                line(sb.toString());
            }
            m.getValue().stream().filter(e -> e.type.equals("NOVO_LIDER") || e.type.equals("SUSPEITA")
                    || e.type.equals("RETOMA")).limit(40).forEach(e -> line("    " + rel(e.t) + " " + e.type + " p" + e.p
                    + " " + e.kv.entrySet().stream().map(x -> x.getKey() + "=" + x.getValue())
                    .collect(Collectors.joining(" "))));
        }
    }

    // =====================================================================================
    // Formatação
    // =====================================================================================

    private void header(String s) {
        line("");
        line("=== " + s + " ===");
    }

    private void line(String s) {
        System.out.println(s);
        out.append(s).append(System.lineSeparator());
    }

    private String rel(long t) {
        return "t=" + secs(t - t0) + "s";
    }

    private static String secs(long ms) {
        return String.format(Locale.ROOT, "%.1f", ms / 1000.0);
    }

    private static String nodes(Set<Integer> s) {
        return s.stream().map(i -> "n" + i).collect(Collectors.joining(",", "{", "}"));
    }

    private static String stats(List<Long> v) {
        if (v.isEmpty()) return "-";
        long min = Collections.min(v), max = Collections.max(v);
        double avg = v.stream().mapToLong(Long::longValue).average().orElse(0);
        return String.format(Locale.ROOT, "mín %d / méd %.0f / máx %d", min, avg, max);
    }

    private static String statsSecs(List<Long> v) {
        if (v.isEmpty()) return "-";
        long min = Collections.min(v), max = Collections.max(v);
        double avg = v.stream().mapToLong(Long::longValue).average().orElse(0);
        return String.format(Locale.ROOT, "mín %.1f / méd %.1f / máx %.1f", min / 1000.0, avg / 1000.0, max / 1000.0);
    }
}
