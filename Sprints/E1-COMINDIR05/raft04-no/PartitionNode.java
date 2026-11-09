package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

// PartitionNode <k> <i> [N] [api]
//
// Um dos GRUPO nós da partição de dados k. Junta o PartitionLeader e a Replica da ficha anterior:
// conforme o papel que o RaftState lhe dá, é seguidor (aplica os LOTE do líder), candidato (pede votos)
// ou líder (atribui seq às leituras IN e publica os LOTE a cada Δ).
//
// Locks: o estado do registo e o estado de líder são protegidos pelo monitor do nó (this). A ordem de
// aquisição é sempre  nó -> RaftState  e  nó -> RecordFile  (e, no fim, o lock dos eventos). RaftState
// nunca chama o nó e o servidor TCP só usa RecordFile, por isso não há ciclos (nem deadlocks).
public class PartitionNode {
    private final int k;                 // partição de dados
    private final int self;              // identificador do nó no grupo (1..GRUPO)
    private final int n;                 // número de partições = dimensão dos relógios vetoriais
    private final String endereco = "127.0.0.1";
    private final int porto;
    private final IpfsPubSub ps;
    private final Partitioner partitioner;
    private final RecordFile file;
    private final Path hbFile;
    private final Path eventFile;
    private final Object eventLock = new Object();
    private final RaftState raft;

    // ---------- estado protegido pelo monitor do nó ----------
    // cópia em memória de no-p<k>-n<i>.txt: log.get(s - 1) é o registo com seq s
    private final List<SensorRecord> log = new ArrayList<>();

    // estado de líder, válido enquanto raft.termIfLeader() == termoLider
    private long termoLider = -1;
    private long seq;
    private long lamport;
    private VectorClock vectorClock;
    private List<SensorRecord> pendentes = new ArrayList<>();
    private long hb;
    private ScheduledFuture<?> agendamento;

    // último LOTE aplicado como seguidor (um lote atrasado do mesmo termo não é usado para comparar)
    private long termoUltimoLote = -1;
    private long hbUltimoLote = 0;
    // ----------------------------------------------------------

    private final ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService verificacao = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean isolado = false;   // só para assinalar o início e o fim do isolamento

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: java raft04.PartitionNode <k> <i> [N] [api]");
            System.exit(1);
        }
        int k = Integer.parseInt(args[0]);
        int i = Integer.parseInt(args[1]);
        int n = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";
        if (n < 1 || k < 0 || k >= n) {
            System.err.println("Erro: é preciso 0 <= k < N (k=" + k + ", N=" + n + ")");
            System.exit(1);
        }
        if (i < 1 || i > Wire.GRUPO) {
            System.err.println("Erro: o identificador do nó deve estar entre 1 e " + Wire.GRUPO);
            System.exit(1);
        }
        new PartitionNode(k, i, n, api).iniciar();
    }

    public PartitionNode(int k, int self, int n, String api) throws IOException {
        this.k = k;
        this.self = self;
        this.n = n;
        this.porto = Wire.nodePort(k, self);
        this.ps = new IpfsPubSub(api);
        this.partitioner = new Partitioner(n);
        this.file = new RecordFile("no-p" + k + "-n" + self + ".txt", n);
        this.hbFile = Paths.get("hb-p" + k + "-n" + self + ".txt");
        this.eventFile = Paths.get("eventos-p" + k + "-n" + self + ".txt");

        // registos locais: determinam ultimoSeq e ultimoTermo
        List<SensorRecord> lidos = file.readAll();
        for (SensorRecord r : lidos) {
            if (r.getSeq() != log.size() + 1) {
                throw new IllegalStateException("Ficheiro " + "no-p" + k + "-n" + self
                        + ".txt com seq fora de ordem: " + r.getSeq() + " depois de " + log.size());
            }
            log.add(r);
        }

        // estado persistente (termo e voto), como seguidor
        Consumer<String> registarEvento = this::evento;
        this.raft = new RaftState(k, self, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS,
                Paths.get("estado-p" + k + "-n" + self + ".txt"), registarEvento);
    }

    private void iniciar() throws IOException {
        synchronized (this) {
            evento("ARRANQUE;p" + k + ";termo=" + raft.term() + ";votouEm=" + raft.votedFor()
                    + ";ultimoSeq=" + ultimoSeq() + ";ultimoTermo=" + ultimoTermo() + ";porto=" + porto
                    + ";eleicao=" + Wire.ELEICAO_MIN_MS + ".." + Wire.ELEICAO_MAX_MS
                    + (Wire.FALHA != 0 ? ";FALHA=" + Wire.FALHA : ""));
        }
        System.out.println("Nó n" + self + " de p" + k + " (N=" + n + ", G=" + Wire.GRUPO + ", Δ="
                + Wire.PERIODO_HB_MS + " ms, eleição em [" + Wire.ELEICAO_MIN_MS + ", " + Wire.ELEICAO_MAX_MS
                + "] ms" + (Wire.FALHA != 0 ? ", FALHA " + Wire.FALHA : "") + ")");

        // 1. servidor TCP de recuperação, aberto antes de qualquer subscrição; serve qualquer nó
        ServerSocket server = new ServerSocket(porto);
        iniciarThread("recuperacao-p" + k + "-n" + self, () -> servirRecuperacao(server));

        // 2. subscrições: leituras, lotes e eleição
        subscrever(Wire.topicIn(k), this::onIn);
        subscrever(Wire.topicData(k), this::onLote);
        subscrever(Wire.topicElection(k), this::onEleicao);

        // 3. thread de verificação: prazo de eleição
        verificacao.scheduleAtFixedRate(this::verificar, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS,
                TimeUnit.MILLISECONDS);
    }

    private void subscrever(String topico, Consumer<String> handler) {
        iniciarThread("sub-" + topico + "-n" + self, () -> {
            try {
                ps.subscribe(topico, handler);
            } catch (Exception e) {
                evento("ERRO;p" + k + ";subscricao de " + topico + " terminou: " + e.getMessage());
            }
        });
    }

    private static void iniciarThread(String nome, Runnable r) {
        Thread t = new Thread(r, nome);
        t.start();
    }

    // =====================================================================================
    // Eleição
    // =====================================================================================

    // a cada VERIFICACAO_MS: se o prazo de eleição expirou, o nó candidata-se
    private void verificar() {
        try {
            assinalarIsolamento();
            VoteRequest pedido = null;
            synchronized (this) {
                long t = raft.tick();      // grava termo e voto antes de devolver
                if (t != -1) {
                    pedido = new VoteRequest(t, self, ultimoSeq(), ultimoTermo());
                }
            }
            if (pedido != null) {
                publicar(Wire.topicElection(k), pedido.encode());
            }
        } catch (Exception e) {
            // uma exceção não apanhada cancelaria o agendamento
            evento("ERRO;p" + k + ";verificacao: " + e);
        }
    }

    private void onEleicao(String msg) {
        if (Wire.isolated(k, self)) {
            return;                        // partição de rede simulada
        }
        try {
            String tipo = Wire.type(msg);
            if (tipo.equals("PEDIDO_VOTO")) {
                VoteRequest p = VoteRequest.decode(msg);
                if (p.candidate == self) {
                    return;                // o próprio pedido
                }
                RaftState.Vote v;
                synchronized (this) {
                    // pode demitir o nó (termo superior); o estado é gravado antes de publicar o VOTO
                    v = raft.onVoteRequest(p.term, p.candidate, p.lastSeq, p.lastTerm, ultimoSeq(), ultimoTermo());
                }
                // responde sempre, mesmo a recusar: um candidato obsoleto fica a conhecer o termo atual
                publicar(Wire.topicElection(k), new VoteReply(v.term, self, p.candidate, v.granted).encode());

            } else if (tipo.equals("VOTO")) {
                VoteReply v = VoteReply.decode(msg);
                if (v.voter == self || v.candidate != self) {
                    return;                // a própria resposta, ou dirigida a outro candidato
                }
                synchronized (this) {
                    if (raft.onVote(v.term, v.voter, v.candidate, v.granted)) {
                        assumirLideranca(raft.termIfLeader());
                    }
                }
            } else {
                throw new IllegalArgumentException("tipo desconhecido no tópico de eleição");
            }
        } catch (Exception e) {
            System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
        }
    }

    // =====================================================================================
    // Líder
    // =====================================================================================

    // chamado com o monitor do nó, no instante em que onVote devolve true
    private void assumirLideranca(long t) {
        if (agendamento != null) {
            agendamento.cancel(false);     // um agendamento de uma liderança anterior
        }
        termoLider = t;
        seq = ultimoSeq();
        SensorRecord ultimo = ultimoRegisto();
        if (Wire.FALHA == 5 || ultimo == null) {
            lamport = 0;                   // falha 5: L e V a zero em vez de a partir do último registo
            vectorClock = new VectorClock(n);
        } else {
            lamport = ultimo.getLamport();
            vectorClock = ultimo.getVector();
        }
        hb = 0;
        pendentes = new ArrayList<>();
        System.out.println("LIDER do termo " + t + ": seq=" + seq + " L=" + lamport + " V=" + vectorClock);
        final long termo = t;
        // um LOTE de imediato, que afirma a liderança, e depois a cada Δ
        agendamento = heartbeat.scheduleAtFixedRate(() -> enviarLote(termo), 0, Wire.PERIODO_HB_MS,
                TimeUnit.MILLISECONDS);
    }

    // leitura IN: só o líder lhe atribui seq
    private void onIn(String msg) {
        try {
            if (!Wire.type(msg).equals("IN")) {
                throw new IllegalArgumentException("A mensagem não é do tipo IN");
            }
            String[] partes = Wire.body(msg).split(";");
            if (partes.length != 4) {
                throw new IllegalArgumentException("Corpo inválido");
            }
            String sensor = partes[0];
            double temp = Double.parseDouble(partes[1]);
            long incomingLamport = Long.parseLong(partes[2]);
            VectorClock incomingVector = VectorClock.parse(partes[3], n);

            int j = partitioner.of(sensor);
            if (j != k) {
                System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                return;
            }

            synchronized (this) {
                // mesma secção que a demissão: ou a leitura é numerada antes de o nó adotar o termo novo,
                // ou já vê termIfLeader() == -1
                long t = raft.termIfLeader();
                if (t == -1 || t != termoLider) {
                    evento("IGNORADO;p" + k + ";IN;sensor=" + sensor + ";termo=" + raft.term()
                            + ";papel=" + raft.role());
                    return;
                }
                long novoSeq = seq + 1;
                long novoL = Math.max(lamport, incomingLamport) + 1;
                VectorClock novoV = vectorClock.copy();
                novoV.merge(incomingVector);
                novoV.tick(k);
                if (Wire.FALHA != 5 && novoV.get(k) != novoSeq) {
                    throw new IllegalStateException("Relógio vetorial inconsistente: V[" + k + "]="
                            + novoV.get(k) + ", seq=" + novoSeq);
                }
                SensorRecord r = SensorRecord.now(novoSeq, t, sensor, temp, novoL, novoV);
                file.append(r);            // só depois de gravado é que o estado avança
                seq = novoSeq;
                lamport = novoL;
                vectorClock = novoV;
                log.add(r);
                pendentes.add(r);
                System.out.println("EVENTO: seq=" + r.getSeq() + " termo=" + t + " L=" + r.getLamport()
                        + " V=" + r.getVector());
            }
        } catch (Exception e) {
            System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
        }
    }

    // fecha o lote do intervalo e publica-o; serve também de heartbeat
    private void enviarLote(long termo) {
        try {
            List<SensorRecord> lote;
            long hbAtual;
            long seqAtual;
            long termoUltimo;
            synchronized (this) {
                if (raft.termIfLeader() != termo) {
                    // demitido: este agendamento termina
                    if (termoLider == termo && agendamento != null) {
                        agendamento.cancel(false);
                        agendamento = null;
                        termoLider = -1;
                        System.out.println("HEARTBEAT terminado: já não é líder do termo " + termo);
                    }
                    return;
                }
                lote = pendentes;
                pendentes = new ArrayList<>();
                seqAtual = seq;
                termoUltimo = ultimoTermo();
                hb++;
                hbAtual = hb;
            }

            // fora da secção sincronizada: a publicação não bloqueia a entrada nem a eleição
            if (Wire.isolated(k, self)) {
                System.out.println("ISOLADO: lote hb=" + hbAtual + " (" + lote.size() + " registos) não publicado");
                return;
            }
            String msg = new Batch(termo, self, endereco, porto, hbAtual, seqAtual, termoUltimo, lote).encode();
            int bytes = msg.getBytes(StandardCharsets.UTF_8).length;
            long instante = System.currentTimeMillis();
            ps.publish(Wire.topicData(k), msg);
            Files.writeString(hbFile,
                    termo + ";" + hbAtual + ";" + instante + ";" + seqAtual + ";" + termoUltimo + ";"
                            + lote.size() + ";" + bytes + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            System.out.println("HEARTBEAT: termo=" + termo + " hb=" + hbAtual + " n=" + lote.size()
                    + " ultimoSeq=" + seqAtual + " ultimoTermo=" + termoUltimo);
        } catch (Exception e) {
            // não se republica: os seguidores recuperam pelo ultimoSeq do lote seguinte
            System.out.println("ERRO na publicação do lote do termo " + termo + ": " + e.getMessage());
        }
    }

    // =====================================================================================
    // Seguidor
    // =====================================================================================

    private void onLote(String msg) {
        if (Wire.isolated(k, self)) {
            return;                        // partição de rede simulada
        }
        try {
            if (!Wire.type(msg).equals("LOTE")) {
                throw new IllegalArgumentException("Mensagem não é LOTE");
            }
            Batch b = Batch.decode(msg, n);
            if (b.getLeader() == self) {
                return;                    // o próprio lote
            }
            synchronized (this) {
                // termo superior: adota-o (e demite-se se era líder); inferior: líder obsoleto
                if (!raft.onBatch(b.getTerm(), b.getLeader())) {
                    evento("IGNORADO;p" + k + ";LOTE;termo=" + b.getTerm() + ";lider=n" + b.getLeader()
                            + ";hb=" + b.getHb() + ";meuTermo=" + raft.term());
                    return;
                }
                evento("RECEBIDO;p" + k + ";termo=" + b.getTerm() + ";lider=n" + b.getLeader() + ";hb=" + b.getHb()
                        + ";ultimoSeq=" + b.getLastSeq() + ";ultimoTermo=" + b.getLastTerm() + ";n=" + b.size()
                        + ";seq=" + b.seqRange());
                aplicarLote(b);
            }
        } catch (Exception e) {
            evento("ERRO;p" + k + ";lote rejeitado: " + e.getMessage());
        }
    }

    // Aplica os registos de um LOTE pela ordem; chamado com o monitor do nó
    private void aplicarLote(Batch b) throws IOException {
        boolean atrasado = b.getTerm() == termoUltimoLote && b.getHb() <= hbUltimoLote;
        if (!atrasado) {
            termoUltimoLote = b.getTerm();
            hbUltimoLote = b.getHb();
        }

        for (SensorRecord r : b.getRecords()) {
            long m = ultimoSeq();
            if (r.getSeq() <= m) {
                long meu = termoDe(r.getSeq());
                if (meu == r.getTerm()) {
                    continue;              // duplicado: mesmo seq e mesmo termo
                }
                resolverDivergencia(b, "seq " + r.getSeq() + " tem termo " + meu + " e o do lider tem " + r.getTerm());
                return;
            }
            if (r.getSeq() == m + 1 && (m == 0 || termoDe(m) == r.getTerm())) {
                acrescentar(r);            // a seguir ao último, do mesmo líder
                continue;
            }
            // lacuna (seq > m + 1), ou primeiro registo de outro termo: o registo m pode não ser o do líder
            recuperar(b, r.getSeq() > m + 1 ? "lacuna" : "fronteira de termo");
            return;
        }

        if (atrasado) {
            return;                        // ultimoSeq de um lote atrasado já está ultrapassado
        }
        long m = ultimoSeq();
        if (m < b.getLastSeq()) {
            recuperar(b, "atraso");        // faltam registos que o líder já numerou
        } else if (m > b.getLastSeq()) {
            resolverDivergencia(b, "registos para alem de ultimoSeq=" + b.getLastSeq() + " (ultimo local " + m + ")");
        } else if (termoDe(m) != b.getLastTerm()) {
            resolverDivergencia(b, "registo " + m + " tem termo " + termoDe(m) + " e ultimoTermo=" + b.getLastTerm());
        }
    }

    // Recupera por TCP, ao líder, os registos ultimoSeq+1 .. b.ultimoSeq. Pede também o último registo local
    // para confirmar que coincide com o do líder; se não coincidir, há divergência.
    private void recuperar(Batch b, String motivo) throws IOException {
        long m = ultimoSeq();
        long ate = b.getLastSeq();
        if (ate <= m) {
            return;
        }
        evento("RECUPERACAO;p" + k + ";de=" + (m + 1) + ";ate=" + ate + ";motivo=" + motivo);
        List<SensorRecord> recebidos;
        try {
            recebidos = pedir(b, m == 0 ? 1 : m, ate);
        } catch (IOException e) {
            evento("ERRO;p" + k + ";recuperacao " + (m + 1) + ".." + ate + " falhou: " + e.getMessage());
            return;
        }
        if (m > 0 && (recebidos.isEmpty() || !recebidos.get(0).sameEntry(log.get((int) m - 1)))) {
            resolverDivergencia(b, "registo " + m + " nao coincide com o do lider");
            return;
        }
        acrescentarSeguintes(recebidos);
    }

    // Divergência: pede ao líder os registos a partir do primeiro seq do último bloco de termo, recua um
    // bloco de cada vez enquanto o primeiro registo recebido não coincidir com o local, trunca o ficheiro
    // (de forma atómica) a partir do primeiro seq diferente e aplica os do líder.
    private void resolverDivergencia(Batch b, String motivo) throws IOException {
        long m = ultimoSeq();
        if (m == 0) {
            recuperar(b, "divergencia sem registos locais");
            return;
        }
        long ate = Math.max(b.getLastSeq(), m);
        System.out.println("DIVERGENCIA: " + motivo);

        long inicio = inicioBloco(m);
        List<SensorRecord> recebidos;
        while (true) {
            try {
                recebidos = pedir(b, inicio, ate);
            } catch (IOException e) {
                evento("ERRO;p" + k + ";divergencia (" + motivo + "): pedido " + inicio + ".." + ate
                        + " falhou: " + e.getMessage());
                return;
            }
            boolean coincide = !recebidos.isEmpty() && recebidos.get(0).sameEntry(log.get((int) inicio - 1));
            if (coincide || inicio == 1) {
                break;
            }
            inicio = inicioBloco(inicio - 1);   // recua um bloco de termo
        }

        // primeiro seq em que o registo local difere do do líder
        long d = inicio;
        for (SensorRecord r : recebidos) {
            if (r.getSeq() > m || !r.sameEntry(log.get((int) r.getSeq() - 1))) {
                break;
            }
            d = r.getSeq() + 1;
        }

        if (d <= m) {
            List<SensorRecord> descartados = new ArrayList<>(log.subList((int) d - 1, (int) m));
            List<SensorRecord> mantidos = new ArrayList<>(log.subList(0, (int) d - 1));
            file.rewrite(mantidos);        // atómico: o ficheiro nunca fica a meio
            log.subList((int) d - 1, (int) m).clear();
            Set<Long> termos = new LinkedHashSet<>();
            for (SensorRecord r : descartados) {
                termos.add(r.getTerm());
            }
            evento("TRUNCADO;p" + k + ";de=" + d + ";descartados=" + descartados.size() + ";termos="
                    + termos.stream().map(String::valueOf).collect(Collectors.joining(","))
                    + ";leituras=" + descartados.stream().map(SensorRecord::reading).collect(Collectors.joining(",")));
        } else {
            System.out.println("DIVERGENCIA sem registos a descartar (o registo local é prefixo do do líder)");
        }
        acrescentarSeguintes(recebidos);
    }

    // acrescenta os registos recebidos que se seguem ao último local
    private void acrescentarSeguintes(List<SensorRecord> recebidos) throws IOException {
        for (SensorRecord r : recebidos) {
            if (r.getSeq() <= ultimoSeq()) {
                continue;
            }
            if (r.getSeq() != ultimoSeq() + 1) {
                evento("ERRO;p" + k + ";recuperacao com lacuna: recebido " + r.getSeq() + ", esperado "
                        + (ultimoSeq() + 1));
                return;
            }
            acrescentar(r);
            System.out.println("RECUPERADO: seq=" + r.getSeq() + " termo=" + r.getTerm());
        }
    }

    private void acrescentar(SensorRecord r) throws IOException {
        file.append(r);
        log.add(r);
    }

    // pedido de recuperação "PEDIDO <de> <até>" ao nó que publicou o lote; termina com FIM
    private List<SensorRecord> pedir(Batch b, long de, long ate) throws IOException {
        if (Wire.isolated(k, self)) {
            throw new IOException("no isolado (particao de rede simulada)");
        }
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(b.getAddress(), b.getPort()), Wire.T_TCP_MS);
            s.setSoTimeout(Wire.T_TCP_MS);
            BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8),
                    true);
            out.println("PEDIDO " + de + " " + ate);
            List<SensorRecord> recebidos = new ArrayList<>();
            String linha;
            while ((linha = in.readLine()) != null) {
                if (linha.equals("FIM")) {
                    System.out.println("PEDIDO " + de + ".." + ate + " a n" + b.getLeader() + ": "
                            + recebidos.size() + " registos");
                    return recebidos;
                }
                if (linha.startsWith("ERRO")) {
                    throw new IOException(linha);
                }
                try {
                    recebidos.add(SensorRecord.fromLine(linha, n));
                } catch (IllegalArgumentException e) {
                    throw new IOException("registo mal formado: " + linha);
                }
            }
            throw new IOException("resposta interrompida antes de FIM");
        }
    }

    // =====================================================================================
    // Servidor TCP de recuperação (serve o ficheiro do nó a qualquer nó, seja ou não líder)
    // =====================================================================================

    private void servirRecuperacao(ServerSocket server) {
        System.out.println("À escuta para recuperação no porto " + porto + "...");
        while (true) {
            try (Socket s = server.accept();
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                    PrintWriter out = new PrintWriter(
                            new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

                if (Wire.isolated(k, self)) {
                    continue;              // partição de rede simulada: fecha sem responder
                }
                s.setSoTimeout(Wire.T_TCP_MS);
                String pedido = in.readLine();
                if (pedido == null || !pedido.startsWith("PEDIDO ")) {
                    out.println("ERRO pedido mal formado");
                    continue;
                }
                String[] partes = pedido.split(" ");
                if (partes.length != 3) {
                    out.println("ERRO formato deve ser PEDIDO <de> <até>");
                    continue;
                }
                long de;
                long ate;
                try {
                    de = Long.parseLong(partes[1]);
                    ate = Long.parseLong(partes[2]);
                } catch (NumberFormatException e) {
                    out.println("ERRO valores numericos invalidos");
                    continue;
                }
                if (de > ate) {
                    out.println("ERRO <de> maior que <até>");
                    continue;
                }
                // só o monitor de RecordFile: o servidor nunca adquire o monitor do nó
                List<SensorRecord> registos = file.range(de, ate);
                for (SensorRecord r : registos) {
                    out.println(r.toLine());
                }
                out.println("FIM");
                System.out.println("RECUPERAÇÃO servida: " + de + ".." + ate + " para "
                        + s.getRemoteSocketAddress() + " (" + registos.size() + " registos)");
            } catch (IOException e) {
                System.out.println("Recuperação interrompida: " + e.getMessage());
            }
        }
    }

    // =====================================================================================
    // Auxiliares
    // =====================================================================================

    private void publicar(String topico, String msg) throws IOException, InterruptedException {
        if (Wire.isolated(k, self)) {
            return;                        // partição de rede simulada: a mensagem não sai do nó
        }
        ps.publish(topico, msg);
    }

    private void assinalarIsolamento() {
        boolean agora = Wire.isolated(k, self);
        if (agora != isolado) {
            isolado = agora;
            evento("ISOLAMENTO;p" + k + ";" + (agora ? "inicio" : "fim"));
        }
    }

    // com o monitor do nó
    private long ultimoSeq() {
        return log.size();
    }

    private long ultimoTermo() {
        return log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
    }

    private long termoDe(long s) {
        return s == 0 ? 0 : log.get((int) s - 1).getTerm();
    }

    private SensorRecord ultimoRegisto() {
        return log.isEmpty() ? null : log.get(log.size() - 1);
    }

    // primeiro seq do bloco de termo (sequência de registos consecutivos com o mesmo termo) que contém s
    private long inicioBloco(long s) {
        long t = termoDe(s);
        while (s > 1 && termoDe(s - 1) == t) {
            s--;
        }
        return s;
    }

    // instante;evento;p<k>;detalhe, na consola e em eventos-p<k>-n<i>.txt
    private void evento(String linha) {
        String l = System.currentTimeMillis() + ";" + linha;
        synchronized (eventLock) {
            System.out.println(l);
            try {
                Files.writeString(eventFile, l + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.out.println("Erro ao escrever " + eventFile + ": " + e.getMessage());
            }
        }
    }
}
