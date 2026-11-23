package tpf;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.entities.Message;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.InteractionHook;
import net.dv8tion.jda.api.interactions.commands.OptionMapping;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.CommandData;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;
import net.dv8tion.jda.api.utils.messages.MessageRequest;

/**
 * Líder do arquivo. Desde o sprint E2-02 implementa o caminho de escrita local: cada /registar, cada
 * mensagem em #arquivo e cada registo de /rajada recebe um LSN, é gravado em registos.log com fsync e
 * entra no chunk aberto, que é selado por tamanho ou por tempo, cifrado e gravado em selados/.
 * /estado, /config, /recuperar, /replica-token e /replica-remover mantêm a resposta provisória.
 */
public class LeaderBot extends ListenerAdapter {

    private static final String FICHEIRO_CONFIG = "bot.properties";
    private static final String CANAL_ARQUIVO = "arquivo";
    private static final String PAPEL_ADMIN = "admin-arquivo";
    private static final Set<String> COMANDOS_ADMIN =
            Set.of("config r", "replica-token", "replica-remover", "rajada");

    /**
     * true: regista os 7 comandos no servidor em cada arranque. false: não os regista, e o Discord
     * continua a mostrar os que foram registados num arranque anterior.
     */
    private static final boolean REGISTAR_COMANDOS = false;

    /** Mantém as respostas abaixo do limite de 2000 caracteres do Discord. */
    private static final int MAX_ECO = 1500;
    private static final String FORMATO_ID = "[\\w-]{1,64}";

    // Ficheiros do líder, na pasta de trabalho
    static final String MASTER_KEY = "master.key";
    static final String REGISTOS_LOG = "registos.log";
    static final String EVENTOS = "eventos-lider.txt";
    static final String SELADOS = "selados";
    /** Ficheiro temporário das gravações atómicas; fica fora de selados/, que só tem chunks completos. */
    private static final String TEMPORARIO = ".selagem.tmp";
    /** Não há rotação da master key: a versão da chave é sempre 1. */
    private static final int KEY_VERSION = 1;
    /** Com textos deste tamanho, /rajada 500 0 enche vários chunks de CHUNK_MAX_BYTES. */
    private static final int TEXTO_SINTETICO_CHARS = 500;

    private final long guildId;
    private final ChunkCrypto crypto;
    private final Path pastaSelados;
    private final Path temporario;
    private final FileChannel log;
    private final FileChannel eventos;
    private final ScheduledExecutorService temporizador =
            Executors.newSingleThreadScheduledExecutor(daemon("selagem"));
    private final ExecutorService rajadas = Executors.newSingleThreadExecutor(daemon("rajada"));

    // Estado do caminho de escrita. Só é lido e alterado em métodos synchronized: a thread do JDA, a
    // do temporizador e a da rajada nunca o veem a meio de uma alteração.
    private long proximoLsn;
    private List<Registo> chunkAberto = new ArrayList<>();
    private long bytesChunkAberto = Chunk.CABECALHO_BYTES;
    private ScheduledFuture<?> selagemAgendada;
    private boolean logIncerto;

    LeaderBot(long guildId, Path pasta, byte[] masterKey) throws IOException {
        this.guildId = guildId;
        this.crypto = new ChunkCrypto(masterKey, KEY_VERSION);
        this.pastaSelados = pasta.resolve(SELADOS);
        this.temporario = pasta.resolve(TEMPORARIO);
        Files.createDirectories(pastaSelados);
        Files.deleteIfExists(temporario); // resto de uma selagem interrompida a meio
        Path ficheiroLog = pasta.resolve(REGISTOS_LOG);
        // lerRegistos garante LSN 1, 2, 3, ..., por isso o próximo é o número de registos mais um
        this.proximoLsn = lerRegistos(ficheiroLog, true).size() + 1L;
        this.log = FileChannel.open(ficheiroLog,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        this.eventos = FileChannel.open(pasta.resolve(EVENTOS),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        mostrarParametros();
        Path pasta = Path.of("");
        if (args.length > 0 && args[0].equals("abrir")) {
            System.exit(abrir(pasta, List.of(args).subList(1, args.length)));
        }

        Properties ficheiro = lerFicheiroConfig();
        String token = config(ficheiro, "DISCORD_TOKEN");
        long guildId = guildIdDe(config(ficheiro, "DISCORD_GUILD_ID"));

        LeaderBot bot;
        try {
            bot = new LeaderBot(guildId, pasta, masterKey(pasta.resolve(MASTER_KEY), true));
        } catch (IOException | IllegalStateException e) {
            terminar("Arranque recusado: " + e.getMessage());
            return;
        }
        System.out.println(REGISTOS_LOG + ": o próximo LSN é " + bot.proximoLsn);

        // O texto dos utilizadores é ecoado nas respostas e não deve notificar ninguém.
        MessageRequest.setDefaultMentions(EnumSet.noneOf(Message.MentionType.class));

        JDA jda = JDABuilder
                .createLight(token, EnumSet.of(GatewayIntent.GUILD_MESSAGES, GatewayIntent.MESSAGE_CONTENT))
                .addEventListeners(bot)
                .build();
        jda.awaitReady();

        Guild guild = jda.getGuildById(guildId);
        if (guild == null) {
            System.err.println("DISCORD_GUILD_ID=" + guildId + " não corresponde a nenhum servidor onde o bot esteja.");
            System.err.println("Servidores onde o bot está (" + jda.getGuilds().size() + "):");
            jda.getGuilds().forEach(g -> System.err.println("  " + g.getName() + ": " + g.getId()));
            jda.shutdownNow();
            System.exit(1);
        }

        if (!REGISTAR_COMANDOS) {
            System.out.println("Registo dos comandos ignorado (REGISTAR_COMANDOS = false)");
            return;
        }
        guild.updateCommands().addCommands(definirComandos()).queue(
                comandos -> System.out.println(comandos.size() + " comandos registados em " + guild.getName()),
                erro -> System.err.println("Falha ao registar os comandos: " + erro.getMessage()));
    }

    /** Lê os parâmetros -Dtpf.* logo no arranque, para um valor inválido o recusar com uma mensagem clara. */
    private static void mostrarParametros() {
        try {
            System.out.println("Parâmetros: CHUNK_MAX_BYTES=" + Wire.CHUNK_MAX_BYTES + ", PAD_BYTES=" + Wire.PAD_BYTES
                    + ", SELAGEM_MS=" + Wire.SELAGEM_MS);
        } catch (ExceptionInInitializerError e) {
            terminar("Parâmetro inválido: " + e.getCause().getMessage());
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Master key
    // ---------------------------------------------------------------------------------------------

    /**
     * Lê a master.key, que tem de ter exatamente 32 bytes. Se não existir e gerar for true (primeiro
     * arranque do líder), gera-a com SecureRandom e deixa-a só com permissão de leitura para o dono.
     * Uma master.key com outro tamanho lança IllegalStateException, o que recusa o arranque.
     */
    static byte[] masterKey(Path ficheiro, boolean gerar) throws IOException {
        if (Files.notExists(ficheiro)) {
            if (!gerar) {
                throw new IllegalStateException(ficheiro.toAbsolutePath() + " não existe");
            }
            byte[] chave = new byte[32];
            new SecureRandom().nextBytes(chave);
            gravarMasterKey(ficheiro, chave);
            System.out.println("master.key gerada em " + ficheiro.toAbsolutePath());
            return chave;
        }
        if (!Files.isRegularFile(ficheiro)) {
            throw new IllegalStateException(ficheiro.toAbsolutePath() + " não é um ficheiro");
        }
        byte[] chave = Files.readAllBytes(ficheiro);
        if (chave.length != 32) {
            throw new IllegalStateException(ficheiro.toAbsolutePath() + " tem " + chave.length
                    + " bytes, mas a master key tem de ter exatamente 32");
        }
        return chave;
    }

    /** CREATE_NEW nunca substitui uma chave que já exista. Em POSIX, o ficheiro já nasce sem acesso para os outros. */
    private static void gravarMasterKey(Path ficheiro, byte[] chave) throws IOException {
        Set<StandardOpenOption> opcoes = EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        boolean posix = ficheiro.getFileSystem().supportedFileAttributeViews().contains("posix");
        try (FileChannel c = posix
                ? FileChannel.open(ficheiro, opcoes, PosixFilePermissions.asFileAttribute(
                        EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE)))
                : FileChannel.open(ficheiro, opcoes)) {
            escreverTudo(c, chave);
            c.force(true);
        }
        try {
            soLeituraParaODono(ficheiro);
        } catch (IOException | RuntimeException e) {
            try {
                Files.deleteIfExists(ficheiro); // uma chave nova sem as permissões pedidas não fica para depois
            } catch (IOException ignorada) {
                // a mensagem de erro já indica o problema
            }
            throw new IOException("não foi possível restringir as permissões de " + ficheiro + ": " + e.getMessage(), e);
        }
    }

    /** Equivalente a chmod 400: em POSIX, só OWNER_READ; no Windows, uma ACL com uma única entrada de leitura para o dono. */
    private static void soLeituraParaODono(Path ficheiro) throws IOException {
        PosixFileAttributeView posix = Files.getFileAttributeView(ficheiro, PosixFileAttributeView.class);
        if (posix != null) {
            posix.setPermissions(EnumSet.of(PosixFilePermission.OWNER_READ));
            return;
        }
        AclFileAttributeView acl = Files.getFileAttributeView(ficheiro, AclFileAttributeView.class);
        if (acl == null) {
            throw new IOException("o sistema de ficheiros não suporta permissões POSIX nem ACL");
        }
        acl.setAcl(List.of(AclEntry.newBuilder()
                .setType(AclEntryType.ALLOW)
                .setPrincipal(acl.getOwner())
                .setPermissions(AclEntryPermission.READ_DATA, AclEntryPermission.READ_ATTRIBUTES,
                        AclEntryPermission.READ_NAMED_ATTRS, AclEntryPermission.READ_ACL,
                        AclEntryPermission.SYNCHRONIZE)
                .build()));
    }

    // ---------------------------------------------------------------------------------------------
    // Registo durável
    // ---------------------------------------------------------------------------------------------

    /**
     * Lê registos.log e confirma que os LSN são 1, 2, 3, ..., sem repetições nem saltos. Uma última linha
     * sem '\n' é uma escrita interrompida, que nunca foi confirmada ao utilizador: com cortarIncompleta o
     * ficheiro é cortado antes dela, sem ela é só ignorada. Qualquer outra irregularidade lança
     * IllegalStateException, o que recusa o arranque.
     */
    static List<Registo> lerRegistos(Path ficheiro, boolean cortarIncompleta) throws IOException {
        List<Registo> registos = new ArrayList<>();
        if (!Files.exists(ficheiro)) {
            return registos;
        }
        byte[] dados = Files.readAllBytes(ficheiro);
        int fim = dados.length;
        while (fim > 0 && dados[fim - 1] != '\n') {
            fim--;
        }
        if (fim < dados.length) {
            System.err.println(ficheiro + ": a última linha está incompleta (" + (dados.length - fim) + " bytes) e foi "
                    + (cortarIncompleta ? "removida" : "ignorada"));
            if (cortarIncompleta) {
                try (FileChannel c = FileChannel.open(ficheiro, StandardOpenOption.WRITE)) {
                    c.truncate(fim);
                    c.force(true);
                }
            }
        }
        String[] linhas = new String(dados, 0, fim, StandardCharsets.UTF_8).split("\n", -1);
        for (int i = 0; i < linhas.length - 1; i++) { // a seguir ao último '\n' fica uma string vazia
            String linha = linhas[i].endsWith("\r") ? linhas[i].substring(0, linhas[i].length() - 1) : linhas[i];
            Registo registo;
            try {
                registo = Registo.deLinha(linha);
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException(ficheiro + ", linha " + (i + 1) + ": " + e.getMessage());
            }
            if (registo.lsn() != registos.size() + 1L) {
                throw new IllegalStateException(ficheiro + ", linha " + (i + 1) + ": LSN " + registo.lsn()
                        + " quando se esperava " + (registos.size() + 1L));
            }
            registos.add(registo);
        }
        return registos;
    }

    /**
     * Caminho de escrita comum a /registar, às mensagens de #arquivo e a /rajada: atribui o LSN seguinte,
     * acrescenta a linha a registos.log com fsync e só depois junta o registo ao chunk aberto. Devolve o
     * LSN atribuído; quem chama só responde ao utilizador depois de este método terminar.
     */
    private synchronized long registar(String autor, String canal, String texto) throws IOException {
        if (logIncerto) {
            throw new IOException(REGISTOS_LOG + " ficou num estado incerto depois de uma falha de escrita; reinicie o líder");
        }
        Registo registo = new Registo(proximoLsn, System.currentTimeMillis(), Registo.limpar(autor),
                Registo.limpar(canal), texto);
        acrescentarAoLog(registo);
        proximoLsn++;
        evento(registo.instante(), "REGISTO",
                "lsn=" + registo.lsn() + ";autor=" + registo.autor() + ";canal=" + registo.canal());

        if (chunkAberto.isEmpty()) { // primeiro registo de um chunk novo: a selagem por tempo conta a partir daqui
            List<Registo> alvo = chunkAberto;
            selagemAgendada = temporizador.schedule(() -> selarPorTempo(alvo), Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
        }
        chunkAberto.add(registo);
        bytesChunkAberto += Chunk.bytes(registo);
        if (bytesChunkAberto >= Wire.CHUNK_MAX_BYTES) {
            selar(); // o registo que faz o chunk chegar ao limite sela-o de imediato
        }
        return registo.lsn();
    }

    /** Acrescenta a linha do registo a registos.log e força-a para o disco (fsync) antes de voltar. */
    private void acrescentarAoLog(Registo registo) throws IOException {
        long tamanhoAntes = log.size();
        try {
            escreverTudo(log, (registo.paraLinha() + "\n").getBytes(StandardCharsets.UTF_8));
            log.force(true);
        } catch (IOException e) {
            try {
                log.truncate(tamanhoAntes); // desfaz uma escrita parcial: o LSN não fica gasto nem repetido
            } catch (IOException ignorada) {
                logIncerto = true;
            }
            throw e;
        }
    }

    /** Acrescenta instante;evento;detalhe a eventos-lider.txt. Uma falha aqui não desfaz o registo. */
    private synchronized void evento(long instante, String tipo, String detalhe) {
        System.out.println(tipo + " " + detalhe);
        try {
            escreverTudo(eventos, (instante + ";" + tipo + ";" + detalhe + "\n").getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.err.println("Falha ao escrever em " + EVENTOS + ": " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Chunk aberto, selagem e cifra
    // ---------------------------------------------------------------------------------------------

    /** Tarefa do temporizador: sela o chunk para o qual foi agendada, se ainda for o chunk aberto. */
    private synchronized void selarPorTempo(List<Registo> alvo) {
        if (alvo == chunkAberto && !chunkAberto.isEmpty()) {
            selar();
        }
    }

    /**
     * Sela o chunk aberto: fixa os seus registos num Chunk imutável, serializa-o com padding, cifra-o
     * com uma DEK derivada da master key, grava-o em selados/ e regista o evento SELADO. O registo
     * seguinte abre um chunk novo.
     */
    private synchronized void selar() {
        if (chunkAberto.isEmpty()) {
            return;
        }
        if (selagemAgendada != null) {
            selagemAgendada.cancel(false);
            selagemAgendada = null;
        }
        String motivo = bytesChunkAberto >= Wire.CHUNK_MAX_BYTES ? "tamanho" : "tempo";
        Chunk chunk = new Chunk(chunkAberto);
        chunkAberto = new ArrayList<>();
        bytesChunkAberto = Chunk.CABECALHO_BYTES;
        List<Registo> registos = chunk.registos();
        long lsnDe = registos.get(0).lsn();
        long lsnAte = registos.get(registos.size() - 1).lsn();
        try {
            byte[] texto = chunk.serializar(Wire.PAD_BYTES);
            ChunkCrypto.Sealed selado = crypto.seal(texto);
            gravarSelado(selado);
            evento(System.currentTimeMillis(), "SELADO", "chunkId=" + selado.id + ";lsnDe=" + lsnDe
                    + ";lsnAte=" + lsnAte + ";registos=" + registos.size() + ";tamanho=" + texto.length
                    + ";bin=" + selado.ciphertext.length + ";motivo=" + motivo);
        } catch (IOException | GeneralSecurityException | RuntimeException e) {
            // os registos continuam em registos.log; só este chunk fica por gravar em selados/
            evento(System.currentTimeMillis(), "ERRO", "selagem;lsnDe=" + lsnDe + ";lsnAte=" + lsnAte
                    + ";causa=" + Registo.limpar(e.toString()));
        }
    }

    /** Grava selados/&lt;chunkId&gt;.hdr e depois selados/&lt;chunkId&gt;.bin: um .bin tem sempre o seu .hdr. */
    private void gravarSelado(ChunkCrypto.Sealed selado) throws IOException {
        Base64.Encoder base64 = Base64.getEncoder();
        String cabecalho = selado.keyVersion + ";" + base64.encodeToString(selado.salt) + ";"
                + base64.encodeToString(selado.nonce) + "\n";
        gravarAtomico(pastaSelados.resolve(selado.id + ".hdr"), cabecalho.getBytes(StandardCharsets.US_ASCII));
        gravarAtomico(pastaSelados.resolve(selado.id + ".bin"), selado.ciphertext);
    }

    /** Escreve num ficheiro temporário, força-o para o disco e move-o para o destino com ATOMIC_MOVE. */
    private void gravarAtomico(Path destino, byte[] dados) throws IOException {
        try (FileChannel c = FileChannel.open(temporario,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            escreverTudo(c, dados);
            c.force(true);
        }
        Files.move(temporario, destino, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void escreverTudo(FileChannel canal, byte[] dados) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(dados);
        while (buffer.hasRemaining()) {
            canal.write(buffer);
        }
    }

    private static ThreadFactory daemon(String nome) {
        return tarefa -> {
            Thread thread = new Thread(tarefa, nome);
            thread.setDaemon(true);
            return thread;
        };
    }

    // ---------------------------------------------------------------------------------------------
    // Comandos e mensagens do Discord
    // ---------------------------------------------------------------------------------------------

    /**
     * Os números são STRING de propósito: com INTEGER o Discord bloqueia valores como
     * {@code abc} no cliente e o bot nunca os receberia para os recusar.
     */
    static List<CommandData> definirComandos() {
        return List.of(
                Commands.slash("registar", "Regista um texto no arquivo")
                        .addOption(OptionType.STRING, "texto", "Texto a registar", true),
                Commands.slash("estado", "Mostra o estado do arquivo"),
                Commands.slash("config", "Altera a configuração do arquivo")
                        .addSubcommands(new SubcommandData("r", "Define o valor de r")
                                .addOption(OptionType.STRING, "n", "Novo valor de r (inteiro, mínimo 1)", true)),
                Commands.slash("recuperar", "Recupera um intervalo de registos")
                        .addOption(OptionType.STRING, "de", "Primeiro registo (inteiro, mínimo 0)", true)
                        .addOption(OptionType.STRING, "ate", "Último registo (inteiro, não inferior a de)", true),
                Commands.slash("replica-token", "Gera um token para adicionar uma réplica"),
                Commands.slash("replica-remover", "Remove uma réplica")
                        .addOption(OptionType.STRING, "id", "Identificador da réplica", true),
                Commands.slash("rajada", "Envia uma rajada de registos de teste")
                        .addOption(OptionType.STRING, "n", "Número de registos (inteiro, mínimo 1)", true)
                        .addOption(OptionType.STRING, "intervalo-ms",
                                "Intervalo entre registos em ms (inteiro, mínimo 0)", true));
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String comando = event.getFullCommandName();
        UnaryOperator<String> argumentos = nome -> event.getOption(nome, OptionMapping::getAsString);
        // processar valida as permissões e os argumentos de todos os comandos; a resposta provisória que
        // devolve só é usada pelos comandos que ainda não estão implementados.
        Resposta validacao = processar(comando, temPapelAdmin(event.getMember()), argumentos);
        if (validacao.erro()) {
            event.reply(validacao.texto()).setEphemeral(true).queue();
        } else if (comando.equals("registar")) {
            comandoRegistar(event, argumentos.apply("texto"));
        } else if (comando.equals("rajada")) {
            comandoRajada(event, Long.parseLong(argumentos.apply("n").trim()),
                    Long.parseLong(argumentos.apply("intervalo-ms").trim()));
        } else {
            event.reply(validacao.texto()).queue();
        }
    }

    private void comandoRegistar(SlashCommandInteractionEvent event, String texto) {
        try {
            long lsn = registar(event.getUser().getName(), nomeCanal(event), texto);
            event.reply("Registado com LSN " + lsn + ": \"" + eco(texto) + "\"").queue();
        } catch (IOException e) {
            event.reply("Não foi possível registar: " + e.getMessage()).setEphemeral(true).queue();
        }
    }

    private void comandoRajada(SlashCommandInteractionEvent event, long n, long intervaloMs) {
        String autor = event.getUser().getName();
        String canal = nomeCanal(event);
        InteractionHook hook = event.getHook();
        Consumer<String> aviso = texto -> {
            System.out.println(texto);
            hook.sendMessage(texto).queue(null, erro -> { }); // o hook expira 15 minutos depois do comando
        };
        // a rajada só começa depois de o Discord ter aceitado a resposta ao comando
        event.reply("Rajada de " + n + " registos sintéticos iniciada (intervalo de " + intervaloMs + " ms).")
                .queue(enviada -> rajadas.execute(() -> rajada(n, intervaloMs, autor, canal, aviso)));
    }

    /** Injeta n registos sintéticos pelo mesmo método de /registar, com intervaloMs entre eles. */
    private void rajada(long n, long intervaloMs, String autor, String canal, Consumer<String> aviso) {
        long id = System.currentTimeMillis();
        long feitos = 0;
        long primeiro = 0;
        long ultimo = 0;
        try {
            for (long k = 1; k <= n; k++) {
                if (k > 1 && intervaloMs > 0) {
                    Thread.sleep(intervaloMs);
                }
                ultimo = registar(autor, canal, textoSintetico(id, k, n));
                if (k == 1) {
                    primeiro = ultimo;
                }
                feitos++;
            }
            aviso.accept("Rajada concluída: " + feitos + " registos sintéticos, LSN " + primeiro + " a " + ultimo + ".");
        } catch (IOException e) {
            aviso.accept("Rajada interrompida depois de " + feitos + " de " + n + " registos: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            aviso.accept("Rajada interrompida depois de " + feitos + " de " + n + " registos.");
        }
    }

    /** Texto do registo sintético k de n, com TEXTO_SINTETICO_CHARS caracteres. */
    static String textoSintetico(long rajada, long k, long n) {
        StringBuilder texto = new StringBuilder("Registo sintético ").append(k).append('/').append(n)
                .append(" da rajada ").append(rajada).append('.');
        while (texto.length() < TEXTO_SINTETICO_CHARS) {
            texto.append(" Lorem ipsum dolor sit amet, consectetur adipiscing elit.");
        }
        texto.setLength(TEXTO_SINTETICO_CHARS);
        return texto.toString();
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (!event.isFromGuild() || event.getGuild().getIdLong() != guildId) {
            return;
        }
        if (event.getAuthor().isBot() || !CANAL_ARQUIVO.equals(event.getChannel().getName())) {
            return;
        }
        if (event.getMessage().getType().isSystem()) {
            return; // avisos do próprio Discord, como "afixou uma mensagem", não são mensagens enviadas
        }
        String autor = event.getAuthor().getName();
        try {
            long lsn = registar(autor, CANAL_ARQUIVO, event.getMessage().getContentRaw());
            // sendMessage em vez de reply: responder a uma mensagem exige a permissão de ler o histórico.
            event.getChannel().sendMessage("Mensagem de " + autor + " registada com LSN " + lsn).queue();
        } catch (IOException e) {
            System.err.println("Falha ao registar a mensagem de " + autor + ": " + e.getMessage());
            event.getChannel().sendMessage("Não foi possível registar a mensagem de " + autor + ": "
                    + e.getMessage()).queue();
        }
    }

    private static String nomeCanal(SlashCommandInteractionEvent event) {
        return event.getChannel() == null ? "" : event.getChannel().getName();
    }

    private static boolean temPapelAdmin(Member membro) {
        return membro != null
                && membro.getRoles().stream().anyMatch(papel -> PAPEL_ADMIN.equals(papel.getName()));
    }

    /** Separado dos eventos do JDA para se poder validar sem ligação ao Discord. */
    static Resposta processar(String comando, boolean admin, UnaryOperator<String> argumentos) {
        if (COMANDOS_ADMIN.contains(comando) && !admin) {
            return new Resposta("Sem permissão: `/" + comando + "` é exclusivo de membros com o papel `"
                    + PAPEL_ADMIN + "`.", true);
        }
        try {
            String recebidos = validarArgumentos(comando, argumentos);
            String detalhe = recebidos.isEmpty() ? "" : ": " + recebidos;
            return new Resposta("`/" + comando + "` recebido" + detalhe + " — ainda não implementado", false);
        } catch (ArgumentoInvalido e) {
            return new Resposta("Argumento inválido: " + e.getMessage(), true);
        }
    }

    private static String validarArgumentos(String comando, UnaryOperator<String> arg) {
        return switch (comando) {
            case "registar" -> "texto=\"" + eco(preenchido("texto", arg)) + "\"";
            case "estado", "replica-token" -> "";
            case "config r" -> "n=" + inteiro("n", arg, 1);
            case "recuperar" -> {
                long de = inteiro("de", arg, 0);
                long ate = inteiro("ate", arg, 0);
                if (de > ate) {
                    throw new ArgumentoInvalido("`de` (" + de + ") não pode ser maior do que `ate` (" + ate + ").");
                }
                yield "de=" + de + ", ate=" + ate;
            }
            case "replica-remover" -> "id=" + identificador("id", arg);
            case "rajada" -> "n=" + inteiro("n", arg, 1) + ", intervaloMs=" + inteiro("intervalo-ms", arg, 0);
            default -> throw new ArgumentoInvalido("comando desconhecido `/" + comando + "`.");
        };
    }

    private static String preenchido(String nome, UnaryOperator<String> arg) {
        String valor = arg.apply(nome);
        if (valor == null || valor.isBlank()) {
            throw new ArgumentoInvalido("`" + nome + "` não pode estar vazio.");
        }
        return valor.trim();
    }

    private static long inteiro(String nome, UnaryOperator<String> arg, long minimo) {
        String valor = preenchido(nome, arg);
        long numero;
        try {
            numero = Long.parseLong(valor);
        } catch (NumberFormatException e) {
            throw new ArgumentoInvalido("`" + nome + "` tem de ser um número inteiro (recebido: \"" + eco(valor) + "\").");
        }
        if (numero < minimo) {
            throw new ArgumentoInvalido("`" + nome + "` tem de ser pelo menos " + minimo + " (recebido: " + numero + ").");
        }
        return numero;
    }

    private static String identificador(String nome, UnaryOperator<String> arg) {
        String valor = preenchido(nome, arg);
        if (!valor.matches(FORMATO_ID)) {
            throw new ArgumentoInvalido("`" + nome + "` só pode ter letras, números, `_` e `-`, até 64 caracteres"
                    + " (recebido: \"" + eco(valor) + "\").");
        }
        return valor;
    }

    private static String eco(String texto) {
        return texto.length() <= MAX_ECO ? texto : texto.substring(0, MAX_ECO) + "…";
    }

    // ---------------------------------------------------------------------------------------------
    // Modo abrir: o líder abre os chunks selados, sem ligar ao Discord
    // ---------------------------------------------------------------------------------------------

    /**
     * Sem chunkIds, abre todos os chunks de selados/ com a master.key e confere cada um com o seu evento
     * SELADO e com registos.log. Com chunkIds, abre esses e lista os seus registos. Devolve 0 se todos
     * abrirem e coincidirem e 1 caso contrário.
     */
    static int abrir(Path pasta, List<String> chunkIds) throws IOException {
        ChunkCrypto crypto;
        try {
            crypto = new ChunkCrypto(masterKey(pasta.resolve(MASTER_KEY), false), KEY_VERSION);
        } catch (IllegalStateException e) {
            System.err.println("Não é possível abrir os chunks: " + e.getMessage());
            return 1;
        }
        Path selados = pasta.resolve(SELADOS);
        int falhas = 0;

        if (!chunkIds.isEmpty()) {
            for (String argumento : chunkIds) {
                // aceita o chunkId, o nome do ficheiro ou o caminho (selados/<id>.bin)
                String id = argumento.replaceFirst("^.*[\\\\/]", "").replaceFirst("\\.(bin|hdr)$", "");
                try {
                    List<Registo> registos = Chunk.ler(crypto.open(lerSelado(selados, id))).registos();
                    System.out.println(id + ": " + registos.size() + " registos (lsn;instante;autor;canal;texto)");
                    for (Registo r : registos) {
                        System.out.println(r.lsn() + ";" + r.instante() + ";" + r.autor() + ";" + r.canal() + ";" + r.texto());
                    }
                } catch (Exception e) {
                    System.out.println("FALHA  " + id + ": " + e);
                    falhas++;
                }
            }
            return falhas == 0 ? 0 : 1;
        }

        Map<Long, Registo> log = new HashMap<>();
        try {
            for (Registo r : lerRegistos(pasta.resolve(REGISTOS_LOG), false)) {
                log.put(r.lsn(), r);
            }
        } catch (IllegalStateException e) {
            System.err.println("Não é possível conferir os chunks com " + REGISTOS_LOG + ": " + e.getMessage());
            return 1;
        }
        Map<String, long[]> selado = eventosSelado(pasta.resolve(EVENTOS));
        List<String> ids = new ArrayList<>();
        if (Files.isDirectory(selados)) {
            try (DirectoryStream<Path> bins = Files.newDirectoryStream(selados, "*.bin")) {
                for (Path bin : bins) {
                    String nome = bin.getFileName().toString();
                    ids.add(nome.substring(0, nome.length() - ".bin".length()));
                }
            }
        }
        ids.sort(Comparator.comparingLong((String id) -> selado.containsKey(id) ? selado.get(id)[0] : Long.MAX_VALUE)
                .thenComparing(Comparator.naturalOrder()));

        TreeSet<Long> cobertos = new TreeSet<>();
        for (String id : ids) {
            try {
                byte[] texto = crypto.open(lerSelado(selados, id));
                List<Registo> registos = Chunk.ler(texto).registos();
                registos.forEach(r -> cobertos.add(r.lsn()));
                String diferenca = conferir(texto, registos, selado.get(id), log);
                if (diferenca == null) {
                    System.out.println("OK     " + id + "  LSN " + registos.get(0).lsn() + "-"
                            + registos.get(registos.size() - 1).lsn() + ", " + registos.size() + " registos, "
                            + texto.length + " bytes (" + texto.length / Wire.PAD_BYTES + " x " + Wire.PAD_BYTES + ")");
                } else {
                    System.out.println("DIFERE " + id + ": " + diferenca);
                    falhas++;
                }
            } catch (Exception e) {
                System.out.println("FALHA  " + id + ": " + e);
                falhas++;
            }
        }
        System.out.println(ids.size() + " chunks em " + selados.toAbsolutePath() + ", " + falhas + " com falhas");
        TreeSet<Long> foraDeChunks = new TreeSet<>(log.keySet());
        foraDeChunks.removeAll(cobertos);
        if (!foraDeChunks.isEmpty()) {
            System.out.println("LSN de " + REGISTOS_LOG + " que não estão em nenhum chunk selado: " + intervalos(foraDeChunks));
        }
        return falhas == 0 ? 0 : 1;
    }

    /** Lê selados/&lt;id&gt;.hdr (keyVersion;salt;nonce, com salt e nonce em Base64) e selados/&lt;id&gt;.bin. */
    static ChunkCrypto.Sealed lerSelado(Path pasta, String id) throws IOException {
        String[] cabecalho = Files.readString(pasta.resolve(id + ".hdr"), StandardCharsets.US_ASCII).trim().split(";", -1);
        if (cabecalho.length != 3) {
            throw new IOException(id + ".hdr não tem a forma keyVersion;salt;nonce");
        }
        Base64.Decoder base64 = Base64.getDecoder();
        return new ChunkCrypto.Sealed(id, Integer.parseInt(cabecalho[0]), base64.decode(cabecalho[1]),
                base64.decode(cabecalho[2]), Files.readAllBytes(pasta.resolve(id + ".bin")));
    }

    /** null se o chunk coincidir com o evento SELADO e com registos.log; caso contrário, a primeira diferença. */
    private static String conferir(byte[] texto, List<Registo> registos, long[] evento, Map<Long, Registo> log) {
        if (texto.length % Wire.PAD_BYTES != 0) {
            return "o tamanho " + texto.length + " não é múltiplo de PAD_BYTES (" + Wire.PAD_BYTES + ")";
        }
        if (registos.isEmpty()) {
            return "o chunk não tem registos";
        }
        long de = registos.get(0).lsn();
        long ate = registos.get(registos.size() - 1).lsn();
        if (evento == null) {
            return "não há evento SELADO para este chunk em " + EVENTOS;
        }
        if (evento[0] != de || evento[1] != ate) {
            return "o evento SELADO indica LSN " + evento[0] + "-" + evento[1] + " e o chunk tem " + de + "-" + ate;
        }
        for (int i = 0; i < registos.size(); i++) {
            Registo r = registos.get(i);
            if (r.lsn() != de + i) {
                return "os LSN não são consecutivos (" + r.lsn() + " depois de " + (de + i - 1) + ")";
            }
            if (!r.equals(log.get(r.lsn()))) {
                return "o registo com LSN " + r.lsn() + " difere do de " + REGISTOS_LOG;
            }
        }
        return null;
    }

    /** chunkId -> {lsnDe, lsnAte}, a partir dos eventos SELADO de eventos-lider.txt. */
    private static Map<String, long[]> eventosSelado(Path ficheiro) throws IOException {
        Map<String, long[]> intervalos = new HashMap<>();
        if (!Files.exists(ficheiro)) {
            return intervalos;
        }
        for (String linha : Files.readAllLines(ficheiro, StandardCharsets.UTF_8)) {
            String[] campos = linha.trim().split(";");
            if (campos.length < 3 || !campos[1].equals("SELADO")) {
                continue;
            }
            Map<String, String> detalhe = new HashMap<>();
            for (int i = 2; i < campos.length; i++) {
                int igual = campos[i].indexOf('=');
                if (igual > 0) {
                    detalhe.put(campos[i].substring(0, igual), campos[i].substring(igual + 1));
                }
            }
            try {
                intervalos.put(detalhe.get("chunkId"), new long[] {
                        Long.parseLong(detalhe.get("lsnDe")), Long.parseLong(detalhe.get("lsnAte"))});
            } catch (NumberFormatException e) {
                // linha SELADO incompleta: o chunk aparece como não tendo evento
            }
        }
        return intervalos;
    }

    /** {1, 2, 3, 7, 8} -> "1-3, 7-8". */
    private static String intervalos(TreeSet<Long> lsns) {
        StringBuilder texto = new StringBuilder();
        long inicio = lsns.first();
        long anterior = inicio;
        for (long lsn : lsns.tailSet(inicio, false)) {
            if (lsn != anterior + 1) {
                texto.append(inicio == anterior ? inicio : inicio + "-" + anterior).append(", ");
                inicio = lsn;
            }
            anterior = lsn;
        }
        return texto.append(inicio == anterior ? inicio : inicio + "-" + anterior).toString();
    }

    // ---------------------------------------------------------------------------------------------
    // Configuração (bot.properties e variáveis de ambiente)
    // ---------------------------------------------------------------------------------------------

    private static Properties lerFicheiroConfig() throws IOException {
        Properties propriedades = new Properties();
        Path ficheiro = Path.of(FICHEIRO_CONFIG);
        if (Files.isRegularFile(ficheiro)) {
            try (Reader leitor = Files.newBufferedReader(ficheiro, StandardCharsets.UTF_8)) {
                propriedades.load(leitor);
            }
        }
        return propriedades;
    }

    /** A variável de ambiente tem prioridade sobre o ficheiro local. */
    private static String config(Properties ficheiro, String chave) {
        String valor = System.getenv(chave);
        if (valor == null || valor.isBlank()) {
            valor = ficheiro.getProperty(chave, "");
        }
        if (valor.isBlank()) {
            terminar("Falta " + chave + ": define a variável de ambiente ou preenche "
                    + Path.of(FICHEIRO_CONFIG).toAbsolutePath());
        }
        return valor.trim();
    }

    private static long guildIdDe(String valor) {
        try {
            return Long.parseLong(valor);
        } catch (NumberFormatException e) {
            terminar("DISCORD_GUILD_ID tem de ser o ID numérico do servidor (recebido: " + valor + ")");
            throw e;
        }
    }

    private static void terminar(String mensagem) {
        System.err.println(mensagem);
        System.exit(1);
    }

    record Resposta(String texto, boolean erro) {
    }

    private static final class ArgumentoInvalido extends RuntimeException {
        ArgumentoInvalido(String mensagem) {
            super(mensagem);
        }
    }
}
