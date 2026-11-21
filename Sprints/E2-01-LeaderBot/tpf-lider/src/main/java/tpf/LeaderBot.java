package tpf;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.UnaryOperator;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.entities.Message;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionMapping;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.CommandData;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;
import net.dv8tion.jda.api.utils.messages.MessageRequest;

/**
 * Base do líder: regista os comandos no servidor de teste e responde com
 * mensagens provisórias. A funcionalidade do arquivo fica para os próximos sprints.
 */
public class LeaderBot extends ListenerAdapter {

    private static final String FICHEIRO_CONFIG = "bot.properties";
    private static final String CANAL_ARQUIVO = "arquivo";
    private static final String PAPEL_ADMIN = "admin-arquivo";
    private static final Set<String> COMANDOS_ADMIN =
            Set.of("config r", "replica-token", "replica-remover", "rajada");

    /** Mantém as respostas abaixo do limite de 2000 caracteres do Discord. */
    private static final int MAX_ECO = 1500;
    private static final String FORMATO_ID = "[\\w-]{1,64}";

    private final long guildId;

    LeaderBot(long guildId) {
        this.guildId = guildId;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties ficheiro = lerFicheiroConfig();
        String token = config(ficheiro, "DISCORD_TOKEN");
        long guildId = guildIdDe(config(ficheiro, "DISCORD_GUILD_ID"));

        // O texto dos utilizadores é ecoado nas respostas e não deve notificar ninguém.
        MessageRequest.setDefaultMentions(EnumSet.noneOf(Message.MentionType.class));

        JDA jda = JDABuilder
                .createLight(token, EnumSet.of(GatewayIntent.GUILD_MESSAGES, GatewayIntent.MESSAGE_CONTENT))
                .addEventListeners(new LeaderBot(guildId))
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

        guild.updateCommands().addCommands(definirComandos()).queue(
                comandos -> System.out.println(comandos.size() + " comandos registados em " + guild.getName()),
                erro -> System.err.println("Falha ao registar os comandos: " + erro.getMessage()));
    }

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
        Resposta resposta = processar(event.getFullCommandName(), temPapelAdmin(event.getMember()),
                nome -> event.getOption(nome, OptionMapping::getAsString));
        event.reply(resposta.texto()).setEphemeral(resposta.erro()).queue();
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (!event.isFromGuild() || event.getGuild().getIdLong() != guildId) {
            return;
        }
        if (event.getAuthor().isBot() || !CANAL_ARQUIVO.equals(event.getChannel().getName())) {
            return;
        }
        String autor = event.getAuthor().getName();
        String conteudo = eco(event.getMessage().getContentRaw());
        System.out.println("#" + CANAL_ARQUIVO + " <" + autor + "> " + conteudo);
        // sendMessage em vez de reply: responder a uma mensagem exige a permissão de ler o histórico.
        event.getChannel()
                .sendMessage("Mensagem de " + autor + " detetada em #" + CANAL_ARQUIVO
                        + ": \"" + conteudo + "\" — ainda não implementado")
                .queue();
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
