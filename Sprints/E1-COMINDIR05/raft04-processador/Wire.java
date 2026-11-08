package raft04;

import java.nio.file.Files;
import java.nio.file.Paths;

public final class Wire {
    public static final int T_TCP_MS = 5000;       // tempo máximo de ligação e de espera por cada linha
    public static final int PORTO_BASE = 7000;     // porto de recuperação do nó i da partição k: 7000 + 10k + i
    public static final double LIMIAR_ALARME = 28.0;

    // período do heartbeat (Δ); -Draft04.periodo=<ms>
    public static final long PERIODO_HB_MS = Long.getLong("raft04.periodo", 2000L);
    // TIMEOUT dos detetores do produtor e do processador = FATOR_TIMEOUT × Δ; -Draft04.fator=<f>
    public static final int FATOR_TIMEOUT = Integer.getInteger("raft04.fator", 3);
    // período com que cada processo verifica os seus prazos (eleição e detetores)
    public static final long VERIFICACAO_MS = 200;

    // prazo de eleição sorteado em [ELEICAO_MIN_MS, ELEICAO_MAX_MS]; -Draft04.eleicaoMin / -Draft04.eleicaoMax
    public static final long ELEICAO_MIN_MS = Long.getLong("raft04.eleicaoMin", 3 * PERIODO_HB_MS);
    public static final long ELEICAO_MAX_MS = Long.getLong("raft04.eleicaoMax", 6 * PERIODO_HB_MS);
    // número de nós de cada partição de dados (G)
    public static final int GRUPO = 3;

    // Falha deliberada do ponto 10 (0 = código correto), -Draft04.falha=<1..5>:
    // 1 maioria com votes.size() >= groupSize / 2; 2 termo e votedFor só em memória;
    // 3 voto sem a verificação upToDate; 4 seguidor aplica lotes de termo inferior;
    // 5 novo líder inicializa L e V a zero
    public static final int FALHA = Integer.getInteger("raft04.falha", 0);

    private Wire() { }

    public static String topicIn(int k)       { return "raft04-in-p" + k; }
    public static String topicData(int k)     { return "raft04-p" + k; }
    public static String topicElection(int k) { return "raft04-eleicao-p" + k; }

    // porto TCP de recuperação do nó i da partição k (p0: 7001..7003, p1: 7011..7013, p2: 7021..7023)
    public static int nodePort(int k, int i) {
        return PORTO_BASE + 10 * k + i;
    }

    // Simula uma partição de rede: enquanto existir o ficheiro isolar-p<k>-n<i> na pasta de trabalho,
    // o nó não publica nem processa mensagens de raft04-p<k> e raft04-eleicao-p<k>, e as suas ligações
    // TCP de recuperação falham. O tópico raft04-in-p<k> não é afetado.
    public static boolean isolated(int k, int i) {
        return Files.exists(Paths.get("isolar-p" + k + "-n" + i));
    }

    public static String in(String sensor, double temp, long L, VectorClock V) {
        return "IN;" + sensor + ";" + temp + ";" + L + ";" + V.toString();
    }

    // tipo da mensagem (IN, LOTE, PEDIDO_VOTO, VOTO); lança IllegalArgumentException se não houver tipo
    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }
        return m.substring(0, i);
    }

    // conteúdo da mensagem sem o tipo
    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }
}
