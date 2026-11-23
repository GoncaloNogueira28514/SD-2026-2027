package tpf;

/**
 * Parâmetros do arquivo. Neste sprint só existem os que o caminho de escrita do líder já usa; cada um
 * pode ser alterado com uma propriedade de sistema com o prefixo tpf., por exemplo -Dtpf.selagem=2000.
 */
public final class Wire {

    /** O chunk aberto é selado quando o seu tamanho serializado chega a este valor; -Dtpf.chunkMax=<bytes>. */
    public static final int CHUNK_MAX_BYTES = (int) parametro("tpf.chunkMax", 65536, Integer.MAX_VALUE);

    /** O chunk serializado é completado com padding até um múltiplo deste valor; -Dtpf.pad=<bytes>. */
    public static final int PAD_BYTES = (int) parametro("tpf.pad", 4096, Integer.MAX_VALUE);

    /** Tempo máximo entre o primeiro registo de um chunk e a sua selagem; -Dtpf.selagem=<ms>. */
    public static final long SELAGEM_MS = parametro("tpf.selagem", 5000, Long.MAX_VALUE);

    private Wire() {
    }

    /** Valor de -D&lt;propriedade&gt;, ou o valor por omissão; recusa o que não for um inteiro entre 1 e maximo. */
    private static long parametro(String propriedade, long omissao, long maximo) {
        String valor = System.getProperty(propriedade);
        if (valor == null) {
            return omissao;
        }
        try {
            long numero = Long.parseLong(valor.trim());
            if (numero >= 1 && numero <= maximo) {
                return numero;
            }
        } catch (NumberFormatException e) {
            // tratado a seguir, com a mesma mensagem
        }
        throw new IllegalArgumentException("-D" + propriedade + " tem de ser um inteiro entre 1 e " + maximo
                + " (recebido: \"" + valor + "\")");
    }
}
