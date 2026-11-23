package tpf;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * Um registo do arquivo, em claro. Só existe no líder: numa linha de registos.log e, depois de selado,
 * dentro de um chunk cifrado.
 *
 * @param lsn      número de sequência do log, atribuído pelo líder a partir de 1
 * @param instante instante em que o líder aceitou o registo, em ms desde 1970-01-01T00:00Z
 * @param autor    nome do utilizador Discord que o enviou
 * @param canal    nome do canal Discord onde foi enviado
 * @param texto    conteúdo do registo
 */
public record Registo(long lsn, long instante, String autor, String canal, String texto) {

    /**
     * O autor e o canal ficam em claro, separados por ';', em registos.log e em eventos-lider.txt, por
     * isso não podem conter ';' nem quebras de linha. O texto pode conter tudo: vai em Base64.
     */
    public Registo {
        Objects.requireNonNull(autor, "autor");
        Objects.requireNonNull(canal, "canal");
        Objects.requireNonNull(texto, "texto");
        if (!limpar(autor).equals(autor) || !limpar(canal).equals(canal)) {
            throw new IllegalArgumentException("autor e canal não podem conter ';' nem quebras de linha");
        }
    }

    /** Troca por '_' os caracteres que partiriam uma linha lsn;instante;autor;canal;texto. */
    public static String limpar(String campo) {
        return campo == null ? "" : campo.replaceAll("[;\\r\\n]", "_");
    }

    /** Linha de registos.log, sem a quebra de linha: lsn;instante;autor;canal;texto, com o texto em Base64. */
    public String paraLinha() {
        return lsn + ";" + instante + ";" + autor + ";" + canal + ";"
                + Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
    }

    /** Inverso de paraLinha; lança IllegalArgumentException se a linha estiver mal formada. */
    public static Registo deLinha(String linha) {
        String[] campos = linha.split(";", -1);
        if (campos.length != 5) {
            throw new IllegalArgumentException("esperados 5 campos separados por ';', encontrados " + campos.length);
        }
        try {
            return new Registo(Long.parseLong(campos[0]), Long.parseLong(campos[1]), campos[2], campos[3],
                    new String(Base64.getDecoder().decode(campos[4]), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) { // inclui NumberFormatException e Base64 inválido
            throw new IllegalArgumentException("campo inválido (" + e.getMessage() + ")", e);
        }
    }
}
