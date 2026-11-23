package tpf;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Chunk em claro: os registos de um chunk selado e o seu formato binário, que é o texto cifrado por
 * ChunkCrypto. Inteiros em big-endian, a ordem de ByteBuffer e de DataOutputStream:
 *
 * <pre>
 * cabeçalho, 9 bytes
 *   4 bytes   magia "TPFC" em ASCII
 *   1 byte    versão do formato, 1
 *   4 bytes   n, número de registos (int)
 * n registos, cada um com 28 bytes fixos seguidos dos três textos
 *   8 bytes   lsn (long)
 *   8 bytes   instante (long, ms desde 1970-01-01T00:00Z)
 *   4 bytes   comprimento do autor em bytes (int), seguido do autor em UTF-8
 *   4 bytes   comprimento do canal em bytes (int), seguido do canal em UTF-8
 *   4 bytes   comprimento do texto em bytes (int), seguido do texto em UTF-8
 * padding
 *   bytes 0x00 até o total ser múltiplo de PAD_BYTES (nenhum, se já o for)
 * </pre>
 *
 * @param registos os registos do chunk, por ordem de LSN; a lista é copiada e não pode ser alterada
 */
public record Chunk(List<Registo> registos) {

    static final int CABECALHO_BYTES = 9;
    private static final byte[] MAGIA = {'T', 'P', 'F', 'C'};
    private static final byte VERSAO = 1;

    /** Depois de selado, o chunk é imutável: guarda uma cópia que não pode ser alterada. */
    public Chunk {
        registos = List.copyOf(registos);
    }

    /** Bytes que um registo ocupa no formato serializado. */
    static int bytes(Registo r) {
        return 8 + 8 + 4 + utf8(r.autor()).length + 4 + utf8(r.canal()).length + 4 + utf8(r.texto()).length;
    }

    /** Tamanho serializado sem o padding: cabeçalho e registos. */
    public int tamanho() {
        long total = CABECALHO_BYTES;
        for (Registo r : registos) {
            total += bytes(r);
        }
        return Math.toIntExact(total);
    }

    /** Serializa os registos e completa com bytes a zero até um múltiplo de padBytes. */
    public byte[] serializar(int padBytes) {
        if (padBytes < 1) {
            throw new IllegalArgumentException("padBytes tem de ser pelo menos 1 (recebido: " + padBytes + ")");
        }
        long util = tamanho();
        int total = Math.toIntExact((util + padBytes - 1) / padBytes * padBytes);
        ByteBuffer b = ByteBuffer.allocate(total); // um array novo já está a zeros: o que sobrar é o padding
        b.put(MAGIA).put(VERSAO).putInt(registos.size());
        for (Registo r : registos) {
            b.putLong(r.lsn()).putLong(r.instante());
            escrever(b, r.autor());
            escrever(b, r.canal());
            escrever(b, r.texto());
        }
        return b.array();
    }

    /**
     * Leitura inversa de serializar: reconstrói os registos e ignora o padding. Lança
     * IllegalArgumentException se os dados não estiverem neste formato.
     */
    public static Chunk ler(byte[] dados) {
        ByteBuffer b = ByteBuffer.wrap(dados);
        try {
            byte[] magia = new byte[MAGIA.length];
            b.get(magia);
            if (!Arrays.equals(magia, MAGIA)) {
                throw new IllegalArgumentException("não é um chunk: a magia não é TPFC");
            }
            byte versao = b.get();
            if (versao != VERSAO) {
                throw new IllegalArgumentException("versão de formato desconhecida: " + versao);
            }
            int n = b.getInt();
            if (n < 0) {
                throw new IllegalArgumentException("número de registos negativo: " + n);
            }
            List<Registo> registos = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                long lsn = b.getLong();
                long instante = b.getLong();
                String autor = lerTexto(b);
                String canal = lerTexto(b);
                String texto = lerTexto(b);
                registos.add(new Registo(lsn, instante, autor, canal, texto));
            }
            return new Chunk(registos); // o que fica por ler em b é padding
        } catch (BufferUnderflowException e) {
            throw new IllegalArgumentException("chunk truncado: faltam bytes para os registos indicados", e);
        }
    }

    private static void escrever(ByteBuffer b, String texto) {
        byte[] bytes = utf8(texto);
        b.putInt(bytes.length).put(bytes);
    }

    private static String lerTexto(ByteBuffer b) {
        int comprimento = b.getInt();
        if (comprimento < 0 || comprimento > b.remaining()) {
            throw new IllegalArgumentException("comprimento de texto inválido: " + comprimento);
        }
        byte[] bytes = new byte[comprimento];
        b.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String texto) {
        return texto.getBytes(StandardCharsets.UTF_8);
    }
}
