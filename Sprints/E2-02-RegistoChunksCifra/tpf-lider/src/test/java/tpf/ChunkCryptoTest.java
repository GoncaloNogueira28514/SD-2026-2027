package tpf;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HexFormat;
import javax.crypto.AEADBadTagException;
import org.junit.jupiter.api.Test;

/**
 * Testes de ChunkCrypto: cifrar e decifrar, byte alterado no texto cifrado, salt, nonce e keyVersion
 * diferentes e master key diferente. Qualquer alteração tem de fazer open lançar uma exceção.
 */
class ChunkCryptoTest {

    private static final SecureRandom RNG = new SecureRandom();

    private final byte[] masterKey = aleatorio(32);
    private final ChunkCrypto crypto = new ChunkCrypto(masterKey, 1);
    /** Texto com padding, como o que sai de Chunk.serializar: dois blocos de 4096 bytes. */
    private final byte[] texto = aleatorio(2 * 4096);

    private static byte[] aleatorio(int n) {
        byte[] bytes = new byte[n];
        RNG.nextBytes(bytes);
        return bytes;
    }

    @Test
    void cifrarEDecifrar() throws GeneralSecurityException {
        ChunkCrypto.Sealed selado = crypto.seal(texto);

        assertArrayEquals(texto, crypto.open(selado));
        assertEquals(ChunkCrypto.sha256Hex(selado.ciphertext), selado.id);
        assertEquals(64, selado.id.length());
        assertEquals(1, selado.keyVersion);
        assertEquals(32, selado.salt.length);
        assertEquals(12, selado.nonce.length);
        assertEquals(texto.length + 16, selado.ciphertext.length); // tag GCM de 128 bits no fim
    }

    @Test
    void cadaChunkTemSaltENonceProprios() throws GeneralSecurityException {
        ChunkCrypto.Sealed a = crypto.seal(texto);
        ChunkCrypto.Sealed b = crypto.seal(texto);

        assertFalse(Arrays.equals(a.salt, b.salt));
        assertFalse(Arrays.equals(a.nonce, b.nonce));
        assertNotEquals(a.id, b.id); // o mesmo texto dá chunks diferentes
    }

    @Test
    void byteAlteradoNoTextoCifrado() throws GeneralSecurityException {
        ChunkCrypto.Sealed s = crypto.seal(texto);
        for (int posicao : new int[] {0, s.ciphertext.length / 2, s.ciphertext.length - 1}) { // a última é da tag
            byte[] alterado = s.ciphertext.clone();
            alterado[posicao] ^= 0x01;

            // o hash deixa de coincidir com o id: open falha antes de decifrar
            GeneralSecurityException e = assertThrows(GeneralSecurityException.class,
                    () -> crypto.open(new ChunkCrypto.Sealed(s.id, s.keyVersion, s.salt, s.nonce, alterado)));
            assertTrue(e.getMessage().startsWith("INTEGRIDADE"), e.getMessage());

            // mesmo que quem alterou recalcule o id, a autenticação GCM deteta a alteração
            String novoId = ChunkCrypto.sha256Hex(alterado);
            assertThrows(AEADBadTagException.class,
                    () -> crypto.open(new ChunkCrypto.Sealed(novoId, s.keyVersion, s.salt, s.nonce, alterado)));
        }
    }

    @Test
    void saltDiferente() throws GeneralSecurityException {
        ChunkCrypto.Sealed s = crypto.seal(texto);

        assertThrows(AEADBadTagException.class,
                () -> crypto.open(new ChunkCrypto.Sealed(s.id, s.keyVersion, aleatorio(32), s.nonce, s.ciphertext)));
    }

    @Test
    void nonceDiferente() throws GeneralSecurityException {
        ChunkCrypto.Sealed s = crypto.seal(texto);

        assertThrows(AEADBadTagException.class,
                () -> crypto.open(new ChunkCrypto.Sealed(s.id, s.keyVersion, s.salt, aleatorio(12), s.ciphertext)));
    }

    @Test
    void cabecalhoDeOutroChunk() throws GeneralSecurityException {
        ChunkCrypto.Sealed a = crypto.seal(texto);
        ChunkCrypto.Sealed b = crypto.seal(texto);

        // bytes de a com o salt e o nonce de b, como se uma réplica trocasse os .hdr de dois chunks
        assertThrows(AEADBadTagException.class,
                () -> crypto.open(new ChunkCrypto.Sealed(a.id, a.keyVersion, b.salt, b.nonce, a.ciphertext)));
    }

    @Test
    void keyVersionDiferente() throws GeneralSecurityException {
        ChunkCrypto.Sealed s = crypto.seal(texto);

        // o líder só conhece a versão 1: outra versão é recusada antes de decifrar
        GeneralSecurityException e = assertThrows(GeneralSecurityException.class,
                () -> crypto.open(new ChunkCrypto.Sealed(s.id, 2, s.salt, s.nonce, s.ciphertext)));
        assertTrue(e.getMessage().contains("versao de chave"), e.getMessage());

        // um líder com a versão 2 aceita o cabeçalho, mas o aad usado na cifra tinha a versão 1
        ChunkCrypto versao2 = new ChunkCrypto(masterKey, 2);
        assertThrows(AEADBadTagException.class,
                () -> versao2.open(new ChunkCrypto.Sealed(s.id, 2, s.salt, s.nonce, s.ciphertext)));
    }

    @Test
    void masterKeyDiferente() throws GeneralSecurityException {
        ChunkCrypto.Sealed s = crypto.seal(texto);
        ChunkCrypto outraChave = new ChunkCrypto(aleatorio(32), 1);

        assertThrows(AEADBadTagException.class, () -> outraChave.open(s));
    }

    @Test
    void masterKeyTemDeTer32Bytes() {
        assertThrows(IllegalArgumentException.class, () -> new ChunkCrypto(new byte[31], 1));
        assertThrows(IllegalArgumentException.class, () -> new ChunkCrypto(new byte[33], 1));
    }

    @Test
    void hkdfComUmBlocoDeExpansao() throws GeneralSecurityException {
        // RFC 5869, caso de teste 1: os primeiros 32 bytes do OKM são o bloco T(1) = HMAC(PRK, info || 0x01)
        byte[] ikm = new byte[22];
        Arrays.fill(ikm, (byte) 0x0b);
        byte[] salt = HexFormat.of().parseHex("000102030405060708090a0b0c");
        byte[] info = HexFormat.of().parseHex("f0f1f2f3f4f5f6f7f8f9");

        assertEquals("3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf",
                HexFormat.of().formatHex(ChunkCrypto.hkdf(ikm, salt, info)));
    }

    @Test
    void aadTemPrefixoVersaoSaltENonce() {
        byte[] salt = aleatorio(32);
        byte[] nonce = aleatorio(12);
        byte[] aad = ChunkCrypto.aad(1, salt, nonce);

        assertEquals(6 + 4 + 32 + 12, aad.length);
        assertArrayEquals("tpf-v1".getBytes(StandardCharsets.US_ASCII), Arrays.copyOfRange(aad, 0, 6));
        assertArrayEquals(new byte[] {0, 0, 0, 1}, Arrays.copyOfRange(aad, 6, 10));
        assertArrayEquals(salt, Arrays.copyOfRange(aad, 10, 42));
        assertArrayEquals(nonce, Arrays.copyOfRange(aad, 42, 54));
    }
}
