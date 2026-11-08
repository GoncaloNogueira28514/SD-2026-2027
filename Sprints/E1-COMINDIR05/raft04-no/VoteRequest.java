package raft04;

// PEDIDO_VOTO;<termo>;<candidato>;<ultimoSeq>;<ultimoTermo>
public final class VoteRequest {
    public final long term;        // termo da candidatura
    public final int candidate;    // identificador i do candidato
    public final long lastSeq;     // seq do último registo do candidato
    public final long lastTerm;    // termo do último registo do candidato

    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    public String encode() {
        return "PEDIDO_VOTO;" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }

    public static VoteRequest decode(String msg) {
        String[] f = msg.trim().split(";");
        if (f.length != 5 || !f[0].equals("PEDIDO_VOTO")) {
            throw new IllegalArgumentException("PEDIDO_VOTO mal formado: " + msg);
        }
        return new VoteRequest(Long.parseLong(f[1]), Integer.parseInt(f[2]), Long.parseLong(f[3]),
                Long.parseLong(f[4]));
    }

    @Override
    public String toString() {
        return encode();
    }
}
