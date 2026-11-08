package raft04;

// VOTO;<termo>;<eleitor>;<candidato>;<S|N>
public final class VoteReply {
    public final long term;        // termo do eleitor depois de tratar o pedido
    public final int voter;        // identificador i de quem responde
    public final int candidate;    // a quem se dirige a resposta
    public final boolean granted;  // S = concedido, N = recusado

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";" + (granted ? "S" : "N");
    }

    public static VoteReply decode(String msg) {
        String[] f = msg.trim().split(";");
        if (f.length != 5 || !f[0].equals("VOTO") || !(f[4].equals("S") || f[4].equals("N"))) {
            throw new IllegalArgumentException("VOTO mal formado: " + msg);
        }
        return new VoteReply(Long.parseLong(f[1]), Integer.parseInt(f[2]), Integer.parseInt(f[3]),
                f[4].equals("S"));
    }

    @Override
    public String toString() {
        return encode();
    }
}
