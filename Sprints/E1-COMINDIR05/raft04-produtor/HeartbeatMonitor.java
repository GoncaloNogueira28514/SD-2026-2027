package raft04;

import java.util.function.Consumer;

// Detetor de falhas do líder de uma partição, alimentado pelos cabeçalhos dos LOTE.
// O líder deixa de ser identificado pela incarnação e passa a sê-lo pelo termo.
public final class HeartbeatMonitor {
    public enum State { DESCONHECIDO, ATIVO, SUSPEITO }

    private final int partition;
    private final long timeoutNanos;
    private final Consumer<String> log;

    private State state = State.DESCONHECIDO;
    private long lastNanos;   // instante local da última receção
    private long term = 0;    // maior termo observado (0 = nenhum líder conhecido)
    private int leader = 0;   // líder desse termo
    private long hb = 0;      // último heartbeat observado nesse termo

    public HeartbeatMonitor(int partition, long periodMs, int factor, Consumer<String> log) {
        this.partition = partition;
        this.timeoutNanos = periodMs * factor * 1_000_000L;
        this.log = log;
        this.lastNanos = System.nanoTime();
    }

    // thread de receção: chegou um LOTE da partição
    public synchronized void onHeartbeat(long term, int leader, long hb) {
        if (term < this.term) return;                    // líder obsoleto
        if (term == this.term && hb <= this.hb) return;  // duplicado ou atrasado
        if (term > this.term) {
            log.accept("NOVO_LIDER;p" + partition + ";termo=" + this.term + "->" + term + ";lider=n" + leader);
            this.leader = leader;
        } else if (hb != this.hb + 1) {
            log.accept("HB_PERDIDO;p" + partition + ";termo=" + term + ";esperado=" + (this.hb + 1)
                    + ";recebido=" + hb);
        }
        if (state == State.SUSPEITO) {
            log.accept("RETOMA;p" + partition + ";termo=" + term + ";lider=n" + leader + ";hb=" + hb);
        }
        this.term = term;
        this.hb = hb;
        this.lastNanos = System.nanoTime();
        this.state = State.ATIVO;
    }

    // thread de verificação: chamada a cada VERIFICACAO_MS
    public synchronized void check() {
        long elapsed = System.nanoTime() - lastNanos;
        if (state != State.SUSPEITO && elapsed > timeoutNanos) {
            state = State.SUSPEITO;
            log.accept("SUSPEITA;p" + partition + ";termo=" + term
                    + ";lider=" + (leader == 0 ? "desconhecido" : "n" + leader)
                    + ";silencio_ms=" + elapsed / 1_000_000);
        }
    }

    public synchronized State state() {
        return state;
    }

    public synchronized long term() {
        return term;
    }

    public synchronized int leader() {
        return leader;
    }
}
