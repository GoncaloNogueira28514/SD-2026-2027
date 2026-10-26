package rep03;

import java.util.Arrays;

public final class VectorClock {
    public enum Order {
        BEFORE, AFTER, EQUAL, CONCURRENT
    }

    private final long[] v;

    public VectorClock(int n) {
        this.v = new long[n];
    }

    private VectorClock(long[] v) {
        this.v = v;
    }

    // máximo componente a componente com outro vetor
    public void merge(VectorClock other) {
        for (int i = 0; i < v.length; i++) {
            v[i] = Math.max(v[i], other.v[i]);
        }
    }

    // evento local no processo k
    public void tick(int k) {
        v[k]++;
    }

    public long get(int k) {
        return v[k];
    }

    public VectorClock copy() {
        return new VectorClock(v.clone());
    }

    // relação de this com other
    public Order compare(VectorClock other) {
        if (this.v.length != other.v.length) {
            throw new IllegalArgumentException("Impossível comparar relógios com dimensões distintas.");
        }

        boolean menor = false, maior = false;
        for (int i = 0; i < v.length; i++) {
            if (v[i] < other.v[i])
                menor = true;
            if (v[i] > other.v[i])
                maior = true;
        }
        if (menor && maior)
            return Order.CONCURRENT;
        if (menor)
            return Order.BEFORE;
        if (maior)
            return Order.AFTER;
        return Order.EQUAL;
    }

    // representação "v0,v1,...,vN-1"
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < v.length; i++) {
            if (i > 0)
                sb.append(',');
            sb.append(v[i]);
        }
        return sb.toString();
    }

    public static VectorClock parse(String s, int n) {
        if (s == null || s.trim().isEmpty()) {
            throw new IllegalArgumentException("A string de entrada não pode ser nula nem vazia.");
        }

        String[] f = s.trim().split(",");
        if (f.length != n) {
            throw new IllegalArgumentException("Vetor com dimensão " + f.length + ", esperado " + n + ": " + s);
        }

        long[] v = new long[n];
        for (int i = 0; i < n; i++) {
            v[i] = Long.parseLong(f[i].trim());
            if (v[i] < 0) {
                throw new IllegalArgumentException("Os valores do relógio vetorial não podem ser negativos: " + v[i]);
            }
        }
        return new VectorClock(v);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof VectorClock && Arrays.equals(v, ((VectorClock) o).v);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(v);
    }
}