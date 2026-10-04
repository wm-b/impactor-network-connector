package dev.impactor.network.protocol;

import java.util.concurrent.ThreadLocalRandom;

public final class Backoff {
    private Backoff() {}
    public static long delayMillis(int attempt) {
        long cap = Math.min(300_000L, 2_000L << Math.min(8, Math.max(0, attempt - 1)));
        return cap / 2 + ThreadLocalRandom.current().nextLong(cap / 2 + 1);
    }
}
