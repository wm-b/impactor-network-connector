package dev.impactor.network.client;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** Local Paper API. A queued result does not mean Impactor has changed. */
public interface CurrencyService {
    enum State { PENDING, APPLIED, ALREADY_APPLIED, REJECTED, UNAVAILABLE, PERMANENTLY_FAILED }
    record BalanceResult(State state, BigDecimal balance, String detail) {}
    record MutationResult(UUID transactionId, State state, BigDecimal balance, String detail) {}
    CompletableFuture<BalanceResult> getBalance(UUID player, String currency);
    CompletableFuture<MutationResult> addBalance(UUID player, String currency, BigDecimal amount);
    CompletableFuture<MutationResult> removeBalance(UUID player, String currency, BigDecimal amount);
    CompletableFuture<MutationResult> setBalance(UUID player, String currency, BigDecimal amount, BigDecimal expectedBalance);
    CompletableFuture<MutationResult> transaction(UUID transactionId);
}
