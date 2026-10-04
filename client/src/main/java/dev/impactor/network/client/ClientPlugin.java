package dev.impactor.network.client;

import dev.impactor.network.protocol.Transport;
import dev.impactor.network.protocol.Wire;
import dev.impactor.network.protocol.Backoff;
import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.command.*;
import org.bukkit.entity.Player;
import org.bukkit.event.*;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scoreboard.*;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public final class ClientPlugin extends JavaPlugin implements CurrencyService, Listener, CommandExecutor {
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("currency-client").factory());
    private final Set<UUID> inFlight = new HashSet<>(); // worker thread only
    private volatile Map<String, String> mappings = Map.of(); // Impactor key -> objective
    private volatile String revision;
    private volatile Instant lastConfig;
    private volatile Instant lastTransaction;
    private volatile String lastError = "";
    private volatile boolean ready;
    private String serverName;
    private Transport bridge;
    private Connection db; // worker thread only
    private int configFailures;
    private long nextConfigAttempt;
    private boolean transportOutage;
    private boolean configInFlight;

    @Override public void onEnable() {
        saveDefaultConfig();
        serverName = getConfig().getString("authoritative-server", "");
        if (!serverName.matches("[a-zA-Z0-9_-]{1,32}")) throw new IllegalStateException("Invalid authoritative-server");
        String url = System.getenv().getOrDefault("IMPACTOR_BRIDGE_URL", "http://127.0.0.1:8096/currency");
        String token = System.getenv("IMPACTOR_PAPER_TOKEN");
        if (token == null || token.length() < 24) throw new IllegalStateException("IMPACTOR_PAPER_TOKEN must have at least 24 characters");
        bridge = new Transport(url, token);
        Bukkit.getPluginManager().registerEvents(this, this);
        Objects.requireNonNull(getCommand("currency")).setExecutor(this);
        worker.execute(() -> {
            try { openDatabase(); refreshConfig(); } catch (Exception e) { lastError = e.toString(); getLogger().severe(lastError); }
        });
        worker.scheduleWithFixedDelay(this::tickSafely, 2, 2, TimeUnit.SECONDS);
    }
    @Override public void onDisable() {
        worker.shutdown();
        try { worker.awaitTermination(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        try { if (db != null) db.close(); } catch (SQLException e) { getLogger().warning(e.toString()); }
    }
    public CurrencyService currencyService() { return this; }

    private void openDatabase() throws Exception {
        Files.createDirectories(getDataFolder().toPath());
        Class.forName("org.sqlite.JDBC");
        db = DriverManager.getConnection("jdbc:sqlite:" + getDataFolder().toPath().resolve("outbox.db"));
        try (Statement sql = db.createStatement()) {
            sql.execute("PRAGMA journal_mode=WAL"); sql.execute("PRAGMA synchronous=FULL");
            sql.execute("CREATE TABLE IF NOT EXISTS pending_transactions (transaction_id TEXT PRIMARY KEY, player_uuid TEXT NOT NULL, currency TEXT NOT NULL, operation TEXT NOT NULL, amount TEXT NOT NULL, expected_balance TEXT, config_revision TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, attempt_count INTEGER NOT NULL, next_attempt_at INTEGER NOT NULL, last_error TEXT, balance TEXT)");
            sql.execute("UPDATE pending_transactions SET status='PENDING',next_attempt_at=0 WHERE status='IN_FLIGHT'");
        }
    }
    private void tickSafely() {
        try { tick(); } catch (Exception e) { lastError = e.toString(); getLogger().warning("Outbox tick: " + e); }
    }
    private void tick() throws Exception {
        if (db == null) return;
        List<UUID> recoverIds = new ArrayList<>();
        try (Statement sql = db.createStatement(); ResultSet rows = sql.executeQuery(
                "SELECT transaction_id FROM pending_transactions WHERE status='IN_FLIGHT'")) {
            while (rows.next()) {
                UUID id = UUID.fromString(rows.getString(1));
                if (!inFlight.contains(id)) recoverIds.add(id);
            }
        }
        for (UUID id : recoverIds) try (PreparedStatement recover = db.prepareStatement(
                "UPDATE pending_transactions SET status='PENDING',next_attempt_at=0 WHERE transaction_id=?")) {
            recover.setString(1, id.toString()); recover.executeUpdate();
        }
        if (!ready) { if (System.currentTimeMillis() >= nextConfigAttempt) refreshConfig(); return; }
        List<Row> due = new ArrayList<>();
        try (PreparedStatement ps = db.prepareStatement("SELECT * FROM pending_transactions WHERE status='PENDING' AND next_attempt_at<=? ORDER BY created_at LIMIT 32")) {
            ps.setLong(1, System.currentTimeMillis());
            try (ResultSet rs = ps.executeQuery()) { while (rs.next()) due.add(read(rs)); }
        }
        for (Row row : due) dispatch(row);
    }
    private void refreshConfig() {
        if (configInFlight) return;
        configInFlight = true;
        ready = false;
        Wire.Message request = Wire.Message.request(Wire.Type.GET_CONFIG, serverName, null, "", null, null, "", null);
        bridge.send(request).whenComplete((reply, error) -> worker.execute(() -> {
            configInFlight = false;
            if (error != null || reply.type() != Wire.Type.CONFIG_RESPONSE || reply.status() != Wire.Status.APPLIED) {
                lastError = error == null ? "Invalid config response" : error.toString();
                nextConfigAttempt = System.currentTimeMillis() + Backoff.delayMillis(++configFailures);
                return;
            }
            try {
                Map<String, String> validated = new HashMap<>(); Set<String> objectives = new HashSet<>();
                if (reply.revision() == null || !reply.revision().matches("[a-f0-9]{64}") || reply.mappings().isEmpty())
                    throw new IllegalArgumentException("Invalid configuration revision or empty mappings");
                for (Wire.Mapping m : reply.mappings()) {
                    if (!objectives.add(m.objective().toLowerCase(Locale.ROOT)) || validated.putIfAbsent(m.impactor(), m.objective()) != null)
                        throw new IllegalArgumentException("Duplicate currency mapping");
                }
                mappings = Map.copyOf(validated); revision = reply.revision(); lastConfig = Instant.now();
                configFailures = 0; ready = true; lastError = "";
                Bukkit.getScheduler().runTask(this, () -> {
                    createObjectives();
                    for (Player player : Bukkit.getOnlinePlayers()) refreshPlayer(player.getUniqueId());
                });
                tickSafely();
            } catch (Exception e) {
                lastError = e.toString(); nextConfigAttempt = System.currentTimeMillis() + Backoff.delayMillis(++configFailures);
            }
        }));
    }
    private void createObjectives() {
        Scoreboard board = Bukkit.getScoreboardManager().getMainScoreboard();
        for (String objective : mappings.values()) if (board.getObjective(objective) == null)
            board.registerNewObjective(objective, Criteria.DUMMY, objective);
    }
    private void dispatch(Row row) throws Exception {
        if (!inFlight.add(row.id)) return;
        try (PreparedStatement ps = db.prepareStatement("UPDATE pending_transactions SET status='IN_FLIGHT',updated_at=? WHERE transaction_id=?")) {
            ps.setString(1, Instant.now().toString()); ps.setString(2, row.id.toString()); ps.executeUpdate();
        }
        Wire.Type type = Wire.Type.valueOf(row.operation);
        Wire.Message request = Wire.Message.request(type, serverName, row.player, row.currency,
                row.amount, row.expected, row.revision, row.id);
        bridge.send(request).whenComplete((reply, error) -> worker.execute(() -> {
            try {
                if (error != null || reply.type() != Wire.Type.MUTATION_RESPONSE || !row.id.equals(reply.transactionId())) {
                    if (error != null) transportOutage = true;
                    retry(row.id, row.attempts + 1, error == null ? "Invalid mutation response" : error.toString());
                } else if (reply.status() == Wire.Status.APPLIED || reply.status() == Wire.Status.ALREADY_APPLIED) {
                    finish(row.id, "COMPLETED", reply.balance(), reply.detail()); lastTransaction = Instant.now();
                    refreshPlayerCurrency(row.player, row.currency);
                } else if (reply.status() == Wire.Status.REJECTED) {
                    finish(row.id, "REJECTED", reply.balance(), reply.detail());
                    if (reply.detail() != null && reply.detail().contains("Configuration revision changed")) refreshConfig();
                } else retry(row.id, row.attempts + 1, reply.status() + ": " + reply.detail());
                if (error == null && transportOutage) { transportOutage = false; refreshConfig(); }
            } catch (Exception e) { lastError = e.toString(); getLogger().warning("Outbox response: " + e); }
            finally { inFlight.remove(row.id); }
        }));
    }
    private void retry(UUID id, int attempts, String error) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("UPDATE pending_transactions SET status='PENDING',updated_at=?,attempt_count=?,next_attempt_at=?,last_error=? WHERE transaction_id=?")) {
            ps.setString(1, Instant.now().toString()); ps.setInt(2, attempts);
            ps.setLong(3, System.currentTimeMillis() + Backoff.delayMillis(attempts)); ps.setString(4, error); ps.setString(5, id.toString()); ps.executeUpdate();
        }
    }
    private void finish(UUID id, String status, BigDecimal balance, String detail) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("UPDATE pending_transactions SET status=?,updated_at=?,balance=?,last_error=? WHERE transaction_id=?")) {
            ps.setString(1, status); ps.setString(2, Instant.now().toString()); ps.setString(3, str(balance));
            ps.setString(4, detail); ps.setString(5, id.toString()); ps.executeUpdate();
        }
    }
    private record Row(UUID id, UUID player, String currency, String operation, BigDecimal amount,
                       BigDecimal expected, String revision, int attempts, String status, BigDecimal balance, String detail) {}
    private static Row read(ResultSet rs) throws SQLException {
        return new Row(UUID.fromString(rs.getString("transaction_id")), UUID.fromString(rs.getString("player_uuid")),
                rs.getString("currency"), rs.getString("operation"), new BigDecimal(rs.getString("amount")),
                decimal(rs.getString("expected_balance")), rs.getString("config_revision"), rs.getInt("attempt_count"),
                rs.getString("status"), decimal(rs.getString("balance")), rs.getString("last_error"));
    }
    private static BigDecimal decimal(String s) { return s == null || s.isEmpty() ? null : new BigDecimal(s); }
    private static String str(Object x) { return x == null ? "" : x.toString(); }

    @Override public CompletableFuture<BalanceResult> getBalance(UUID player, String currency) {
        if (!ready || !mappings.containsKey(currency)) return CompletableFuture.completedFuture(new BalanceResult(State.UNAVAILABLE, null, "Currency configuration unavailable"));
        Wire.Message request = Wire.Message.request(Wire.Type.GET_BALANCE, serverName, player, currency, null, null, revision, null);
        return bridge.send(request).handle((reply, error) -> {
            if (error != null) return new BalanceResult(State.UNAVAILABLE, null, error.toString());
            if (reply.type() != Wire.Type.BALANCE_RESPONSE || reply.status() != Wire.Status.APPLIED)
                return new BalanceResult(State.REJECTED, null, reply.detail());
            project(player, currency, reply.balance());
            return new BalanceResult(State.APPLIED, reply.balance(), "");
        });
    }
    @Override public CompletableFuture<MutationResult> addBalance(UUID p, String c, BigDecimal a) { return enqueue(Wire.Type.ADD_BALANCE, p, c, a, null); }
    @Override public CompletableFuture<MutationResult> removeBalance(UUID p, String c, BigDecimal a) { return enqueue(Wire.Type.REMOVE_BALANCE, p, c, a, null); }
    @Override public CompletableFuture<MutationResult> setBalance(UUID p, String c, BigDecimal a, BigDecimal expected) { return enqueue(Wire.Type.SET_BALANCE, p, c, a, expected); }
    private CompletableFuture<MutationResult> enqueue(Wire.Type type, UUID player, String currency, BigDecimal amount, BigDecimal expected) {
        if (!ready || !mappings.containsKey(currency)) return CompletableFuture.completedFuture(new MutationResult(null, State.UNAVAILABLE, null, "Currency configuration unavailable"));
        UUID id = UUID.randomUUID(); String capturedRevision = revision;
        try { Wire.validate(Wire.Message.request(type, serverName, player, currency, amount, expected, capturedRevision, id)); }
        catch (Exception e) { return CompletableFuture.completedFuture(new MutationResult(id, State.REJECTED, null, e.getMessage())); }
        CompletableFuture<MutationResult> future = new CompletableFuture<>();
        worker.execute(() -> {
            try (PreparedStatement ps = db.prepareStatement("INSERT INTO pending_transactions(transaction_id,player_uuid,currency,operation,amount,expected_balance,config_revision,status,created_at,updated_at,attempt_count,next_attempt_at,last_error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                String now = Instant.now().toString();
                ps.setString(1, id.toString()); ps.setString(2, player.toString()); ps.setString(3, currency);
                ps.setString(4, type.name()); ps.setString(5, amount.toString()); ps.setString(6, str(expected));
                ps.setString(7, capturedRevision); ps.setString(8, "PENDING"); ps.setString(9, now); ps.setString(10, now);
                ps.setInt(11, 0); ps.setLong(12, 0); ps.setString(13, ""); ps.executeUpdate();
                future.complete(new MutationResult(id, State.PENDING, null, "Durably queued")); tickSafely();
            } catch (Exception e) { future.complete(new MutationResult(id, State.PERMANENTLY_FAILED, null, "Outbox write failed: " + e)); }
        });
        return future;
    }
    @Override public CompletableFuture<MutationResult> transaction(UUID id) {
        CompletableFuture<MutationResult> future = new CompletableFuture<>();
        worker.execute(() -> {
            try (PreparedStatement ps = db.prepareStatement("SELECT * FROM pending_transactions WHERE transaction_id=?")) {
                ps.setString(1, id.toString());
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) future.complete(new MutationResult(id, State.REJECTED, null, "Unknown transaction"));
                    else { Row row = read(rs); State state = switch (row.status) { case "COMPLETED" -> State.APPLIED; case "REJECTED" -> State.REJECTED; default -> State.PENDING; };
                        future.complete(new MutationResult(id, state, row.balance, row.detail)); }
                }
            } catch (Exception e) { future.complete(new MutationResult(id, State.UNAVAILABLE, null, e.toString())); }
        }); return future;
    }
    private void project(UUID player, String currency, BigDecimal balance) {
        if (balance == null) return;
        Bukkit.getScheduler().runTask(this, () -> {
            Player online = Bukkit.getPlayer(player); if (online == null) return;
            String name = mappings.get(currency); if (name == null) return;
            Objective objective = Bukkit.getScoreboardManager().getMainScoreboard().getObjective(name); if (objective == null) return;
            try { objective.getScore(online.getName()).setScore(balance.intValueExact()); }
            catch (ArithmeticException e) { getLogger().warning("Cannot project non-integer/out-of-range balance for " + currency); }
        });
    }
    private void refreshPlayerCurrency(UUID player, String currency) { getBalance(player, currency); }
    private void refreshPlayer(UUID player) { for (String currency : mappings.keySet()) getBalance(player, currency); }
    @EventHandler public void onJoin(PlayerJoinEvent event) { if (ready) refreshPlayer(event.getPlayer().getUniqueId()); }

    @Override public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (args.length == 0 || args[0].equalsIgnoreCase("status")) {
            worker.execute(() -> {
                try (Statement sql = db.createStatement(); ResultSet rs = sql.executeQuery("SELECT COUNT(*), MIN(created_at) FROM pending_transactions WHERE status IN ('PENDING','IN_FLIGHT')")) {
                    rs.next();
                    String message = "Fabric=" + (ready ? "ready" : "unavailable") + " revision=" + revision
                            + " pending=" + rs.getInt(1) + " oldest=" + rs.getString(2)
                            + " lastConfig=" + lastConfig + " lastTransaction=" + lastTransaction + " error=" + lastError;
                    tell(sender, message);
                } catch (Exception e) { tell(sender, e.toString()); }
            }); return true;
        }
        if (args[0].equalsIgnoreCase("config")) { worker.execute(this::refreshConfig); tell(sender, "Configuration refresh requested"); return true; }
        if (args[0].equalsIgnoreCase("pending")) {
            worker.execute(() -> {
                try (Statement sql = db.createStatement(); ResultSet rs = sql.executeQuery("SELECT transaction_id,status,attempt_count,last_error FROM pending_transactions WHERE status IN ('PENDING','IN_FLIGHT','REJECTED') ORDER BY created_at DESC LIMIT 10")) {
                    while (rs.next()) tell(sender, rs.getString(1) + " " + rs.getString(2) + " attempts=" + rs.getInt(3) + " " + rs.getString(4));
                } catch (Exception e) { tell(sender, e.toString()); }
            }); return true;
        }
        if (args.length == 2 && args[0].equalsIgnoreCase("transaction")) {
            try { transaction(UUID.fromString(args[1])).thenAccept(result -> tell(sender, result.toString())); }
            catch (IllegalArgumentException e) { tell(sender, "Invalid transaction UUID"); } return true;
        }
        if (args.length == 2 && args[0].equalsIgnoreCase("retry")) {
            try { UUID id = UUID.fromString(args[1]); worker.execute(() -> {
                try (PreparedStatement ps = db.prepareStatement("UPDATE pending_transactions SET next_attempt_at=0 WHERE transaction_id=? AND status='PENDING'")) {
                    ps.setString(1, id.toString()); tell(sender, ps.executeUpdate() == 1 ? "Retry scheduled" : "Transaction not pending"); tickSafely();
                } catch (Exception e) { tell(sender, e.toString()); }
            }); } catch (IllegalArgumentException e) { tell(sender, "Invalid transaction UUID"); } return true;
        }
        if (args.length == 2 && args[0].equalsIgnoreCase("refresh")) {
            OfflinePlayer player = Bukkit.getOfflinePlayer(args[1]); refreshPlayer(player.getUniqueId()); tell(sender, "Balance refresh requested"); return true;
        }
        tell(sender, "Usage: /currency status|config|pending|transaction <id>|retry <id>|refresh <player>"); return true;
    }
    private void tell(CommandSender sender, String message) { Bukkit.getScheduler().runTask(this, () -> sender.sendMessage(message)); }
}
