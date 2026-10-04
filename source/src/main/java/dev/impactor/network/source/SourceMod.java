package dev.impactor.network.source;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.mojang.brigadier.arguments.StringArgumentType;
import dev.impactor.network.protocol.Wire;
import net.fabricmc.api.ModInitializer;
import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.fabricmc.fabric.api.event.lifecycle.v1.ServerLifecycleEvents;
import net.fabricmc.loader.api.FabricLoader;
import net.minecraft.server.command.CommandManager;
import net.minecraft.server.command.ServerCommandSource;
import net.minecraft.text.Text;
import net.impactdev.impactor.api.economy.EconomyService;
import net.impactdev.impactor.api.economy.accounts.Account;
import net.impactdev.impactor.api.economy.currency.Currency;
import net.impactdev.impactor.api.economy.transactions.EconomyTransaction;
import net.impactdev.impactor.api.economy.transactions.details.EconomyResultType;
import net.kyori.adventure.key.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public final class SourceMod implements ModInitializer {
    private static final Logger LOG = LoggerFactory.getLogger("impactor-network-source");
    private final ExecutorService worker = Executors.newSingleThreadExecutor(Thread.ofVirtual().name("currency-source").factory());
    private HttpServer http;
    private Connection db;
    private EconomyService economy;
    private List<Wire.Mapping> mappings;
    private String revision, serverName, token;

    @Override public void onInitialize() {
        CommandRegistrationCallback.EVENT.register((dispatcher, registry, environment) -> dispatcher.register(
                CommandManager.literal("impactorcurrency").requires(source -> source.hasPermissionLevel(4))
                        .then(CommandManager.literal("status").executes(context -> {
                            worker.execute(() -> {
                                try (Statement statement = db.createStatement(); ResultSet rs = statement.executeQuery(
                                        "SELECT COUNT(*) FROM transactions WHERE status='NEEDS_RECONCILIATION'")) {
                                    rs.next();
                                    int count = rs.getInt(1);
                                    context.getSource().getServer().execute(() -> context.getSource().sendFeedback(
                                            () -> Text.literal("Currency transactions requiring reconciliation: " + count), false));
                                } catch (Exception e) { LOG.error("Could not read ledger status", e); }
                            }); return 1;
                        }))
                        .then(CommandManager.literal("reconcile")
                                .then(CommandManager.argument("transaction", StringArgumentType.word())
                                        .then(CommandManager.literal("applied").executes(ctx -> reconcileCommand(ctx.getSource(),
                                                StringArgumentType.getString(ctx, "transaction"), true)))
                                        .then(CommandManager.literal("rejected").executes(ctx -> reconcileCommand(ctx.getSource(),
                                                StringArgumentType.getString(ctx, "transaction"), false)))))));
        ServerLifecycleEvents.SERVER_STARTED.register(server -> {
            try { start(); } catch (Exception e) { throw new IllegalStateException("Currency source failed to start", e); }
        });
        ServerLifecycleEvents.SERVER_STOPPING.register(server -> stop());
    }

    private int reconcileCommand(ServerCommandSource source, String text, boolean applied) {
        worker.execute(() -> {
            try {
                UUID id = UUID.fromString(text);
                UUID player;
                String currencyKey;
                try (PreparedStatement ps = db.prepareStatement("SELECT player_uuid,currency FROM transactions WHERE transaction_id=? AND status='NEEDS_RECONCILIATION'")) {
                    ps.setString(1, id.toString());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (!rs.next()) { feedback(source, "No ambiguous transaction with ID " + id); return; }
                        player = UUID.fromString(rs.getString(1)); currencyKey = rs.getString(2);
                    }
                }
                Currency currency = economy.currencies().currency(Key.key(currencyKey)).orElseThrow();
                BigDecimal current = economy.account(currency, player).join().balance();
                finish(id, applied ? "APPLIED" : "REJECTED", current, "Operator reconciled after external verification");
                audit(id, "RECONCILED", applied ? "marked applied" : "marked rejected");
                LOG.warn("Operator reconciled transaction {} as {}; current Impactor balance {}", id, applied ? "applied" : "rejected", current);
                feedback(source, "Reconciled " + id + " as " + (applied ? "applied" : "rejected") + "; current balance " + current);
            } catch (Exception e) { LOG.error("Reconciliation failed", e); feedback(source, "Reconciliation failed: " + e.getMessage()); }
        });
        return 1;
    }
    private static void feedback(ServerCommandSource source, String message) {
        source.getServer().execute(() -> source.sendFeedback(() -> Text.literal(message), false));
    }

    private void start() throws Exception {
        economy = EconomyService.instance();
        serverName = System.getenv().getOrDefault("IMPACTOR_FABRIC_SERVER", "fabric");
        token = required("IMPACTOR_FABRIC_TOKEN");
        Path dir = FabricLoader.getInstance().getConfigDir();
        Path config = dir.resolve("impactor-network-source.properties");
        if (Files.notExists(config)) {
            try (var in = getClass().getResourceAsStream("/impactor-currencies.properties")) { Files.copy(in, config); }
        }
        loadMappings(config);
        Class.forName("org.sqlite.JDBC");
        db = DriverManager.getConnection("jdbc:sqlite:" + dir.resolve("impactor-network-source.db"));
        try (Statement sql = db.createStatement()) {
            sql.execute("PRAGMA journal_mode=WAL"); sql.execute("PRAGMA synchronous=FULL");
            sql.execute("CREATE TABLE IF NOT EXISTS transactions (transaction_id TEXT PRIMARY KEY, player_uuid TEXT NOT NULL, currency TEXT NOT NULL, operation TEXT NOT NULL, amount TEXT NOT NULL, expected_balance TEXT, config_revision TEXT NOT NULL, status TEXT NOT NULL, before_balance TEXT, balance TEXT, detail TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)");
            sql.execute("CREATE TABLE IF NOT EXISTS transaction_audit (audit_id INTEGER PRIMARY KEY AUTOINCREMENT, transaction_id TEXT NOT NULL, event_type TEXT NOT NULL, player_uuid TEXT, currency TEXT, operation TEXT, amount TEXT, timestamp TEXT NOT NULL, source TEXT NOT NULL DEFAULT 'paper', detail TEXT NOT NULL)");
            boolean hasBefore = false;
            try (ResultSet columns = sql.executeQuery("PRAGMA table_info(transactions)")) {
                while (columns.next()) if (columns.getString("name").equals("before_balance")) hasBefore = true;
            }
            if (!hasBefore) sql.execute("ALTER TABLE transactions ADD COLUMN before_balance TEXT");
            Set<String> auditColumns = new HashSet<>();
            try (ResultSet columns = sql.executeQuery("PRAGMA table_info(transaction_audit)")) {
                while (columns.next()) auditColumns.add(columns.getString("name"));
            }
            for (String name : List.of("player_uuid", "currency", "operation", "amount"))
                if (!auditColumns.contains(name)) sql.execute("ALTER TABLE transaction_audit ADD COLUMN " + name + " TEXT");
            if (!auditColumns.contains("source")) sql.execute("ALTER TABLE transaction_audit ADD COLUMN source TEXT NOT NULL DEFAULT 'paper'");
            sql.execute("UPDATE transactions SET status='NEEDS_RECONCILIATION', updated_at=datetime('now') WHERE status='APPLYING'");
        }
        String[] bind = System.getenv().getOrDefault("IMPACTOR_SOURCE_BIND", "127.0.0.1:8097").split(":", 2);
        http = HttpServer.create(new InetSocketAddress(bind[0], Integer.parseInt(bind[1])), 32);
        http.createContext("/currency", this::handle);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor()); http.start();
        LOG.info("Currency source listening on {} with revision {}", http.getAddress(), revision);
    }

    private void loadMappings(Path config) throws Exception {
        Properties properties = new Properties();
        try (var in = Files.newInputStream(config)) { properties.load(in); }
        String raw = properties.getProperty("currencies", "");
        List<Wire.Mapping> result = new ArrayList<>(); Set<String> objectives = new HashSet<>(), currencies = new HashSet<>();
        if (raw.isBlank()) throw new IllegalArgumentException("No exposed currencies configured");
        for (String item : raw.split(",", -1)) {
            String[] parts = item.trim().split("\\|", -1);
            if (parts.length != 2) throw new IllegalArgumentException("Use objective|namespace:currency");
            Wire.Mapping mapping = new Wire.Mapping(parts[0].trim(), parts[1].trim());
            if (!mapping.objective().matches("[A-Za-z][A-Za-z0-9_]{0,15}")
                    || !mapping.impactor().matches("[a-z0-9_.-]+:[a-z0-9_./-]+")
                    || !objectives.add(mapping.objective().toLowerCase(Locale.ROOT)) || !currencies.add(mapping.impactor()))
                throw new IllegalArgumentException("Invalid or duplicate currency mapping: " + item);
            if (economy.currencies().currency(Key.key(mapping.impactor())).isEmpty())
                throw new IllegalArgumentException("Unknown Impactor currency: " + mapping.impactor());
            result.add(mapping);
        }
        if (result.size() > 32) throw new IllegalArgumentException("Too many currencies");
        mappings = List.copyOf(result);
        byte[] hash = MessageDigest.getInstance("SHA-256").digest(String.join(",", result.stream()
                .map(m -> m.objective() + "|" + m.impactor()).toList()).getBytes(StandardCharsets.UTF_8));
        revision = HexFormat.of().formatHex(hash);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"POST".equals(exchange.getRequestMethod()) || !same(token, exchange.getRequestHeaders().getFirst("X-Impactor-Token"))) {
                respond(exchange, 403, new byte[0]); return;
            }
            byte[] bytes = exchange.getRequestBody().readNBytes(Wire.MAX_BYTES + 1);
            Wire.Message request = Wire.decode(bytes);
            if (!serverName.equals(request.server()) || request.status() != Wire.Status.NONE) { respond(exchange, 400, new byte[0]); return; }
            Wire.Message result = worker.submit(() -> process(request)).get(8, TimeUnit.SECONDS);
            respond(exchange, 200, Wire.encode(result));
        } catch (Exception e) {
            LOG.warn("Currency request failed: {}", e.toString());
            try { if (exchange.getResponseCode() == -1) respond(exchange, 503, new byte[0]); } catch (Exception ignored) {}
        }
    }
    private Wire.Message process(Wire.Message m) throws Exception {
        if (m.type() == Wire.Type.GET_CONFIG)
            return m.reply(Wire.Type.CONFIG_RESPONSE, Wire.Status.APPLIED, null, "", revision, mappings);
        Currency currency = mappings.stream().filter(x -> x.impactor().equals(m.currency())).findAny()
                .flatMap(x -> economy.currencies().currency(Key.key(x.impactor()))).orElse(null);
        if (m.type() == Wire.Type.GET_BALANCE) {
            if (currency == null) return m.reply(Wire.Type.BALANCE_RESPONSE,
                    Wire.Status.REJECTED, null, "Unknown currency", revision, List.of());
            BigDecimal balance = economy.account(currency, m.player()).join().balance();
            return m.reply(Wire.Type.BALANCE_RESPONSE, Wire.Status.APPLIED, balance, "", revision, List.of());
        }
        if (!m.mutation()) throw new IOException("Unexpected request type");
        return mutate(m, currency);
    }
    private Wire.Message mutate(Wire.Message m, Currency currency) throws Exception {
        boolean receivedEarlier = false;
        Wire.Message duplicateResponse = null;
        try (PreparedStatement find = db.prepareStatement("SELECT * FROM transactions WHERE transaction_id=?")) {
            find.setString(1, m.transactionId().toString());
            try (ResultSet row = find.executeQuery()) {
                if (row.next()) {
                    if (!row.getString("player_uuid").equals(m.player().toString()) || !row.getString("currency").equals(m.currency())
                            || !row.getString("operation").equals(m.type().name()) || !new BigDecimal(row.getString("amount")).equals(m.amount())
                            || !Objects.equals(row.getString("expected_balance"), str(m.expectedBalance()))
                            || !row.getString("config_revision").equals(m.revision()))
                        return m.reply(Wire.Type.MUTATION_RESPONSE, Wire.Status.REJECTED, null, "Transaction ID payload mismatch", revision, List.of());
                    String status = row.getString("status");
                    if (status.equals("RECEIVED")) receivedEarlier = true;
                    else duplicateResponse = m.reply(Wire.Type.MUTATION_RESPONSE, switch (status) {
                        case "APPLIED" -> Wire.Status.ALREADY_APPLIED;
                        case "REJECTED" -> Wire.Status.REJECTED;
                        default -> Wire.Status.NEEDS_RECONCILIATION;
                    }, decimal(row.getString("balance")), row.getString("detail"), revision, List.of());
                }
            }
        }
        if (receivedEarlier || duplicateResponse != null) audit(m.transactionId(), "DUPLICATE_REQUEST", "");
        if (duplicateResponse != null) return duplicateResponse;
        if (currency == null) return m.reply(Wire.Type.MUTATION_RESPONSE, Wire.Status.REJECTED,
                null, "Unknown currency", revision, List.of());
        if (!revision.equals(m.revision()))
            return m.reply(Wire.Type.MUTATION_RESPONSE, Wire.Status.REJECTED, null, "Configuration revision changed", revision, List.of());
        try (PreparedStatement blocked = db.prepareStatement("SELECT 1 FROM transactions WHERE player_uuid=? AND currency=? AND status IN ('APPLYING','NEEDS_RECONCILIATION') LIMIT 1")) {
            blocked.setString(1, m.player().toString()); blocked.setString(2, m.currency());
            try (ResultSet rows = blocked.executeQuery()) {
                if (rows.next()) return m.reply(Wire.Type.MUTATION_RESPONSE,
                        Wire.Status.NEEDS_RECONCILIATION, null, "Account has ambiguous transaction", revision, List.of());
            }
        }
        if (!receivedEarlier) { insert(m); audit(m.transactionId(), "RECEIVED", ""); }
        Account account = economy.account(currency, m.player()).join();
        BigDecimal before = account.balance();
        if (m.type() == Wire.Type.SET_BALANCE && before.compareTo(m.expectedBalance()) != 0) {
            finish(m.transactionId(), "REJECTED", before, "Expected balance mismatch");
            return m.reply(Wire.Type.MUTATION_RESPONSE, Wire.Status.REJECTED, before, "Expected balance mismatch", revision, List.of());
        }
        applying(m.transactionId(), before); audit(m.transactionId(), "APPLYING", "before=" + before);
        try {
            EconomyTransaction result = switch (m.type()) {
                case ADD_BALANCE -> account.deposit(m.amount());
                case REMOVE_BALANCE -> account.withdraw(m.amount());
                case SET_BALANCE -> account.set(m.amount());
                default -> throw new IllegalStateException();
            };
            if (result.result() == EconomyResultType.FAILED) {
                transition(m.transactionId(), "NEEDS_RECONCILIATION", "Impactor reported FAILED after uncertain processing");
                audit(m.transactionId(), "RECONCILIATION_REQUIRED", "Impactor reported FAILED");
                return m.reply(Wire.Type.MUTATION_RESPONSE, Wire.Status.NEEDS_RECONCILIATION, null,
                        "Impactor outcome unknown", revision, List.of());
            }
            if (result.result() != EconomyResultType.SUCCESS) {
                finish(m.transactionId(), "REJECTED", account.balance(), result.result().name());
                return m.reply(Wire.Type.MUTATION_RESPONSE, Wire.Status.REJECTED, account.balance(), result.result().name(), revision, List.of());
            }
            // Impactor's Account methods schedule their own asynchronous save; await an explicit save before acknowledging.
            economy.save(account).join();
            finish(m.transactionId(), "APPLIED", account.balance(), "");
            return m.reply(Wire.Type.MUTATION_RESPONSE, Wire.Status.APPLIED, account.balance(), "", revision, List.of());
        } catch (Exception e) {
            transition(m.transactionId(), "NEEDS_RECONCILIATION", e.toString());
            audit(m.transactionId(), "RECONCILIATION_REQUIRED", e.toString());
            return m.reply(Wire.Type.MUTATION_RESPONSE, Wire.Status.NEEDS_RECONCILIATION, null, "Impactor outcome unknown", revision, List.of());
        }
    }
    private void insert(Wire.Message m) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("INSERT INTO transactions(transaction_id,player_uuid,currency,operation,amount,expected_balance,config_revision,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)")) {
            ps.setString(1, m.transactionId().toString()); ps.setString(2, m.player().toString()); ps.setString(3, m.currency());
            ps.setString(4, m.type().name()); ps.setString(5, m.amount().toString()); ps.setString(6, str(m.expectedBalance()));
            ps.setString(7, m.revision()); ps.setString(8, "RECEIVED");
            ps.setString(9, Instant.now().toString()); ps.setString(10, Instant.now().toString()); ps.executeUpdate();
        }
    }
    private void transition(UUID id, String status, String detail) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("UPDATE transactions SET status=?,detail=?,updated_at=? WHERE transaction_id=?")) {
            ps.setString(1, status); ps.setString(2, detail); ps.setString(3, Instant.now().toString()); ps.setString(4, id.toString()); ps.executeUpdate();
        }
    }
    private void applying(UUID id, BigDecimal before) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("UPDATE transactions SET status='APPLYING',before_balance=?,updated_at=? WHERE transaction_id=?")) {
            ps.setString(1, before.toString()); ps.setString(2, Instant.now().toString());
            ps.setString(3, id.toString()); ps.executeUpdate();
        }
    }
    private void finish(UUID id, String status, BigDecimal balance, String detail) throws SQLException {
        try (PreparedStatement ps = db.prepareStatement("UPDATE transactions SET status=?,balance=?,detail=?,updated_at=? WHERE transaction_id=?")) {
            ps.setString(1, status); ps.setString(2, str(balance)); ps.setString(3, detail);
            ps.setString(4, Instant.now().toString()); ps.setString(5, id.toString()); ps.executeUpdate();
        }
        audit(id, status, detail);
    }
    private void audit(UUID id, String event, String detail) throws SQLException {
        String player, currency, operation, amount;
        try (PreparedStatement lookup = db.prepareStatement("SELECT player_uuid,currency,operation,amount FROM transactions WHERE transaction_id=?")) {
            lookup.setString(1, id.toString());
            try (ResultSet row = lookup.executeQuery()) {
                if (!row.next()) throw new SQLException("Missing audited transaction " + id);
                player = row.getString(1); currency = row.getString(2); operation = row.getString(3); amount = row.getString(4);
            }
        }
        try (PreparedStatement ps = db.prepareStatement("INSERT INTO transaction_audit(transaction_id,event_type,player_uuid,currency,operation,amount,timestamp,source,detail) VALUES(?,?,?,?,?,?,?,?,?)")) {
            ps.setString(1, id.toString()); ps.setString(2, event); ps.setString(3, player); ps.setString(4, currency);
            ps.setString(5, operation); ps.setString(6, amount); ps.setString(7, Instant.now().toString());
            ps.setString(8, "paper"); ps.setString(9, detail == null ? "" : detail); ps.executeUpdate();
        }
    }
    private void stop() {
        if (http != null) http.stop(0);
        worker.shutdown();
        try { worker.awaitTermination(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        try { if (db != null) db.close(); } catch (SQLException e) { LOG.error("Could not close ledger", e); }
    }
    private static BigDecimal decimal(String s) { return s == null || s.isEmpty() ? null : new BigDecimal(s); }
    private static String str(Object o) { return o == null ? "" : o.toString(); }
    private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException { exchange.sendResponseHeaders(status, body.length); exchange.getResponseBody().write(body); }
    private static boolean same(String expected, String actual) { return actual != null && MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), actual.getBytes(StandardCharsets.UTF_8)); }
    private static String required(String name) { String v = System.getenv(name); if (v == null || v.length() < 24) throw new IllegalStateException(name + " must have at least 24 characters"); return v; }
}
