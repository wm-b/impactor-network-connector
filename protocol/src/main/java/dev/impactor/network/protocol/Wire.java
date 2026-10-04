package dev.impactor.network.protocol;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Bounded, versioned server-to-server frame. */
public final class Wire {
    public static final int VERSION = 1, MAX_BYTES = 16384;
    public enum Type { GET_CONFIG, CONFIG_RESPONSE, GET_BALANCE, BALANCE_RESPONSE,
        ADD_BALANCE, REMOVE_BALANCE, SET_BALANCE, MUTATION_RESPONSE }
    public enum Status { NONE, APPLIED, ALREADY_APPLIED, REJECTED, UNAVAILABLE, NEEDS_RECONCILIATION }
    public record Mapping(String objective, String impactor) {}
    public record Message(Type type, UUID requestId, UUID transactionId, String server,
                          UUID player, String currency, BigDecimal amount, BigDecimal expectedBalance,
                          String revision, Status status, BigDecimal balance, String detail,
                          List<Mapping> mappings) {
        public Message {
            Objects.requireNonNull(type); Objects.requireNonNull(requestId); Objects.requireNonNull(status);
            mappings = mappings == null ? List.of() : List.copyOf(mappings);
        }
        public boolean mutation() { return type == Type.ADD_BALANCE || type == Type.REMOVE_BALANCE || type == Type.SET_BALANCE; }
        public static Message request(Type type, String server, UUID player, String currency,
                                      BigDecimal amount, BigDecimal expected, String revision, UUID tx) {
            return new Message(type, UUID.randomUUID(), tx, server, player, currency, amount,
                    expected, revision, Status.NONE, null, "", List.of());
        }
        public Message reply(Type type, Status status, BigDecimal balance, String detail, String revision, List<Mapping> mappings) {
            return new Message(type, requestId, transactionId, server, player, currency,
                    null, null, revision, status, balance, detail, mappings);
        }
    }
    private Wire() {}

    public static byte[] encode(Message m) throws IOException {
        validate(m);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(bytes);
        out.writeInt(VERSION); out.writeByte(m.type.ordinal());
        for (String s : List.of(m.requestId.toString(), str(m.transactionId), str(m.server), str(m.player),
                str(m.currency), str(m.amount), str(m.expectedBalance), str(m.revision))) write(out, s);
        out.writeByte(m.status.ordinal()); write(out, str(m.balance)); write(out, str(m.detail));
        if (m.mappings.size() > 32) throw new IOException("Too many currencies");
        out.writeByte(m.mappings.size());
        for (Mapping mapping : m.mappings) { write(out, mapping.objective); write(out, mapping.impactor); }
        out.flush(); if (bytes.size() > MAX_BYTES) throw new IOException("Frame too large"); return bytes.toByteArray();
    }
    public static Message decode(byte[] bytes) throws IOException {
        if (bytes.length > MAX_BYTES || bytes.length < 7) throw new IOException("Invalid frame size");
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        if (in.readInt() != VERSION) throw new IOException("Unsupported protocol version");
        Type type = at(Type.values(), in.readUnsignedByte()); UUID id = uuid(read(in)); UUID tx = optionalUuid(read(in));
        String server = read(in); UUID player = optionalUuid(read(in)); String currency = read(in);
        BigDecimal amount = decimal(read(in)), expected = decimal(read(in)); String revision = read(in);
        Status status = at(Status.values(), in.readUnsignedByte()); BigDecimal balance = decimal(read(in)); String detail = read(in);
        int count = in.readUnsignedByte(); if (count > 32) throw new IOException("Too many currencies");
        List<Mapping> mappings = new ArrayList<>();
        for (int i = 0; i < count; i++) mappings.add(new Mapping(read(in), read(in)));
        if (in.available() != 0) throw new IOException("Trailing bytes");
        Message m = new Message(type, id, tx, server, player, currency, amount, expected, revision, status, balance, detail, mappings);
        validate(m); return m;
    }
    public static void validate(Message m) throws IOException {
        if (m.server == null || !m.server.matches("[a-zA-Z0-9_-]{1,32}")) throw new IOException("Invalid server");
        if (m.currency != null && !m.currency.isEmpty() && !m.currency.matches("[a-z0-9_.-]+:[a-z0-9_./-]+")) throw new IOException("Invalid currency");
        if (m.revision != null && m.revision.length() > 128) throw new IOException("Invalid revision");
        if (m.detail != null && m.detail.length() > 1024) throw new IOException("Invalid detail");
        if (m.mutation()) {
            if (m.transactionId == null || m.player == null || m.currency == null || m.currency.isEmpty()
                    || m.amount == null || m.revision == null || m.revision.isEmpty()) throw new IOException("Incomplete mutation");
            if (m.type == Type.SET_BALANCE && m.expectedBalance == null) throw new IOException("Set needs expected balance");
            if (m.expectedBalance != null && (m.expectedBalance.signum() < 0
                    || m.expectedBalance.abs().compareTo(new BigDecimal("1000000000000")) > 0
                    || m.expectedBalance.scale() > 4)) throw new IOException("Expected balance out of bounds");
            if (m.type != Type.SET_BALANCE && m.amount.signum() <= 0) throw new IOException("Amount must be positive");
            if (m.amount.signum() < 0 || m.amount.abs().compareTo(new BigDecimal("1000000000000")) > 0
                    || m.amount.scale() > 4) throw new IOException("Amount out of bounds");
        }
        if (m.type == Type.GET_BALANCE && (m.player == null || m.currency == null || m.currency.isEmpty())) throw new IOException("Incomplete balance request");
        for (Mapping mapping : m.mappings) if (!mapping.objective.matches("[A-Za-z][A-Za-z0-9_]{0,15}")
                || !mapping.impactor.matches("[a-z0-9_.-]+:[a-z0-9_./-]+")) throw new IOException("Invalid mapping");
    }
    private static void write(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8); if (b.length > 2048) throw new IOException("Field too long");
        out.writeShort(b.length); out.write(b);
    }
    private static String read(DataInputStream in) throws IOException {
        int n = in.readUnsignedShort(); if (n > 2048) throw new IOException("Field too long");
        byte[] b = in.readNBytes(n); if (b.length != n) throw new EOFException(); return new String(b, StandardCharsets.UTF_8);
    }
    private static String str(Object o) { return o == null ? "" : o.toString(); }
    private static BigDecimal decimal(String s) throws IOException { try { return s.isEmpty() ? null : new BigDecimal(s); } catch (NumberFormatException e) { throw new IOException("Invalid decimal", e); } }
    private static UUID uuid(String s) throws IOException { try { return UUID.fromString(s); } catch (IllegalArgumentException e) { throw new IOException("Invalid UUID", e); } }
    private static UUID optionalUuid(String s) throws IOException { return s.isEmpty() ? null : uuid(s); }
    private static <T> T at(T[] values, int i) throws IOException { if (i >= values.length) throw new IOException("Unknown enum value"); return values[i]; }
}
