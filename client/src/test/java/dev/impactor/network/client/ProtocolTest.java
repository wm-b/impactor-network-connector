package dev.impactor.network.client;

import dev.impactor.network.protocol.Backoff;
import dev.impactor.network.protocol.Transport;
import dev.impactor.network.protocol.Wire;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolTest {
    @Test void roundTripsMutationWithoutChangingItsId() throws Exception {
        UUID id = UUID.randomUUID(); UUID player = UUID.randomUUID();
        Wire.Message original = Wire.Message.request(Wire.Type.SET_BALANCE, "fabric", player,
                "impactor:event_points", new BigDecimal("100"), new BigDecimal("95"), "revision", id);
        Wire.Message decoded = Wire.decode(Wire.encode(original));
        assertEquals(original, decoded);
        assertEquals(id, decoded.transactionId());
        assertEquals(id, Wire.decode(Wire.encode(decoded)).transactionId());
    }

    @Test void rejectsMalformedAndUnsupportedFrames() throws Exception {
        Wire.Message missingExpected = Wire.Message.request(Wire.Type.SET_BALANCE, "fabric", UUID.randomUUID(),
                "impactor:event_points", BigDecimal.TEN, null, "revision", UUID.randomUUID());
        assertThrows(IOException.class, () -> Wire.encode(missingExpected));
        Wire.Message config = Wire.Message.request(Wire.Type.GET_CONFIG, "fabric", null, "", null, null, "", null);
        byte[] version = Wire.encode(config); version[3] = 2;
        assertThrows(IOException.class, () -> Wire.decode(version));
        assertThrows(IOException.class, () -> Wire.decode(new byte[Wire.MAX_BYTES + 1]));
        assertThrows(IOException.class, () -> Wire.encode(config.reply(Wire.Type.CONFIG_RESPONSE,
                Wire.Status.APPLIED, null, "", "revision", List.of(new Wire.Mapping("bad name", "impactor:event_points")))));
    }

    @Test void retryDelayIsBoundedWithJitter() {
        for (int attempt = 1; attempt <= 100; attempt++) {
            long cap = Math.min(300_000L, 2_000L << Math.min(8, attempt - 1));
            long delay = Backoff.delayMillis(attempt);
            assertTrue(delay >= cap / 2 && delay <= cap);
        }
    }

    @Test void transportRejectsMismatchedResponse() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/currency", exchange -> {
            byte[] body = Wire.encode(Wire.Message.request(Wire.Type.GET_CONFIG, "fabric", null, "", null, null, "", null)
                    .reply(Wire.Type.CONFIG_RESPONSE, Wire.Status.APPLIED, null, "", "revision", List.of()));
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
        try {
            Transport transport = new Transport("http://127.0.0.1:" + server.getAddress().getPort() + "/currency", "test-token");
            Wire.Message request = Wire.Message.request(Wire.Type.GET_CONFIG, "fabric", null, "", null, null, "", null);
            assertThrows(Exception.class, () -> transport.send(request).join());
        } finally { server.stop(0); }
    }
}
