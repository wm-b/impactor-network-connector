package dev.impactor.network.bridge;

import com.google.inject.Inject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.Plugin;
import com.velocitypowered.api.proxy.ProxyServer;
import dev.impactor.network.protocol.Transport;
import dev.impactor.network.protocol.Wire;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.concurrent.Executors;

@Plugin(id = "impactor_network_bridge", name = "Impactor Network Connector Bridge", version = "0.1.0")
public final class BridgePlugin {
    private final Logger log;
    private HttpServer http;
    private Transport fabric;
    private String paperToken;
    private String fabricServer;

    @Inject public BridgePlugin(ProxyServer proxy, Logger log) { this.log = log; }

    @Subscribe public void start(ProxyInitializeEvent event) throws IOException {
        paperToken = required("IMPACTOR_PAPER_TOKEN");
        fabricServer = System.getenv().getOrDefault("IMPACTOR_FABRIC_SERVER", "fabric");
        fabric = new Transport(required("IMPACTOR_FABRIC_URL"), required("IMPACTOR_FABRIC_TOKEN"));
        String[] bind = System.getenv().getOrDefault("IMPACTOR_BRIDGE_BIND", "127.0.0.1:8096").split(":", 2);
        http = HttpServer.create(new InetSocketAddress(bind[0], Integer.parseInt(bind[1])), 32);
        http.createContext("/currency", this::handle);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.start();
        log.info("Currency bridge listening on {}", http.getAddress());
    }

    @Subscribe public void stop(ProxyShutdownEvent event) { if (http != null) http.stop(0); }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"POST".equals(exchange.getRequestMethod()) || !same(paperToken, exchange.getRequestHeaders().getFirst("X-Impactor-Token"))) {
                respond(exchange, 403, new byte[0]); return;
            }
            byte[] input = exchange.getRequestBody().readNBytes(Wire.MAX_BYTES + 1);
            Wire.Message request = Wire.decode(input);
            if (!request.server().equals(fabricServer) || !(request.mutation() || request.type() == Wire.Type.GET_CONFIG
                    || request.type() == Wire.Type.GET_BALANCE) || request.status() != Wire.Status.NONE) {
                respond(exchange, 400, new byte[0]); return;
            }
            Wire.Message response = fabric.send(request).join();
            if (response.type() != Wire.Type.CONFIG_RESPONSE && response.type() != Wire.Type.BALANCE_RESPONSE
                    && response.type() != Wire.Type.MUTATION_RESPONSE) throw new IOException("Invalid source response");
            respond(exchange, 200, Wire.encode(response));
        } catch (Exception e) {
            log.warn("Currency bridge request failed: {}", e.toString());
            try { if (exchange.getResponseCode() == -1) respond(exchange, 503, new byte[0]); } catch (Exception ignored) {}
        }
    }
    private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length); exchange.getResponseBody().write(body);
    }
    private static boolean same(String expected, String actual) {
        return actual != null && MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), actual.getBytes(StandardCharsets.UTF_8));
    }
    private static String required(String name) {
        String v = System.getenv(name); if (v == null || v.length() < 24) throw new IllegalStateException(name + " must contain at least 24 characters"); return v;
    }
}
