package dev.impactor.network.protocol;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public final class Transport {
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final URI endpoint;
    private final String token;
    public Transport(String url, String token) { this.endpoint = URI.create(url); this.token = token; }
    public CompletableFuture<Wire.Message> send(Wire.Message message) {
        try {
            HttpRequest request = HttpRequest.newBuilder(endpoint).timeout(Duration.ofSeconds(8))
                    .header("Content-Type", "application/octet-stream").header("X-Impactor-Token", token)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(Wire.encode(message))).build();
            return client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).thenApply(response -> {
                if (response.statusCode() != 200) throw new IllegalStateException("Transport HTTP " + response.statusCode());
                try {
                    Wire.Message reply = Wire.decode(response.body());
                    if (!reply.requestId().equals(message.requestId()) || !reply.server().equals(message.server()))
                        throw new IllegalStateException("Mismatched response");
                    return reply;
                } catch (Exception e) { throw new IllegalStateException("Bad response", e); }
            });
        } catch (Exception e) { return CompletableFuture.failedFuture(e); }
    }
}
