# Impactor Network Connector (Minecraft 1.21.1)

Three Java 21 artefacts connect a Paper server to one authoritative Fabric/Impactor server through Velocity:

| Directory | Artefact | Role |
| --- | --- | --- |
| `client/` | Paper plugin | Async API, durable SQLite outbox, retry, scoreboard projection, diagnostics |
| `bridge/` | Velocity plugin | Authenticated HTTP message broker; stores no balances |
| `source/` | Fabric mod | Impactor adapter, SQLite transaction ledger and audit log |

`protocol/` contains Java source compiled into all three JARs. The bridge uses direct HTTP connections between servers. Paper and Fabric plugin messages depend on a player connection, so they cannot deliver offline player transactions reliably ([Paper documentation](https://docs.papermc.io/paper/dev/plugin-messaging/), [Velocity documentation](https://docs.papermc.io/velocity/dev/plugin-messaging/)). Use private network links or TLS termination for these HTTP endpoints; bearer tokens are otherwise visible to anyone who can capture traffic.

## Build

Run `./gradlew build` (Windows: `.\gradlew.bat build`). Deploy:

- `client/build/libs/client-0.1.0.jar` to Paper's `plugins/` directory.
- `bridge/build/libs/bridge-0.1.0.jar` to Velocity's `plugins/` directory.
- `source/build/libs/source-0.1.0.jar` to Fabric's `mods/` directory alongside Fabric API and Impactor 5.3.5 for Minecraft 1.21.1.

The Fabric build compiles against Impactor's published economy API 5.3.5. The source investigation used [Impactor's 1.21.1 branch](https://github.com/NickImpact/Impactor/tree/1.21.1) and its [economy API](https://github.com/NickImpact/ImpactorAPI). Test against the exact Impactor binary used in deployment before awarding live currency.

## Configuration

Paper `plugins/ImpactorNetworkClient/config.yml` contains only `authoritative-server: fabric`.

Fabric creates `config/impactor-network-source.properties` on first start. Example:

```properties
currencies=EventPoints|impactor:event_points
```

The objective name must be at most 16 characters. Currency keys must include a namespace and must exist in Impactor when Fabric starts. Duplicate objectives or currencies stop the source. Restart Fabric after changing this file; Paper can request the current configuration with `/currency config`.

Set environment variables before starting each process. Use different random secrets of at least 24 characters:

| Process | Variable | Example |
| --- | --- | --- |
| Paper | `IMPACTOR_PAPER_TOKEN` | shared only with Velocity |
| Paper | `IMPACTOR_BRIDGE_URL` | `http://127.0.0.1:8096/currency` |
| Velocity | `IMPACTOR_PAPER_TOKEN` | same as Paper |
| Velocity | `IMPACTOR_FABRIC_TOKEN` | shared only with Fabric |
| Velocity | `IMPACTOR_FABRIC_URL` | `http://127.0.0.1:8097/currency` |
| Velocity | `IMPACTOR_FABRIC_SERVER` | `fabric` (default) |
| Velocity | `IMPACTOR_BRIDGE_BIND` | `127.0.0.1:8096` (default) |
| Fabric | `IMPACTOR_FABRIC_TOKEN` | same as Velocity |
| Fabric | `IMPACTOR_FABRIC_SERVER` | `fabric` (default) |
| Fabric | `IMPACTOR_SOURCE_BIND` | `127.0.0.1:8097` (default) |

Bind to a private interface when the processes run on separate hosts. The Paper server name must match Velocity and Fabric's `IMPACTOR_FABRIC_SERVER` value. The Paper and Fabric SQLite files are created in each component's data/config directory.

## Paper API and commands

Other Paper plugins can obtain `CurrencyService` by casting the plugin instance or calling `((ClientPlugin) plugin).currencyService()`. Methods use player UUIDs and Impactor currency keys, work with offline players, and return `CompletableFuture` values. `addBalance`, `removeBalance`, and `setBalance` return `PENDING` only after the outbox insert commits. Call `transaction(id)` later for the definitive state. `setBalance` requires the caller's expected current balance, obtained from `getBalance`.

`/currency status`, `/currency config`, `/currency pending`, `/currency transaction <uuid>`, `/currency retry <uuid>`, and `/currency refresh <player>` require `impactor.currency.admin`. Mutations cannot be invoked through these commands. The Fabric command `/impactorcurrency status` shows the number of transactions requiring reconciliation.

Paper uses the main scoreboard as a projection. It requests each configured balance on join and after a relevant mutation, and updates scores on the server thread. Scoreboard scores are integers; non-integral or out-of-range balances are logged and left unprojected. The scoreboard is never read to calculate a mutation.

## Recovery and consistency

Paper writes mutations to SQLite before delivery. Requests retain their transaction UUID across retries and restarts. Outstanding rows are checked every two seconds, including after startup; backoff has jitter and caps at five minutes. Definitive results remain in the outbox for inspection. Fabric persists `RECEIVED` before attempting a mutation, then `APPLYING`. A repeated `APPLIED` transaction returns the prior result; a `RECEIVED` transaction may safely resume. `APPLYING` becomes `NEEDS_RECONCILIATION` on restart, and all further writes to that player/currency are blocked.

**This implementation does not claim strict exactly-once effects.** Impactor's account methods change an in-memory balance and schedule a separate asynchronous save; they cannot share an atomic commit with this mod's SQLite transaction row. The source awaits an explicit Impactor `save` before acknowledging, but a crash between the Impactor save and ledger completion still leaves an ambiguous outcome. Impactor also has no compare-and-swap balance API, so `setBalance` checks the expected balance only against other requests serialized through this source. External Impactor mutations can race with it.

For an ambiguous transaction, inspect `config/impactor-network-source.db` (`transactions.before_balance` and append-only `transaction_audit`) and Impactor's authoritative balance, then use `/impactorcurrency reconcile <transaction-uuid> applied` or `rejected` **only after verifying whether that exact effect occurred**. The command logs the decision and adds an audit event. Marking an applied transaction rejected can cause a lost award; marking an unapplied one applied can lose an award. Do not automatically replay ambiguous transactions.

The three modules build and protocol unit tests run with `./gradlew build`. Live Paper → Velocity → Fabric flow, Impactor persistence behavior, and crash fault injection still require a three-server test environment before production use.
