# raknetty-transport

[![Maven Central](https://img.shields.io/maven-central/v/io.github.agent0876.raknetty/raknetty-transport?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.agent0876.raknetty/raknetty-transport)

The **entry-point layer** of RakNetty. Provides high-level fluent builders (`RakNetServerBootstrap`, `RakNetClientBootstrap`) and a convenient callback base class (`SimpleRakNetHandler`) for quickly wiring up RakNet servers and clients.

> **This is the module most users should depend on.** It transitively includes `raknetty-core`, `raknetty-codec`, and `raknetty-handler`.

---

## Installation

```kotlin
// Gradle (Kotlin DSL)
dependencies {
    implementation("io.github.agent0876.raknetty:raknetty-transport:1.0.1")
}
```

```groovy
// Gradle (Groovy DSL)
dependencies {
    implementation 'io.github.agent0876.raknetty:raknetty-transport:1.0.1'
}
```

```xml
<!-- Maven -->
<dependency>
    <groupId>io.github.agent0876.raknetty</groupId>
    <artifactId>raknetty-transport</artifactId>
    <version>1.0.1</version>
</dependency>
```

---

## `RakNetServerBootstrap`

Fluent builder that configures and binds a UDP RakNet server.

### API

| Method | Default | Description |
|---|---|---|
| `group(EventLoopGroup)` | auto-created `MultithreadEventLoopGroup` | Netty event loop to use |
| `serverGuid(Long)` | `System.nanoTime()` | Server's unique 64-bit GUID |
| `serverInfo(() -> String)` | `""` | MOTD / server info string (lazy, called per ping) |
| `maxConnections(Int)` | `20` | Maximum simultaneous connections |
| `connectionConfig(ConnectionConfig)` | defaults | Reliability / timing tuning |
| `handler(ChannelHandler)` | **required** | Your application handler |
| `bind(port: Int)` | — | Binds and returns `Future<Channel>` |

### Example

```kotlin
import io.github.agent0876.raknetty.transport.RakNetServerBootstrap
import io.github.agent0876.raknetty.transport.SimpleRakNetHandler
import io.github.agent0876.raknetty.core.protocol.Reliability
import kotlin.random.Random

val future = RakNetServerBootstrap()
    .serverGuid(Random.nextLong())
    .serverInfo { "MCPE;My Server;594;1.20.80;0;20;${Random.nextLong()};Survival;1;19132;19133;" }
    .maxConnections(20)
    .handler(object : SimpleRakNetHandler() {
        override fun onConnect(ctx, connection) {
            println("+ ${connection.remoteAddress}")
        }
        override fun onMessage(ctx, connection, payload) {
            // echo back
            connection.send(payload.retainedSlice(), Reliability.RELIABLE_ORDERED)
        }
        override fun onDisconnect(ctx, connection, reason) {
            println("- ${connection.remoteAddress} ($reason)")
        }
    })
    .bind(19132)
    .asStage().sync()

val channel = future.getNow()
channel.closeFuture().asStage().sync()
```

---

## `RakNetClientBootstrap`

Fluent builder that performs the full RakNet handshake and returns a `Future<Void>` that resolves **after the handshake completes** (not just after the UDP socket opens).

### API

| Method | Default | Description |
|---|---|---|
| `group(EventLoopGroup)` | auto-created single-thread `MultithreadEventLoopGroup` | Netty event loop to use |
| `clientGuid(Long)` | `System.nanoTime()` | Client's unique 64-bit GUID |
| `mtu(Int)` | `1492` | Starting MTU size for OCReq1 |
| `connectionConfig(ConnectionConfig)` | defaults | Reliability / timing tuning |
| `handler(ChannelHandler)` | **required** | Your application handler |
| `connect(host, port)` | — | Starts handshake, returns `Future<Void>` |

### Example

```kotlin
import io.github.agent0876.raknetty.transport.RakNetClientBootstrap
import io.github.agent0876.raknetty.transport.SimpleRakNetHandler
import kotlin.random.Random

val future = RakNetClientBootstrap()
    .clientGuid(Random.nextLong())
    .handler(object : SimpleRakNetHandler() {
        override fun onConnect(ctx, connection) {
            println("Connected! ping=${connection.ping}ms mtu=${connection.mtu}")
        }
        override fun onMessage(ctx, connection, payload) {
            println("Received ${payload.readableBytes()} bytes")
        }
        override fun onDisconnect(ctx, connection, reason) {
            println("Disconnected: $reason")
        }
    })
    .connect("127.0.0.1", 19132)

future.asStage().sync()  // blocks until RakNet handshake is complete (or times out)
```

---

## `SimpleRakNetHandler`

Abstract convenience base class — extend this instead of implementing a `ChannelHandler` directly.

```kotlin
abstract class SimpleRakNetHandler {

    /** Called when a RakNet connection is fully established (post-handshake). */
    open fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {}

    /**
     * Called when an application-layer message arrives.
     * [payload] is automatically released after this method returns.
     * Call [Buffer.copy] if you need to hold onto it longer.
     */
    open fun onMessage(ctx: ChannelHandlerContext, connection: RakNetConnection, payload: Buffer) {}

    /** Called when the connection is closed. */
    open fun onDisconnect(ctx: ChannelHandlerContext, connection: RakNetConnection, reason: DisconnectReason) {}
}
```

> Unhandled messages and events are forwarded down the pipeline automatically.

---

## Pipeline Layout

The pipeline assembled by both bootstraps:

```
[DatagramDispatcher]      ← routes inbound datagrams to per-connection handlers
[ServerHandshakeHandler]  ← (server) or [ClientHandshakeHandler] (client)
[your handler]            ← receives RakNetPacket reads + RakNetEvent user events
```
