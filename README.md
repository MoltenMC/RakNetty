# RakNetty

[![Maven Central](https://img.shields.io/maven-central/v/io.github.agent0876.raknetty/raknetty-core?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.agent0876.raknetty/raknetty-core)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Kotlin](https://img.shields.io/badge/Kotlin-2.4-purple.svg)](https://kotlinlang.org)

**RakNetty** is a Kotlin implementation of the [RakNet](https://github.com/facebookarchive/RakNet) UDP networking protocol built on top of [Netty 4](https://netty.io/).  
It provides a Netty-idiomatic API for both server and client roles, with full reliability, ordering, and fragmentation support.

---

## Features

- **Full RakNet reliability layer** — UNRELIABLE, RELIABLE, RELIABLE_ORDERED, RELIABLE_SEQUENCED, and ACK-receipt variants
- **Automatic fragmentation & reassembly** — large payloads are split and reassembled transparently
- **AIMD congestion control** — adaptive send window with RTT-based retransmission timeout (RTO)
- **Keepalive & timeout detection** — idle-connection pings and inactivity timeout
- **Multi-connection multiplexing** — many logical connections share a single UDP socket
- **Netty pipeline integration** — plug your own `ChannelHandler` into the pipeline

---

## Modules

| Module | Artifact | Description |
|---|---|---|
| `raknetty-core` | `io.github.agent0876.raknetty:raknetty-core` | Protocol constants, packet models, `RakNetConnection` interface |
| `raknetty-codec` | `io.github.agent0876.raknetty:raknetty-codec` | Netty codecs for datagrams, frames, offline/connected packets |
| `raknetty-handler` | `io.github.agent0876.raknetty:raknetty-handler` | Handshake, reliability, and connection lifecycle handlers |
| `raknetty-transport` | `io.github.agent0876.raknetty:raknetty-transport` | `RakNetServerBootstrap` & `RakNetClientBootstrap`, `SimpleRakNetHandler` |

> **Typical dependency**: just add `raknetty-transport` — it transitively pulls in all other modules.

---

## Requirements

- JDK 17+
- Kotlin 1.9+

---

## Installation

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("io.github.agent0876.raknetty:raknetty-transport:0.1.0")
}
```

### Gradle (Groovy DSL)

```groovy
dependencies {
    implementation 'io.github.agent0876.raknetty:raknetty-transport:0.1.0'
}
```

### Maven

```xml
<dependency>
    <groupId>io.github.agent0876.raknetty</groupId>
    <artifactId>raknetty-transport</artifactId>
    <version>0.1.0</version>
</dependency>
```

---

## Quick Start

### Server

```kotlin
import io.github.agent0876.raknetty.core.connection.DisconnectReason
import io.github.agent0876.raknetty.core.connection.RakNetConnection
import io.github.agent0876.raknetty.core.protocol.Reliability
import io.github.agent0876.raknetty.transport.RakNetServerBootstrap
import io.github.agent0876.raknetty.transport.SimpleRakNetHandler
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import kotlin.random.Random

fun main() {
    val future = RakNetServerBootstrap()
        .serverGuid(Random.nextLong())
        .serverInfo { "MCPE;My Server;594;1.20.80;0;20;${Random.nextLong()};Survival;1;19132;19133;" }
        .maxConnections(20)
        .handler(object : SimpleRakNetHandler() {
            override fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {
                println("Connected: ${connection.remoteAddress}")
            }

            override fun onMessage(ctx: ChannelHandlerContext, connection: RakNetConnection, payload: ByteBuf) {
                println("Packet from ${connection.remoteAddress}: ${payload.readableBytes()} bytes")

                // Echo back
                val echo = payload.retainedSlice()
                connection.send(echo, Reliability.RELIABLE_ORDERED)
            }

            override fun onDisconnect(ctx: ChannelHandlerContext, connection: RakNetConnection, reason: DisconnectReason) {
                println("Disconnected: ${connection.remoteAddress} ($reason)")
            }
        })
        .bind(19132)
        .sync()

    println("Server listening on :19132")
    future.channel().closeFuture().sync()
}
```

### Client

```kotlin
import io.github.agent0876.raknetty.transport.RakNetClientBootstrap
import io.github.agent0876.raknetty.transport.SimpleRakNetHandler
import kotlin.random.Random

fun main() {
    val connectFuture = RakNetClientBootstrap()
        .clientGuid(Random.nextLong())
        .handler(object : SimpleRakNetHandler() {
            override fun onConnect(ctx, connection) {
                println("Connected to ${connection.remoteAddress} (ping: ${connection.ping} ms)")
            }

            override fun onMessage(ctx, connection, payload) {
                println("Received ${payload.readableBytes()} bytes")
            }

            override fun onDisconnect(ctx, connection, reason) {
                println("Disconnected: $reason")
            }
        })
        .connect("127.0.0.1", 19132)

    connectFuture.sync()  // blocks until RakNet handshake completes
    println("Handshake complete!")
}
```

---

## Advanced Usage

### Custom `ConnectionConfig`

Fine-tune reliability and congestion control parameters:

```kotlin
import io.github.agent0876.raknetty.handler.connection.ConnectionConfig

val config = ConnectionConfig(
    initialRto          = 200L,   // Initial retransmit timeout (ms)
    minRto              = 100L,   // Minimum RTO
    maxRto              = 3_000L, // Maximum RTO (backoff cap)
    initialCwnd         = 4,      // Initial congestion window (datagrams)
    pingInterval        = 5_000L, // Keepalive ping interval (ms)
    connectionTimeout   = 10_000L,// Inactivity timeout (ms)
    maxUnackedDatagrams = 512,    // Congestion window hard cap
    tickIntervalMs      = 10L,    // ACK/NAK flush interval (ms)
)

RakNetServerBootstrap()
    .connectionConfig(config)
    // ...
```

### Reliability Modes

| `Reliability` | Guaranteed delivery | Order preserved | Notes |
|---|---|---|---|
| `UNRELIABLE` | ✗ | ✗ | Fire-and-forget |
| `UNRELIABLE_SEQUENCED` | ✗ | Latest only | Out-of-order packets dropped |
| `RELIABLE` | ✓ | ✗ | Retransmitted until ACKed |
| `RELIABLE_ORDERED` | ✓ | ✓ | Most common for game packets |
| `RELIABLE_SEQUENCED` | ✓ | Latest only | Reliable but only newest matters |

### Sending with Priority

```kotlin
import io.github.agent0876.raknetty.core.protocol.RakNetPriority

// IMMEDIATE: bypasses send queue (for control messages)
connection.send(buf, Reliability.UNRELIABLE, priority = RakNetPriority.IMMEDIATE)

// NORMAL (default): queued, subject to congestion control
connection.send(buf, Reliability.RELIABLE_ORDERED)
```

---

## Architecture

```
raknetty-transport
    RakNetServerBootstrap / RakNetClientBootstrap
    SimpleRakNetHandler (callback base class)
        │
raknetty-handler
    DatagramDispatcher       ← routes inbound datagrams to per-connection handlers
    ServerHandshakeHandler   ← OCReq1/2, OCReply1/2, ConnectionRequest/Accepted
    ClientHandshakeHandler
    RakNetConnectionImpl     ← per-connection reliability, reordering, fragmentation
        │
raknetty-codec
    RakNetDatagramCodec      ← decode/encode UDP datagrams
    RakNetFrameCodec         ← decode/encode frames inside a datagram
    OfflinePacketCodec       ← pre-handshake offline packets
    ConnectedPacketCodec     ← post-handshake connected packets
        │
raknetty-core
    RakNetConnection (interface)
    Reliability, RakNetPriority, PacketId
    RakNetDatagram, RakNetFrame
    SequenceNumber, RangeList
```

---

## Contributing

Pull requests are welcome! Please open an issue first to discuss major changes.

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Commit your changes
4. Push and open a Pull Request

---

## License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.
