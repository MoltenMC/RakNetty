# raknetty-handler

[![Maven Central](https://img.shields.io/maven-central/v/io.github.agent0876.raknetty/raknetty-handler?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.agent0876.raknetty/raknetty-handler)

The **handler layer** of RakNetty. Contains the core business logic: handshake negotiation, per-connection reliability, reordering, fragmentation/reassembly, congestion control, and keepalive.

---

## Installation

```kotlin
// Gradle (Kotlin DSL)
dependencies {
    implementation("io.github.agent0876.raknetty:raknetty-handler:1.0.0")
}
```

> Most users should depend on `raknetty-transport` instead, which pulls this in transitively.

---

## Contents

### `DatagramDispatcher`

A `SimpleChannelInboundHandler<DatagramPacket>` placed at the front of the Netty pipeline.  
Routes each inbound datagram to the correct `RakNetConnectionImpl` based on `remoteAddress`, or passes offline packets downstream for handshake handling.

### `ConnectionRegistry`

Thread-safe map of `InetSocketAddress → RakNetConnectionImpl`.  
Shared between `DatagramDispatcher` and handshake handlers.

### `ServerHandshakeHandler`

Handles the server-side of the RakNet handshake:

```
Client                              Server
  │──── UnconnectedPing ────────────▶│  (optional, for MOTD)
  │◀─── UnconnectedPong ─────────────│
  │
  │──── OpenConnectionRequest1 ─────▶│  (MTU probe, step 1)
  │◀─── OpenConnectionReply1 ────────│
  │
  │──── OpenConnectionRequest2 ─────▶│  (MTU confirm, step 2)
  │◀─── OpenConnectionReply2 ────────│
  │
  │──── ConnectionRequest ──────────▶│  (connected phase)
  │◀─── ConnectionRequestAccepted ───│
  │──── NewIncomingConnection ──────▶│
  │                                  │  ← RakNetEvent.Connected fired
```

Configurable via:
- `serverGuid: Long` — server's 64-bit GUID
- `serverInfo: () -> String` — MOTD / server info string (evaluated lazily per ping)
- `maxConnections: Int` — rejects new connections when at capacity

### `ClientHandshakeHandler`

Handles the client-side of the RakNet handshake.  
Sends `OCReq1` with MTU probes in descending order (`1492 → 1200 → 576`) and retries on timeout (`ocReq1RetryIntervalMs`).  
After `ConnectionRequestAccepted`, fires `RakNetEvent.Connected`.

### `RakNetConnectionImpl`

The core per-connection engine implementing `RakNetConnection`:

#### Reliability & Retransmission
- **Send buffer** — holds unACKed frames, retransmits on timeout using RTT-based RTO with exponential backoff
- **Receive buffer** — reorders out-of-order RELIABLE_ORDERED frames per channel
- **ACK / NAK** — batched and flushed on every tick

#### Congestion Control (AIMD)
- Slow start and congestion avoidance phases
- Window shrinks on loss, grows on ACK
- Configurable `initialCwnd` and `maxUnackedDatagrams`

#### Fragmentation & Reassembly (`FragmentAccumulator`)
- Large payloads automatically split into frames ≤ MTU
- Fragments reassembled in order; incomplete sets expire after `fragmentTimeout`

#### Keepalive & Timeout
- Sends `CONNECTED_PING` after `pingInterval` ms of inactivity
- Disconnects after `connectionTimeout` ms without any inbound datagram

### Events

User-layer handlers receive these via `channelInboundEvent`:

```kotlin
sealed class RakNetEvent {
    data class Connected(val connection: RakNetConnection) : RakNetEvent()
    data class Disconnected(val connection: RakNetConnection, val reason: DisconnectReason) : RakNetEvent()
}
```

Application data arrives as `channelRead` with a `RakNetPacket`:

```kotlin
class RakNetPacket(val connection: RakNetConnection, val payload: Buffer)
```

### `ConnectionConfig`

All tunable reliability and lifecycle parameters:

```kotlin
data class ConnectionConfig(
    val initialRto: Long          = 200L,     // Initial RTO before RTT samples
    val minRto: Long              = 100L,     // Minimum RTO
    val maxRto: Long              = 3_000L,   // Maximum RTO (backoff cap)
    val initialCwnd: Int          = 4,        // Initial congestion window (datagrams)
    val pingInterval: Long        = 5_000L,   // Keepalive interval (ms)
    val connectionTimeout: Long   = 10_000L,  // Inactivity timeout (ms)
    val maxUnackedDatagrams: Int  = 512,      // Congestion window hard cap
    val fragmentOverhead: Int     = 60,       // Header bytes subtracted from MTU for split threshold
    val tickIntervalMs: Long      = 10L,      // ACK/NAK & retransmit tick interval (ms)
    val fragmentTimeout: Long     = 30_000L,  // Incomplete split packet expiry (ms)
    val ocReq1RetryIntervalMs: Long = 1_000L, // OCReq1 retry interval during MTU probe (ms)
)
```
