# raknetty-core

[![Maven Central](https://img.shields.io/maven-central/v/io.github.agent0876.raknetty/raknetty-core?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.agent0876.raknetty/raknetty-core)

The **foundation layer** of RakNetty. Contains protocol constants, packet data models, and the core `RakNetConnection` interface — all with zero external dependencies beyond Netty's buffer API.

---

## Installation

```kotlin
// Gradle (Kotlin DSL)
dependencies {
    implementation("io.github.agent0876.raknetty:raknetty-core:0.1.0")
}
```

> Most users should depend on `raknetty-transport` instead, which pulls this in transitively.

---

## Contents

### Protocol Constants — `RakNetProtocol`

```kotlin
RakNetProtocol.VERSION           // 11  (Minecraft-compatible)
RakNetProtocol.MAX_ORDER_CHANNELS // 32
RakNetProtocol.MAX_SPLIT_COUNT   // 128
RakNetProtocol.MTU_SIZES         // [1492, 1200, 576]
```

### Packet IDs — `PacketId`

All RakNet packet type constants, grouped by phase:

| Group | Examples |
|---|---|
| Offline (pre-handshake) | `UNCONNECTED_PING`, `OPEN_CONNECTION_REQUEST_1/2`, `OPEN_CONNECTION_REPLY_1/2` |
| Connection handshake | `CONNECTION_REQUEST`, `CONNECTION_REQUEST_ACCEPTED`, `NEW_INCOMING_CONNECTION` |
| Connected lifecycle | `DISCONNECTION_NOTIFICATION`, `CONNECTED_PING`, `CONNECTED_PONG` |
| Datagram flags | `FLAG_VALID (0x80)`, `FLAG_ACK (0x40)`, `FLAG_NAK (0x20)` |

### Reliability — `Reliability`

Delivery guarantee modes encoded in each frame's 3-bit flags field:

| Value | Reliable | Ordered | Sequenced |
|---|---|---|---|
| `UNRELIABLE` | ✗ | ✗ | ✗ |
| `UNRELIABLE_SEQUENCED` | ✗ | ✗ | ✓ |
| `RELIABLE` | ✓ | ✗ | ✗ |
| `RELIABLE_ORDERED` | ✓ | ✓ | ✗ |
| `RELIABLE_SEQUENCED` | ✓ | ✗ | ✓ |
| `UNRELIABLE_WITH_ACK_RECEIPT` | ✗ | ✗ | ✗ |
| `RELIABLE_WITH_ACK_RECEIPT` | ✓ | ✗ | ✗ |
| `RELIABLE_ORDERED_WITH_ACK_RECEIPT` | ✓ | ✓ | ✗ |

### Priority — `RakNetPriority`

```kotlin
RakNetPriority.NORMAL    // queued, subject to AIMD congestion control (default)
RakNetPriority.IMMEDIATE // bypasses send queue — use for control messages (ping, disconnect)
```

### `RakNetConnection` Interface

The public API for a single logical connection:

```kotlin
interface RakNetConnection {
    val remoteAddress: InetSocketAddress
    val guid: Long           // 64-bit remote GUID
    val mtu: Int             // negotiated MTU
    val state: ConnectionState
    val channel: Channel     // underlying shared UDP channel
    val ping: Int            // smoothed RTT in ms

    fun send(
        payload: ByteBuf,
        reliability: Reliability,
        orderChannel: Int = 0,
        priority: RakNetPriority = RakNetPriority.NORMAL,
    ): ChannelFuture

    fun disconnect(reason: DisconnectReason = DisconnectReason.CLIENT_REQUESTED): ChannelFuture
}
```

### Connection States — `ConnectionState`

`CONNECTING` → `CONNECTED` → `DISCONNECTING` → `DISCONNECTED`

### Disconnect Reasons — `DisconnectReason`

`CLIENT_REQUESTED`, `KICKED`, `TIMED_OUT`, `CONNECTION_LOST`

### Utilities

- **`SequenceNumber`** — wrapping 24-bit sequence number with comparison helpers
- **`RangeList`** — compact range-list encoding for ACK/NAK datagram payloads
