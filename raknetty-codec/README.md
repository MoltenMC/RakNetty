# raknetty-codec

[![Maven Central](https://img.shields.io/maven-central/v/io.github.agent0876.raknetty/raknetty-codec?label=Maven%20Central)](https://central.sonatype.com/artifact/io.github.agent0876.raknetty/raknetty-codec)

The **codec layer** of RakNetty. Provides Netty `ChannelHandler` codecs that encode and decode raw UDP bytes into structured RakNet packet objects, and vice versa.

---

## Installation

```kotlin
// Gradle (Kotlin DSL)
dependencies {
    implementation("io.github.agent0876.raknetty:raknetty-codec:1.0.1")
}
```

> Most users should depend on `raknetty-transport` instead, which pulls this in transitively.

---

## Contents

### `RakNetDatagramCodec`

Decodes incoming `DatagramPacket` bytes into either:
- An **offline packet** (pre-handshake, routed by `FLAG_VALID` absence)
- A **RakNet datagram** (post-handshake, `FLAG_VALID` set)

Encodes outgoing `RakNetDatagram` objects back into `DatagramPacket` for UDP transmission.

### `RakNetFrameCodec`

Decodes the **frame list** packed inside a single RakNet datagram.  
Each frame carries:
- Reliability flags (3 bits)
- Split-packet metadata (fragment index, split ID, fragment count)
- Order channel + order index (for ordered/sequenced reliabilities)
- The actual payload `Buffer`

### `OfflinePacketCodec`

Encodes/decodes **pre-handshake offline packets**:

| Packet | Direction |
|---|---|
| `UnconnectedPing` / `UnconnectedPong` | Server discovery |
| `OpenConnectionRequest1` / `OpenConnectionReply1` | MTU negotiation (step 1) |
| `OpenConnectionRequest2` / `OpenConnectionReply2` | MTU negotiation (step 2) |
| `IncompatibleProtocolVersion` | Version mismatch rejection |

All offline packets are prefixed with the 16-byte `OFFLINE_MESSAGE_ID` magic to distinguish them from non-RakNet UDP traffic.

### `ConnectedPacketCodec`

Encodes/decodes **post-handshake connected packets**:

| Packet | Direction |
|---|---|
| `ConnectionRequest` | Client → Server |
| `ConnectionRequestAccepted` | Server → Client |
| `NewIncomingConnection` | Client → Server |
| `ConnectedPing` / `ConnectedPong` | Both (keepalive) |
| `DisconnectionNotification` | Both (clean close) |

### `BufExtensions` (internal)

Extension functions on `Buffer` for reading/writing RakNet-specific types:
- `readUInt24LE()` / `writeUInt24LE()` — 3-byte little-endian sequence numbers
- `readAddress()` / `writeAddress()` — IPv4/IPv6 socket addresses in RakNet format
- `readOfflineMessageId()` / `assertOfflineMessageId()` — magic bytes validation
