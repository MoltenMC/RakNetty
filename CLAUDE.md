# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

RakNetty is a Netty-based Kotlin implementation of the RakNet reliable-UDP protocol (protocol version 11, Minecraft-compatible). It is structured as a Gradle multi-module project.

## Build Commands

```bash
./gradlew build                                          # build all modules
./gradlew test                                           # run all tests
./gradlew :raknetty-core:test                            # single-module test
./gradlew test --tests "com.raknetty.codec.RakNetFrameCodecTest"      # single class
./gradlew test --tests "com.raknetty.codec.RakNetFrameCodecTest.wireSize matches actual encoded size"  # single method
./gradlew clean
```

## Gradle Configuration

- Gradle 9.6.0; Kotlin 2.4.0; JVM toolchain 25.
- `gradle.properties`: configuration cache, parallel builds, build caching all enabled.
- All dependency versions in `gradle/libs.versions.toml` (version catalog).
- Root `build.gradle.kts` applies plugins with `apply false` and sets group/version on subprojects.

## Module Map

| Module | Purpose |
|--------|---------|
| `raknetty-core` | Protocol constants, type model, utility math. No Netty pipeline code. |
| `raknetty-codec` | Pure encode/decode logic (no `ChannelHandler`). Depends on `raknetty-core`. |
| `raknetty-handler` | Netty `ChannelHandler`s: reliability layer, handshake state machines, connection registry. |
| `raknetty-transport` | `RakNetServerBootstrap` / `RakNetClientBootstrap` — user-facing API. |

## Key Package Layout

```
com.raknetty.core
├── protocol/    PacketId (all message IDs + OFFLINE_MESSAGE_ID magic), Reliability enum (8 modes),
│                RakNetProtocol (version=11, MTU probes, max channels), RakNetPriority
├── packet/      RakNetFrame (per-frame model + wireSize()), SplitInfo, RakNetDatagram (sealed: Data|Ack|Nak)
├── connection/  ConnectionState, DisconnectReason, RakNetConnection interface
├── util/        RangeList (sorted merged ranges, encode/decode for ACK/NAK wire format),
│                SequenceNumber (24-bit wrap-around arithmetic)
└── exception/   RakNetException hierarchy

com.raknetty.codec
├── RakNetDatagramCodec   — ByteBuf ↔ RakNetDatagram (Data/Ack/Nak). Call isOnlineDatagram(firstByte)
│                           to distinguish from offline packets before decoding.
├── RakNetFrameCodec      — ByteBuf ↔ RakNetFrame (individual encapsulated frame within a datagram)
├── offline/              OfflinePacket (sealed class) + OfflinePacketCodec
│                           decode() takes the full buf (ID byte included); returns null on bad magic.
└── connected/            ConnectedPacket (sealed class) + ConnectedPacketCodec
                            Handles CONNECTION_REQUEST, CONNECTION_REQUEST_ACCEPTED,
                            NEW_INCOMING_CONNECTION, DISCONNECTION_NOTIFICATION, CONNECTED_PING/PONG.
```

## Protocol Architecture

```
[Application]
     ↕
[RakNetConnection.send(ByteBuf, Reliability, orderChannel)]   ← user API (raknetty-transport)
     ↕
[ReliabilityLayer]         ACK/NAK tracking, reorder buffer, retransmit queue  (raknetty-handler)
     ↕
[FragmentSplitter/Reassembler]                                                  (raknetty-handler)
     ↕
[ConnectedPacketCodec]     ByteBuf ↔ ConnectedPacket                            (raknetty-codec)
[RakNetFrameCodec]         ByteBuf ↔ RakNetFrame                                (raknetty-codec)
[RakNetDatagramCodec]      ByteBuf ↔ RakNetDatagram (Data|Ack|Nak)              (raknetty-codec)
[OfflinePacketCodec]       ByteBuf ↔ OfflinePacket  (handshake, ping/pong)      (raknetty-codec)
     ↕
[NioDatagramChannel / EpollDatagramChannel]
```

```
com.raknetty.handler
├── DatagramDispatcher          — @Sharable; first handler on NioDatagramChannel pipeline.
│                                 Routes DatagramPacket → online (to ConnectionRegistry) or
│                                 offline (fires AddressedOfflinePacket up the pipeline).
├── ConnectionRegistry          — ConcurrentHashMap<InetSocketAddress, RakNetConnectionImpl>
├── AddressedOfflinePacket      — envelope: OfflinePacket + sender InetSocketAddress
├── event/
│   ├── RakNetEvent             — sealed: Connected | Disconnected (fired via fireUserEventTriggered)
│   └── RakNetPacket            — application data: connection + payload ByteBuf
├── handshake/
│   ├── ServerHandshakeHandler  — OCReq1→OCReply1, OCReq2→OCReply2, creates RakNetConnectionImpl
│   └── ClientHandshakeHandler  — drives client side; call connect() to start
├── connection/
│   ├── ConnectionConfig        — tunable timeouts, tick interval, window size
│   └── RakNetConnectionImpl    — owns SendBuffer + ReceiveBuffer + FragmentAccumulator;
│                                 schedules 10ms tick via EventLoop.scheduleAtFixedRate()
└── reliability/
    ├── SendBuffer              — outbound: frame queuing, MTU packing, unacked window, retransmit
    ├── ReceiveBuffer           — inbound: datagram gap→NAK, reliable dedup, ordered/sequenced delivery
    └── FragmentAccumulator     — split-packet reassembly into composite ByteBuf (zero-copy)

com.raknetty.transport
├── RakNetServerBootstrap       — fluent builder: bind UDP port, configure handler + registry
├── RakNetClientBootstrap       — fluent builder: performs full handshake, returns Future<Void>
└── SimpleRakNetHandler         — convenience abstract class; extend instead of ChannelHandler directly
```

## Handler Pipeline (Server)

```
NioDatagramChannel
  → DatagramDispatcher          (inbound: route or decode offline)
  → ServerHandshakeHandler      (inbound: handle AddressedOfflinePacket)
  → [user application handler]  (inbound: RakNetPacket / RakNetEvent user events)
```

Online datagrams bypass the pipeline after `DatagramDispatcher` — they are dispatched directly
to `RakNetConnectionImpl.onDatagramReceived()`. The connection writes outbound `DatagramPacket`s
directly to the channel, skipping the handler pipeline (UDP server pattern).

## Connection Lifecycle

```
Server                          Client
  ← OCReq1 (MTU probe)         ClientHandshakeHandler.connect()
  → OCReply1                   OfflinePacketCodec
  ← OCReq2
  → OCReply2                   creates RakNetConnectionImpl, starts ticker
  ← ConnectionRequest          (reliable frame inside datagram)
  → ConnectionRequestAccepted  RakNetConnectionImpl.handleConnectedPacket()
  ← NewIncomingConnection
  fireUserEventTriggered(RakNetEvent.Connected)  ← both sides
```

### Connection States

```
UNCONNECTED → CONNECTING → HANDSHAKING → CONNECTED → DISCONNECTING → DISCONNECTED
```

`ConnectionState.isActive` returns `true` for `CONNECTING`, `HANDSHAKING`, and `CONNECTED`.
`forceDisconnect()` gates on `state == DISCONNECTED` to prevent double-release.

## SimpleRakNetHandler

The primary user-facing entry point. Extend this instead of implementing `ChannelHandler` directly:

```kotlin
abstract class SimpleRakNetHandler {
    open fun onConnect(ctx, connection: RakNetConnection) {}
    open fun onMessage(ctx, connection: RakNetConnection, payload: Buffer) {}  // payload auto-released after return
    open fun onDisconnect(ctx, connection: RakNetConnection, reason: DisconnectReason) {}
}
```

**Critical**: `onMessage` payload is released by a `finally` block in `SimpleRakNetHandler.messageReceived`
immediately after `onMessage` returns. Call `payload.copy()` if you need to hold it past the callback.

## Important Invariants

- **Sequence numbers are 24-bit** (0..16_777_215). Always use `SequenceNumber.*` helpers; never compare raw `Int` values across wrap-around boundaries.
- **ByteBuf lifecycle**: `RakNetFrame.payload` is owned by the holder and must be `release()`d. The codec allocates new buffers on decode; call `RakNetDatagram.Data.release()` after processing.
- **Offline vs online discrimination**: first byte `and 0x80 != 0` → online datagram (ACK/NAK/Data). Anything else → offline packet. `RakNetDatagramCodec.isOnlineDatagram(firstByte)` encodes this.
- **IPv4 address bytes are XOR'd with 0xFF** on the wire (RakNet spec). `BufExtensions.readAddress / writeAddress` handles this.
- **MTU for OCReq1** is not encoded in the payload — it is inferred from `totalPayloadSize + 28` (20 IP + 8 UDP headers). The codec captures `buf.readableBytes()` *before* reading the ID byte for this reason.
- **`SendBuffer` thread safety**: all methods must be called from the connection's `EventLoop` thread. The ticker (`scheduleAtFixedRate`) already guarantees this.
- **`RakNetConnectionImpl.state`**: declared `@Volatile override var`. External callers (handshake handlers) set it via direct property assignment (`conn.state = …`) — do NOT add a `fun setState()` as it clashes with the JVM setter signature.
- **Application message IDs**: bytes `0x00–0x7F` are reserved for internal RakNet messages; `0x80+` are user application data. `dispatchPayload()` uses this boundary to decide whether to decode as `ConnectedPacket` or fire as `RakNetPacket`.
- **`RakNetConnection.send()` ownership**: the callee takes ownership of `payload`. After calling `send()`, callers must NOT release or read the buffer.
- **`datagram.release()` is intentionally NOT called** in `onDatagramReceived` — each frame's payload is managed individually inside `processFrame` to avoid double-close.

## Thread Safety

All `RakNetConnectionImpl` mutation (send, receive, tick) must happen on the connection's `EventLoop` thread.

- **`send()`**: safe to call from any thread — marshals to the EventLoop if not already on it.
  However, there is a race: if the connection disconnects between the `state.isActive` check and
  the submitted `execute {}` block running, the payload is leaked. Always re-check `state.isActive`
  inside the lambda if calling from off the EventLoop.
- **`disconnect()`**: currently calls `doSend()` and `forceDisconnect()` directly without
  marshalling to the EventLoop. Must be called from the EventLoop thread or from the same thread
  that owns the connection, to avoid concurrent access to `SendBuffer`.

## Known Latent Bugs

These are confirmed correctness issues present in the codebase that have not yet been fixed:

1. **NAK range wrap-around crash** (`ReceiveBuffer.kt:38`): When a datagram gap spans the 24-bit
   sequence-number wrap boundary (e.g., `nextExpected = 16_777_213`, received `seqNum = 2`),
   `onDatagramArrived` returns `Pair(nakStart=16_777_213, nakEnd=1)`. `RangeList.addRange` calls
   `require(start <= end)` which throws `IllegalArgumentException`. `DatagramDispatcher` only
   catches `InvalidPacketException`, so this propagates uncaught and crashes the channel.
   Fix: when `nakEnd < nakStart`, split into two ranges `[nakStart..MAX]` and `[0..nakEnd]`.

2. **`FragmentAccumulator` splitIndex out of bounds** (`FragmentAccumulator.kt:49`): `splitIndex`
   is never validated against `splitCount` before the array access `set.fragments[split.splitIndex]`.
   A single frame with `splitIndex >= splitCount` (malformed or from a buggy sender) throws
   uncaught `ArrayIndexOutOfBoundsException`. Also, if two fragments arrive with the same `splitId`
   but different `splitCount` values, the second fragment's `splitIndex` may exceed the first
   fragment's array size.
   Fix: add `require(split.splitIndex in 0 until split.splitCount)` and
   `require(split.splitCount == set.splitCount)` in `accumulate()`.
