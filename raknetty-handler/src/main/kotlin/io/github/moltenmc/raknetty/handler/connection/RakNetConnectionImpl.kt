package io.github.moltenmc.raknetty.handler.connection

import io.github.moltenmc.raknetty.codec.RakNetFrameCodec
import io.github.moltenmc.raknetty.codec.connected.ConnectedPacket
import io.github.moltenmc.raknetty.codec.connected.ConnectedPacketCodec
import io.github.moltenmc.raknetty.core.connection.ConnectionState
import io.github.moltenmc.raknetty.core.connection.DisconnectReason
import io.github.moltenmc.raknetty.core.connection.RakNetConnection
import io.github.moltenmc.raknetty.core.protocol.RakNetPriority
import io.github.moltenmc.raknetty.core.exception.ConnectionClosedException
import io.github.moltenmc.raknetty.core.packet.RakNetDatagram
import io.github.moltenmc.raknetty.core.packet.RakNetFrame
import io.github.moltenmc.raknetty.core.packet.SplitInfo
import io.github.moltenmc.raknetty.core.protocol.Reliability
import io.github.moltenmc.raknetty.core.util.SequenceNumber
import io.github.moltenmc.raknetty.handler.ConnectionRegistry
import io.github.moltenmc.raknetty.handler.event.RakNetEvent
import io.github.moltenmc.raknetty.handler.event.RakNetPacket
import io.github.moltenmc.raknetty.handler.reliability.FragmentAccumulator
import io.github.moltenmc.raknetty.handler.reliability.ReceiveBuffer
import io.github.moltenmc.raknetty.handler.reliability.SendBuffer
import io.netty5.buffer.Buffer
import io.netty5.channel.Channel
import io.netty5.channel.ChannelHandlerContext
import io.netty5.channel.socket.DatagramPacket
import io.netty5.util.concurrent.Future
import io.netty5.util.concurrent.Promise
import io.netty5.util.internal.logging.InternalLoggerFactory
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

/**
 * Concrete [RakNetConnection] implementation.
 *
 * Owns the entire reliability layer for one virtual connection. All mutation
 * (send, receive, tick) must happen on [channel]'s EventLoop thread.
 */
class RakNetConnectionImpl(
    override val remoteAddress: InetSocketAddress,
    override val guid: Long,
    override val mtu: Int,
    override val channel: Channel,
    private val registry: ConnectionRegistry,
    private val config: ConnectionConfig = ConnectionConfig(),
) : RakNetConnection {

    @Volatile override var state: ConnectionState = ConnectionState.CONNECTING
    @Volatile override var ping: Int = 0

    private val log = InternalLoggerFactory.getInstance(RakNetConnectionImpl::class.java)

    private val sendBuffer       = SendBuffer(mtu, config)
    private val receiveBuffer    = ReceiveBuffer()
    private val fragmentAccum    = FragmentAccumulator()

    private var lastRecvTime     = System.currentTimeMillis()
    private var lastPingSentTime = 0L
    private var nextSplitId      = 0
    private var tickerFuture: Future<Void>? = null

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    /**
     * Schedules the periodic tick on the [channel]'s EventLoop.
     * Must be called once after the connection is added to [ConnectionRegistry].
     */
    fun startTicker() {
        tickerFuture = channel.executor().scheduleAtFixedRate(
            { if (state != ConnectionState.DISCONNECTED) tick() },
            config.tickIntervalMs,
            config.tickIntervalMs,
            TimeUnit.MILLISECONDS,
        )
    }

    private fun tick() {
        val now  = System.currentTimeMillis()
        val alloc = channel.bufferAllocator()

        sendBuffer.flushAcks(alloc)?.let  { channel.write(DatagramPacket(it, remoteAddress)) }
        sendBuffer.flushNaks(alloc)?.let  { channel.write(DatagramPacket(it, remoteAddress)) }
        sendBuffer.retransmitTimedOut(now, alloc).forEach { channel.write(DatagramPacket(it, remoteAddress)) }
        sendBuffer.flushSendQueue(now, alloc).forEach    { channel.write(DatagramPacket(it, remoteAddress)) }

        // Evict stale incomplete split packets to prevent memory leaks
        fragmentAccum.removeExpired(now, config.fragmentTimeout)

        // Keepalive ping when idle
        if (state == ConnectionState.CONNECTED && now - lastPingSentTime > config.pingInterval) {
            lastPingSentTime = now
            sendConnectedPacket(ConnectedPacket.ConnectedPing(now), Reliability.UNRELIABLE, RakNetPriority.IMMEDIATE)
        }

        // Connection timeout
        if (state.isActive && now - lastRecvTime > config.connectionTimeout) {
            log.warn("Connection to {} timed out after {} ms", remoteAddress, config.connectionTimeout)
            forceDisconnect(DisconnectReason.TIMED_OUT)
        }

        channel.flush()
    }

    // ── Inbound datagram ──────────────────────────────────────────────────────

    fun onDatagramReceived(ctx: ChannelHandlerContext, datagram: RakNetDatagram) {
        val now = System.currentTimeMillis()
        lastRecvTime = now
        when (datagram) {
            is RakNetDatagram.Ack  -> sendBuffer.onAck(datagram.ranges, now)
            is RakNetDatagram.Nak  -> sendBuffer.onNak(datagram.ranges)
            is RakNetDatagram.Data -> {
                sendBuffer.ackDatagramReceived(datagram.sequenceNumber)
                receiveBuffer.onDatagramArrived(datagram.sequenceNumber)
                    .forEach { (s, e) -> sendBuffer.nakRange(s, e) }
                var i = 0
                try {
                    while (i < datagram.frames.size) {
                        processFrame(ctx, datagram.frames[i], now)
                        i++
                    }
                } catch (t: Throwable) {
                    for (j in (i + 1) until datagram.frames.size) {
                        try {
                            datagram.frames[j].payload.close()
                        } catch (e: Exception) {
                            log.debug("Failed to release frame payload during exception recovery", e)
                        }
                    }
                    throw t
                }
            }
        }
    }

    private fun processFrame(ctx: ChannelHandlerContext, frame: RakNetFrame, now: Long) {
        // Reliable dedup
        if (frame.reliability.isReliable) {
            if (!receiveBuffer.isNewReliable(frame.reliableIndex)) {
                frame.payload.close()
                return
            }
            receiveBuffer.markReliableReceived(frame.reliableIndex)
        }

        // Fragment reassembly
        val payload: Buffer = if (frame.split != null) {
            val result = fragmentAccum.accumulate(frame, channel.bufferAllocator(), now)
            // DO NOT close frame.payload here; FragmentAccumulator has taken ownership!
            result ?: return
        } else {
            frame.payload
        }

        deliverPayload(ctx, frame, payload)
    }

    private fun deliverPayload(ctx: ChannelHandlerContext, frame: RakNetFrame, payload: Buffer) {
        when {
            frame.reliability.isOrdered -> {
                receiveBuffer.enqueueOrdered(frame.orderChannel, frame.orderIndex, payload)
                val drained = receiveBuffer.drainOrdered(frame.orderChannel)
                var i = 0
                try {
                    while (i < drained.size) {
                        val buf = drained[i]
                        if (!state.isActive) {
                            buf.close()
                        } else {
                            dispatchPayload(ctx, buf)
                        }
                        i++
                    }
                } finally {
                    for (j in i until drained.size) {
                        try {
                            drained[j].close()
                        } catch (e: Exception) {
                            log.debug("Failed to release drained buffer during exception recovery", e)
                        }
                    }
                }
            }
            frame.reliability.isSequenced -> {
                if (receiveBuffer.isNewerSequenced(frame.orderChannel, frame.sequenceIndex)) {
                    receiveBuffer.updateSequenced(frame.orderChannel, frame.sequenceIndex)
                    dispatchPayload(ctx, payload)
                } else {
                    payload.close()  // stale sequenced packet — drop
                }
            }
            else -> dispatchPayload(ctx, payload)
        }
    }

    private fun dispatchPayload(ctx: ChannelHandlerContext, payload: Buffer) {
        val id = payload.getUnsignedByte(payload.readerOffset()).toInt()
        if (id < 0x80) {
            // Internal RakNet message (0x00–0x7F)
            try {
                handleConnectedPacket(ctx, ConnectedPacketCodec.decode(payload))
            } finally {
                payload.close()
            }
        } else {
            // Application-layer message — hand off to the user's handler
            ctx.fireChannelRead(RakNetPacket(this, payload))
        }
    }

    private fun handleConnectedPacket(ctx: ChannelHandlerContext, packet: ConnectedPacket) {
        when (packet) {
            is ConnectedPacket.ConnectionRequest -> {
                if (state == ConnectionState.HANDSHAKING) {
                    val localAddr = channel.localAddress() as InetSocketAddress
                    val systemAddresses = List(ConnectedPacket.INTERNAL_ADDRESS_COUNT) { i ->
                        if (i == 0) remoteAddress else localAddr
                    }
                    val accepted = ConnectedPacket.ConnectionRequestAccepted(
                        clientAddress     = remoteAddress,
                        clientIndex       = 0,
                        systemAddresses   = systemAddresses,
                        requestTimestamp  = packet.requestTimestamp,
                        acceptedTimestamp = System.currentTimeMillis(),
                    )
                    sendConnectedPacket(accepted, Reliability.RELIABLE_ORDERED)
                    state = ConnectionState.CONNECTED
                    ctx.fireChannelInboundEvent(RakNetEvent.Connected(this))
                }
            }
            is ConnectedPacket.ConnectionRequestAccepted -> {
                if (state == ConnectionState.HANDSHAKING) {
                    val incoming = ConnectedPacket.NewIncomingConnection(
                        serverAddress     = remoteAddress,
                        internalAddresses = packet.systemAddresses.drop(1),
                        requestTimestamp  = packet.requestTimestamp,
                        acceptedTimestamp = packet.acceptedTimestamp,
                    )
                    sendConnectedPacket(incoming, Reliability.RELIABLE_ORDERED)
                    state = ConnectionState.CONNECTED
                    ctx.fireChannelInboundEvent(RakNetEvent.Connected(this))
                }
            }
            is ConnectedPacket.NewIncomingConnection -> {
                // Server already transitioned to CONNECTED when ConnectionRequest was accepted
                // (line 183). This is just a client confirmation — nothing to do unless we
                // somehow missed the state transition.
                if (state != ConnectionState.CONNECTED) {
                    state = ConnectionState.CONNECTED
                    ctx.fireChannelInboundEvent(RakNetEvent.Connected(this))
                }
            }
            ConnectedPacket.DisconnectionNotification -> {
                // The remote peer initiated the disconnect: from a server-side connection's
                // perspective this is CLIENT_REQUESTED; keep a single code path and let
                // callers interpret the reason relative to their role if needed.
                forceDisconnect(DisconnectReason.CLIENT_REQUESTED)
            }
            ConnectedPacket.DetectLostConnection -> {
                sendConnectedPacket(ConnectedPacket.DisconnectionNotification, Reliability.UNRELIABLE)
                forceDisconnect(DisconnectReason.TIMED_OUT)
            }
            is ConnectedPacket.ConnectedPing -> {
                sendConnectedPacket(
                    ConnectedPacket.ConnectedPong(System.currentTimeMillis(), packet.sendTimestamp),
                    Reliability.UNRELIABLE,
                    RakNetPriority.IMMEDIATE,
                )
            }
            is ConnectedPacket.ConnectedPong -> {
                ping = (System.currentTimeMillis() - packet.pingTimestamp).toInt().coerceAtLeast(0)
            }
        }
    }

    // ── Outbound ──────────────────────────────────────────────────────────────

    override fun send(
        payload: Buffer,
        reliability: Reliability,
        orderChannel: Int,
        priority: RakNetPriority,
    ): Future<Void> {
        if (!state.isActive) {
            payload.close()
            return channel.executor().newFailedFuture(ConnectionClosedException(DisconnectReason.INTERNAL_ERROR))
        }
        val promise: Promise<Void> = channel.newPromise()
        if (channel.executor().inEventLoop()) {
            doSend(payload, reliability, orderChannel, priority)
            promise.setSuccess(null)
        } else {
            channel.executor().execute {
                if (!state.isActive) {
                    payload.close()
                    promise.setFailure(ConnectionClosedException(DisconnectReason.INTERNAL_ERROR))
                    return@execute
                }
                doSend(payload, reliability, orderChannel, priority)
                promise.setSuccess(null)
            }
        }
        return promise as Future<Void>
    }

    private fun doSend(
        payload: Buffer,
        reliability: Reliability,
        orderChannel: Int,
        priority: RakNetPriority = RakNetPriority.NORMAL,
    ) {
        val threshold = mtu - config.fragmentOverhead - 28
        val orderIdx  = if (reliability.isOrdered)  sendBuffer.nextOrderIndex(orderChannel)   else 0
        val seqIdx    = if (reliability.isSequenced) sendBuffer.nextSequenceIndex(orderChannel) else 0

        if (priority == RakNetPriority.IMMEDIATE) {
            if (payload.readableBytes() <= threshold) {
                val frame = RakNetFrame(
                    reliability   = reliability,
                    reliableIndex = if (reliability.isReliable) sendBuffer.nextReliableIndex() else 0,
                    orderIndex    = orderIdx,
                    sequenceIndex = seqIdx,
                    orderChannel  = orderChannel,
                    payload       = payload,
                )
                val encoded = sendBuffer.sendImmediate(frame, System.currentTimeMillis(), channel.bufferAllocator())
                channel.writeAndFlush(DatagramPacket(encoded, remoteAddress))
            } else {
                splitAndSendImmediate(payload, reliability, orderChannel, orderIdx, seqIdx, threshold)
            }
        } else {
            if (payload.readableBytes() <= threshold) {
                sendBuffer.enqueue(RakNetFrame(
                    reliability   = reliability,
                    reliableIndex = if (reliability.isReliable) sendBuffer.nextReliableIndex() else 0,
                    orderIndex    = orderIdx,
                    sequenceIndex = seqIdx,
                    orderChannel  = orderChannel,
                    payload       = payload,
                ))
            } else {
                splitAndEnqueue(payload, reliability, orderChannel, orderIdx, seqIdx, threshold)
            }
        }
    }

    private fun splitAndEnqueue(
        payload: Buffer,
        reliability: Reliability,
        orderChannel: Int,
        orderIdx: Int,
        seqIdx: Int,
        chunkSize: Int,
    ) {
        val splitId    = (nextSplitId++) and 0xFFFF
        val totalLen   = payload.readableBytes()
        val splitCount = (totalLen + chunkSize - 1) / chunkSize
        var splitIndex = 0

        while (payload.readableBytes() > 0) {
            val len   = minOf(chunkSize, payload.readableBytes())
            val chunk = payload.readSplit(len)
            sendBuffer.enqueue(RakNetFrame(
                reliability   = reliability,
                reliableIndex = if (reliability.isReliable) sendBuffer.nextReliableIndex() else 0,
                orderIndex    = orderIdx,
                sequenceIndex = seqIdx,
                orderChannel  = orderChannel,
                split         = SplitInfo(splitId, splitCount, splitIndex++),
                payload       = chunk,
            ))
        }
        payload.close()
    }

    private fun splitAndSendImmediate(
        payload: Buffer,
        reliability: Reliability,
        orderChannel: Int,
        orderIdx: Int,
        seqIdx: Int,
        chunkSize: Int,
    ) {
        val splitId    = (nextSplitId++) and 0xFFFF
        val totalLen   = payload.readableBytes()
        val splitCount = (totalLen + chunkSize - 1) / chunkSize
        var splitIndex = 0
        val now        = System.currentTimeMillis()

        while (payload.readableBytes() > 0) {
            val len   = minOf(chunkSize, payload.readableBytes())
            val chunk = payload.readSplit(len)
            val frame = RakNetFrame(
                reliability   = reliability,
                reliableIndex = if (reliability.isReliable) sendBuffer.nextReliableIndex() else 0,
                orderIndex    = orderIdx,
                sequenceIndex = seqIdx,
                orderChannel  = orderChannel,
                split         = SplitInfo(splitId, splitCount, splitIndex++),
                payload       = chunk,
            )
            val encoded = sendBuffer.sendImmediate(frame, now, channel.bufferAllocator())
            channel.write(DatagramPacket(encoded, remoteAddress))
        }
        payload.close()
        channel.flush()
    }

    override fun disconnect(reason: DisconnectReason): Future<Void> {
        if (channel.executor().inEventLoop()) {
            doDisconnect(reason)
            return channel.executor().newSucceededFuture(null)
        }
        val promise: Promise<Void> = channel.newPromise()
        channel.executor().execute {
            doDisconnect(reason)
            promise.setSuccess(null)
        }
        return promise as Future<Void>
    }

    private fun doDisconnect(reason: DisconnectReason) {
        if (state == ConnectionState.CONNECTED) {
            // IMMEDIATE encodes and flushes directly — no manual flush needed before teardown.
            sendConnectedPacket(ConnectedPacket.DisconnectionNotification, Reliability.UNRELIABLE, RakNetPriority.IMMEDIATE)
        }
        forceDisconnect(reason)
    }

    private fun forceDisconnect(reason: DisconnectReason) {
        if (state == ConnectionState.DISCONNECTED) return
        log.info("Disconnecting {}: {}", remoteAddress, reason)
        state = ConnectionState.DISCONNECTED
        tickerFuture?.cancel()
        registry.remove(remoteAddress)
        sendBuffer.release()
        receiveBuffer.release()
        fragmentAccum.release()
        channel.pipeline().fireChannelInboundEvent(RakNetEvent.Disconnected(this, reason))
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun sendConnectedPacket(
        packet: ConnectedPacket,
        reliability: Reliability,
        priority: RakNetPriority = RakNetPriority.NORMAL,
    ) {
        val buf = ConnectedPacketCodec.encode(packet, channel.bufferAllocator())
        doSend(buf, reliability, 0, priority)
    }
}
