package io.github.agent0876.raknetty.handler.connection

import io.github.agent0876.raknetty.codec.RakNetFrameCodec
import io.github.agent0876.raknetty.codec.connected.ConnectedPacket
import io.github.agent0876.raknetty.codec.connected.ConnectedPacketCodec
import io.github.agent0876.raknetty.core.connection.ConnectionState
import io.github.agent0876.raknetty.core.connection.DisconnectReason
import io.github.agent0876.raknetty.core.connection.RakNetConnection
import io.github.agent0876.raknetty.core.protocol.RakNetPriority
import io.github.agent0876.raknetty.core.exception.ConnectionClosedException
import io.github.agent0876.raknetty.core.packet.RakNetDatagram
import io.github.agent0876.raknetty.core.packet.RakNetFrame
import io.github.agent0876.raknetty.core.packet.SplitInfo
import io.github.agent0876.raknetty.core.protocol.Reliability
import io.github.agent0876.raknetty.core.util.SequenceNumber
import io.github.agent0876.raknetty.handler.ConnectionRegistry
import io.github.agent0876.raknetty.handler.event.RakNetEvent
import io.github.agent0876.raknetty.handler.event.RakNetPacket
import io.github.agent0876.raknetty.handler.reliability.FragmentAccumulator
import io.github.agent0876.raknetty.handler.reliability.ReceiveBuffer
import io.github.agent0876.raknetty.handler.reliability.SendBuffer
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.socket.DatagramPacket
import java.net.InetSocketAddress
import java.util.concurrent.ScheduledFuture
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

    private val sendBuffer       = SendBuffer(mtu, config)
    private val receiveBuffer    = ReceiveBuffer()
    private val fragmentAccum    = FragmentAccumulator()

    private var lastRecvTime     = System.currentTimeMillis()
    private var lastPingSentTime = 0L
    private var nextSplitId      = 0
    private var tickerFuture: ScheduledFuture<*>? = null

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    /**
     * Schedules the periodic tick on the [channel]'s EventLoop.
     * Must be called once after the connection is added to [ConnectionRegistry].
     */
    fun startTicker() {
        tickerFuture = channel.eventLoop().scheduleAtFixedRate(
            { if (state != ConnectionState.DISCONNECTED) tick() },
            config.tickIntervalMs,
            config.tickIntervalMs,
            TimeUnit.MILLISECONDS,
        )
    }

    private fun tick() {
        val now  = System.currentTimeMillis()
        val alloc = channel.alloc()

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
                    ?.let { (s, e) -> sendBuffer.nakRange(s, e) }
                for (frame in datagram.frames) processFrame(ctx, frame)
            }
        }
    }

    private fun processFrame(ctx: ChannelHandlerContext, frame: RakNetFrame) {
        // Reliable dedup
        if (frame.reliability.isReliable) {
            if (!receiveBuffer.isNewReliable(frame.reliableIndex)) {
                frame.payload.release()
                return
            }
            receiveBuffer.markReliableReceived(frame.reliableIndex)
        }

        // Fragment reassembly
        val payload: ByteBuf = if (frame.split != null) {
            val result = fragmentAccum.accumulate(frame, channel.alloc())
            frame.payload.release()
            result ?: return
        } else {
            frame.payload
        }

        deliverPayload(ctx, frame, payload)
    }

    private fun deliverPayload(ctx: ChannelHandlerContext, frame: RakNetFrame, payload: ByteBuf) {
        when {
            frame.reliability.isOrdered -> {
                receiveBuffer.enqueueOrdered(frame.orderChannel, frame.orderIndex, payload)
                receiveBuffer.drainOrdered(frame.orderChannel).forEach { dispatchPayload(ctx, it) }
            }
            frame.reliability.isSequenced -> {
                if (receiveBuffer.isNewerSequenced(frame.orderChannel, frame.sequenceIndex)) {
                    receiveBuffer.updateSequenced(frame.orderChannel, frame.sequenceIndex)
                    dispatchPayload(ctx, payload)
                } else {
                    payload.release()  // stale sequenced packet — drop
                }
            }
            else -> dispatchPayload(ctx, payload)
        }
    }

    private fun dispatchPayload(ctx: ChannelHandlerContext, payload: ByteBuf) {
        val id = payload.getUnsignedByte(payload.readerIndex()).toInt()
        if (id < 0x80) {
            // Internal RakNet message (0x00–0x7F)
            try {
                handleConnectedPacket(ctx, ConnectedPacketCodec.decode(payload))
            } finally {
                payload.release()
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
                    val accepted = ConnectedPacket.ConnectionRequestAccepted(
                        clientAddress     = remoteAddress,
                        clientIndex       = 0,
                        systemAddresses   = emptyList(),
                        requestTimestamp  = packet.requestTimestamp,
                        acceptedTimestamp = System.currentTimeMillis(),
                    )
                    sendConnectedPacket(accepted, Reliability.RELIABLE_ORDERED)
                    state = ConnectionState.CONNECTED
                    ctx.fireUserEventTriggered(RakNetEvent.Connected(this))
                }
            }
            is ConnectedPacket.ConnectionRequestAccepted -> {
                if (state == ConnectionState.HANDSHAKING) {
                    val incoming = ConnectedPacket.NewIncomingConnection(
                        serverAddress     = remoteAddress,
                        internalAddresses = emptyList(),
                        requestTimestamp  = packet.requestTimestamp,
                        acceptedTimestamp = packet.acceptedTimestamp,
                    )
                    sendConnectedPacket(incoming, Reliability.RELIABLE_ORDERED)
                    state = ConnectionState.CONNECTED
                    ctx.fireUserEventTriggered(RakNetEvent.Connected(this))
                }
            }
            is ConnectedPacket.NewIncomingConnection -> { /* server-side: connection already CONNECTED */ }
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
        payload: ByteBuf,
        reliability: Reliability,
        orderChannel: Int,
        priority: RakNetPriority,
    ): ChannelFuture {
        if (!state.isActive) {
            payload.release()
            return channel.newFailedFuture(ConnectionClosedException(DisconnectReason.INTERNAL_ERROR))
        }
        val promise = channel.newPromise()
        if (channel.eventLoop().inEventLoop()) {
            doSend(payload, reliability, orderChannel, priority)
            promise.setSuccess()
        } else {
            channel.eventLoop().execute {
                doSend(payload, reliability, orderChannel, priority)
                promise.setSuccess()
            }
        }
        return promise
    }

    private fun doSend(
        payload: ByteBuf,
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
                val encoded = sendBuffer.sendImmediate(frame, System.currentTimeMillis(), channel.alloc())
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
        payload: ByteBuf,
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
        var offset     = payload.readerIndex()

        while (offset < payload.writerIndex()) {
            val len   = minOf(chunkSize, payload.writerIndex() - offset)
            val chunk = payload.retainedSlice(offset, len)
            offset += len
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
        payload.release()
    }

    private fun splitAndSendImmediate(
        payload: ByteBuf,
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
        var offset     = payload.readerIndex()
        val now        = System.currentTimeMillis()

        while (offset < payload.writerIndex()) {
            val len   = minOf(chunkSize, payload.writerIndex() - offset)
            val chunk = payload.retainedSlice(offset, len)
            offset += len
            val frame = RakNetFrame(
                reliability   = reliability,
                reliableIndex = if (reliability.isReliable) sendBuffer.nextReliableIndex() else 0,
                orderIndex    = orderIdx,
                sequenceIndex = seqIdx,
                orderChannel  = orderChannel,
                split         = SplitInfo(splitId, splitCount, splitIndex++),
                payload       = chunk,
            )
            val encoded = sendBuffer.sendImmediate(frame, now, channel.alloc())
            channel.write(DatagramPacket(encoded, remoteAddress))
        }
        payload.release()
        channel.flush()
    }

    override fun disconnect(reason: DisconnectReason): ChannelFuture {
        if (state == ConnectionState.CONNECTED) {
            // IMMEDIATE encodes and flushes directly — no manual flush needed before teardown.
            sendConnectedPacket(ConnectedPacket.DisconnectionNotification, Reliability.UNRELIABLE, RakNetPriority.IMMEDIATE)
        }
        forceDisconnect(reason)
        return channel.newSucceededFuture()
    }

    private fun forceDisconnect(reason: DisconnectReason) {
        if (state == ConnectionState.DISCONNECTED) return
        state = ConnectionState.DISCONNECTED
        tickerFuture?.cancel(false)
        registry.remove(remoteAddress)
        sendBuffer.release()
        receiveBuffer.release()
        fragmentAccum.release()
        channel.pipeline().fireUserEventTriggered(RakNetEvent.Disconnected(this, reason))
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun sendConnectedPacket(
        packet: ConnectedPacket,
        reliability: Reliability,
        priority: RakNetPriority = RakNetPriority.NORMAL,
    ) {
        val buf = ConnectedPacketCodec.encode(packet, channel.alloc())
        doSend(buf, reliability, 0, priority)
    }
}
