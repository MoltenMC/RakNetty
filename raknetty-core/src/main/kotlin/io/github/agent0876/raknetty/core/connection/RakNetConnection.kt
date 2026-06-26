package io.github.agent0876.raknetty.core.connection

import io.github.agent0876.raknetty.core.protocol.RakNetPriority
import io.github.agent0876.raknetty.core.protocol.Reliability
import io.netty5.buffer.Buffer
import io.netty5.channel.Channel
import io.netty5.util.concurrent.Future
import java.net.InetSocketAddress

/**
 * A virtual RakNet connection multiplexed over a shared [NioDatagramChannel].
 *
 * Multiple [RakNetConnection]s may share the same physical UDP socket, each
 * identified by its [remoteAddress]. All operations are non-blocking and must
 * be called from the owning [channel]'s EventLoop unless otherwise noted.
 */
interface RakNetConnection {

    val remoteAddress: InetSocketAddress

    /** 64-bit GUID assigned by the remote peer during the OCReq2/OCReply2 handshake. */
    val guid: Long

    /** MTU negotiated during the Open Connection handshake. */
    val mtu: Int

    val state: ConnectionState

    /** The shared underlying [NioDatagramChannel] this connection is multiplexed onto. */
    val channel: Channel

    /** Smoothed round-trip time in milliseconds, updated on each ACK. */
    val ping: Int

    /**
     * Sends [payload] with the given [reliability] on [orderChannel].
     *
     * The reliability layer takes ownership of [payload] — callers must NOT release it
     * after this call.
     *
     * [priority] controls dispatch:
     * - [RakNetPriority.IMMEDIATE] — encoded and written to the channel in this call,
     *   bypassing the send queue and congestion window. Use for time-sensitive
     *   control messages (ping, disconnect notification).
     * - [RakNetPriority.NORMAL] — queued and flushed on the next tick, subject to
     *   AIMD flow control. Default for all application data.
     *
     * @return a future that completes when the data is handed off to the send pipeline,
     *         NOT when the remote end has acknowledged it.
     */
    fun send(
        payload: Buffer,
        reliability: Reliability,
        orderChannel: Int = 0,
        priority: RakNetPriority = RakNetPriority.NORMAL,
    ): Future<Void>

    /**
     * Initiates a clean disconnect by sending a DISCONNECTION_NOTIFICATION,
     * then closes the connection after the send completes.
     */
    fun disconnect(reason: DisconnectReason = DisconnectReason.CLIENT_REQUESTED): Future<Void>
}
