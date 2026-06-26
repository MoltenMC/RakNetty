package io.github.agent0876.raknetty.transport

import io.github.agent0876.raknetty.handler.ConnectionRegistry
import io.github.agent0876.raknetty.handler.DatagramDispatcher
import io.github.agent0876.raknetty.handler.connection.ConnectionConfig
import io.github.agent0876.raknetty.handler.event.RakNetEvent
import io.github.agent0876.raknetty.handler.handshake.ClientHandshakeHandler
import io.github.agent0876.raknetty.core.protocol.RakNetProtocol
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.EventLoopGroup
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioDatagramChannel
import java.net.ConnectException
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

/**
 * Fluent builder for a RakNet client connection.
 *
 * Usage:
 * ```kotlin
 * val future = RakNetClientBootstrap()
 *     .group(NioEventLoopGroup(1))
 *     .clientGuid(Random.nextLong())
 *     .handler(MyClientHandler())
 *     .connect("play.example.com", 19132)
 *
 * future.sync()  // waits until ConnectionRequestAccepted is received
 * ```
 *
 * The returned [ChannelFuture] is a promise that completes **after the full
 * RakNet handshake** (OCReq1/2 + ConnectionRequest/Accepted), not just after
 * the UDP socket is bound.
 */
class RakNetClientBootstrap {

    private var group: EventLoopGroup?        = null
    private var clientGuid: Long              = System.nanoTime()
    private var config: ConnectionConfig      = ConnectionConfig()
    private var handler: ChannelHandler?      = null
    private var mtu: Int                      = RakNetProtocol.MTU_SIZES[0]

    fun group(group: EventLoopGroup)               = apply { this.group = group }
    fun clientGuid(guid: Long)                     = apply { clientGuid = guid }
    fun connectionConfig(config: ConnectionConfig)  = apply { this.config = config }
    fun mtu(mtu: Int)                              = apply { this.mtu = mtu }

    /**
     * Application-layer handler. Receives [io.github.agent0876.raknetty.handler.event.RakNetPacket]
     * channel-reads and [io.github.agent0876.raknetty.handler.event.RakNetEvent] user events.
     * Extend [SimpleRakNetHandler] for convenience.
     */
    fun handler(handler: ChannelHandler) = apply { this.handler = handler }

    fun connect(host: String, port: Int): ChannelFuture = connect(InetSocketAddress(host, port))

    fun connect(address: InetSocketAddress): ChannelFuture {
        val registry         = ConnectionRegistry()
        val dispatcher       = DatagramDispatcher(registry)
        val handshakeHandler = ClientHandshakeHandler(registry, clientGuid, config)
        val userHandler      = requireNotNull(handler) { "handler() must be set before connect()" }

        val ownedGroup   = group == null
        val resolvedGroup = group ?: MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())

        // Bind a local UDP socket (port 0 = OS-assigned ephemeral port)
        val channel = Bootstrap()
            .group(resolvedGroup)
            .channel(NioDatagramChannel::class.java)
            .handler(object : ChannelInitializer<NioDatagramChannel>() {
                override fun initChannel(ch: NioDatagramChannel) {
                    ch.pipeline()
                        .addLast("dispatcher", dispatcher)
                        .addLast("handshake",   handshakeHandler)
                        .addLast("user",        userHandler)
                }
            })
            .bind(0).sync().channel()

        val connectPromise = channel.newPromise()

        // Intercept the first Connected event to resolve connectPromise, then remove itself.
        channel.pipeline().addBefore("user", "connect-resolver",
            object : ChannelInboundHandlerAdapter() {
                override fun userEventTriggered(ctx: ChannelHandlerContext, evt: Any) {
                    if (evt is RakNetEvent.Connected && !connectPromise.isDone) {
                        // Propagate first while the context is still in the pipeline, then
                        // mark success and remove. Removing before firing can leave ctx.next
                        // invalid in Netty 4.2, making the subsequent fire a silent no-op.
                        ctx.fireUserEventTriggered(evt)
                        connectPromise.setSuccess()
                        ctx.pipeline().remove(this)
                        return
                    }
                    ctx.fireUserEventTriggered(evt)
                }
            }
        )

        // Fail the promise if the handshake does not complete within connectionTimeout
        channel.eventLoop().schedule({
            if (!connectPromise.isDone) {
                connectPromise.setFailure(ConnectException("RakNet handshake timed out after ${config.connectionTimeout} ms"))
                channel.close()
                if (ownedGroup) resolvedGroup.shutdownGracefully()
            }
        }, config.connectionTimeout, TimeUnit.MILLISECONDS)

        // Kick off the handshake from inside the EventLoop so ctx is valid
        channel.eventLoop().execute {
            handshakeHandler.connect(
                channel.pipeline().context(handshakeHandler),
                address,
                mtu,
            )
        }

        return connectPromise
    }
}
