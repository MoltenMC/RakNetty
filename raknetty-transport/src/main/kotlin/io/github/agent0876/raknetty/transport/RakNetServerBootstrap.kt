package io.github.agent0876.raknetty.transport

import io.github.agent0876.raknetty.handler.ConnectionRegistry
import io.github.agent0876.raknetty.handler.DatagramDispatcher
import io.github.agent0876.raknetty.handler.connection.ConnectionConfig
import io.github.agent0876.raknetty.handler.handshake.ServerHandshakeHandler
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelHandler
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.nio.NioDatagramChannel
import java.net.InetSocketAddress

/**
 * Fluent builder for a RakNet server.
 *
 * Usage:
 * ```kotlin
 * val future = RakNetServerBootstrap()
 *     .group(NioEventLoopGroup())
 *     .serverGuid(Random.nextLong())
 *     .serverInfo { "MCPE;My Server;594;1.20.0;0;20" }
 *     .maxConnections(20)
 *     .handler(MyServerHandler())
 *     .bind(19132)
 *     .sync()
 * ```
 *
 * If [group] is not provided a default [NioEventLoopGroup] is created. In that case
 * the caller is responsible for shutting it down via [ChannelFuture.channel] → `closeFuture`.
 */
class RakNetServerBootstrap {

    private var group: EventLoopGroup?        = null
    private var serverGuid: Long              = System.nanoTime()
    private var serverInfo: () -> String      = { "" }
    private var maxConnections: Int           = 20
    private var config: ConnectionConfig      = ConnectionConfig()
    private var handler: ChannelHandler?      = null

    fun group(group: EventLoopGroup)              = apply { this.group = group }
    fun serverGuid(guid: Long)                    = apply { serverGuid = guid }
    fun serverInfo(provider: () -> String)        = apply { serverInfo = provider }
    fun maxConnections(max: Int)                  = apply { maxConnections = max }
    fun connectionConfig(config: ConnectionConfig) = apply { this.config = config }

    /**
     * Sets the application-layer handler added last in the server pipeline.
     * Receives [io.github.agent0876.raknetty.handler.event.RakNetPacket] channel-reads and
     * [io.github.agent0876.raknetty.handler.event.RakNetEvent] user events.
     * Extend [SimpleRakNetHandler] for a convenient callback-based API.
     */
    fun handler(handler: ChannelHandler)           = apply { this.handler = handler }

    fun bind(port: Int): ChannelFuture             = bind(InetSocketAddress(port))

    fun bind(address: InetSocketAddress): ChannelFuture {
        val registry         = ConnectionRegistry()
        val dispatcher       = DatagramDispatcher(registry)
        val handshakeHandler = ServerHandshakeHandler(
            registry       = registry,
            serverGuid     = serverGuid,
            serverInfo     = serverInfo,
            config         = config,
            maxConnections = maxConnections,
        )
        val userHandler = requireNotNull(handler) { "handler() must be set before bind()" }

        return Bootstrap()
            .group(group ?: MultiThreadIoEventLoopGroup(0, NioIoHandler.newFactory()))
            .channel(NioDatagramChannel::class.java)
            .option(ChannelOption.SO_BROADCAST, true)
            .option(ChannelOption.SO_REUSEADDR,  true)
            .handler(object : ChannelInitializer<NioDatagramChannel>() {
                override fun initChannel(ch: NioDatagramChannel) {
                    ch.pipeline()
                        .addLast("dispatcher", dispatcher)
                        .addLast("handshake",   handshakeHandler)
                        .addLast("user",        userHandler)
                }
            })
            .bind(address)
    }
}
