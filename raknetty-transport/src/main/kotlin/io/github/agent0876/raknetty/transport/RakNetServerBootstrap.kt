package io.github.agent0876.raknetty.transport

import io.github.agent0876.raknetty.handler.ConnectionRegistry
import io.github.agent0876.raknetty.handler.DatagramDispatcher
import io.github.agent0876.raknetty.handler.connection.ConnectionConfig
import io.github.agent0876.raknetty.handler.handshake.ServerHandshakeHandler
import io.netty5.bootstrap.Bootstrap
import io.netty5.channel.Channel
import io.netty5.channel.ChannelHandler
import io.netty5.channel.ChannelInitializer
import io.netty5.channel.ChannelOption
import io.netty5.channel.EventLoopGroup
import io.netty5.channel.MultithreadEventLoopGroup
import io.netty5.channel.nio.NioHandler
import io.netty5.channel.socket.nio.NioDatagramChannel
import io.netty5.util.concurrent.Future
import java.net.InetSocketAddress

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

    fun handler(handler: ChannelHandler)           = apply { this.handler = handler }

    fun bind(port: Int): Future<Channel>             = bind(InetSocketAddress(port))

    fun bind(address: InetSocketAddress): Future<Channel> {
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
            .group(group ?: MultithreadEventLoopGroup(0, NioHandler.newFactory()))
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
