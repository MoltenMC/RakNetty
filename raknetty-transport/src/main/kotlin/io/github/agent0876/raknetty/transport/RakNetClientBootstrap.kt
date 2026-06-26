package io.github.agent0876.raknetty.transport

import io.github.agent0876.raknetty.handler.ConnectionRegistry
import io.github.agent0876.raknetty.handler.DatagramDispatcher
import io.github.agent0876.raknetty.handler.connection.ConnectionConfig
import io.github.agent0876.raknetty.handler.event.RakNetEvent
import io.github.agent0876.raknetty.handler.handshake.ClientHandshakeHandler
import io.github.agent0876.raknetty.core.protocol.RakNetProtocol
import io.netty5.bootstrap.Bootstrap
import io.netty5.channel.Channel
import io.netty5.channel.ChannelHandler
import io.netty5.channel.ChannelHandlerAdapter
import io.netty5.channel.ChannelHandlerContext
import io.netty5.channel.ChannelInitializer
import io.netty5.channel.EventLoopGroup
import io.netty5.channel.MultithreadEventLoopGroup
import io.netty5.channel.nio.NioHandler
import io.netty5.channel.socket.nio.NioDatagramChannel
import io.netty5.util.concurrent.Future
import io.netty5.util.concurrent.Promise
import java.net.ConnectException
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

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

    fun handler(handler: ChannelHandler) = apply { this.handler = handler }

    fun connect(host: String, port: Int): Future<Void> = connect(InetSocketAddress(host, port))

    fun connect(address: InetSocketAddress): Future<Void> {
        val registry         = ConnectionRegistry()
        val dispatcher       = DatagramDispatcher(registry)
        val handshakeHandler = ClientHandshakeHandler(registry, clientGuid, config)
        val userHandler      = requireNotNull(handler) { "handler() must be set before connect()" }

        val ownedGroup   = group == null
        val resolvedGroup = group ?: MultithreadEventLoopGroup(1, NioHandler.newFactory())

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
            .bind(0).asStage().sync().getNow()

        val connectPromise: Promise<Void> = channel.newPromise()

        channel.pipeline().addBefore("user", "connect-resolver",
            object : ChannelHandlerAdapter() {
                override fun channelInboundEvent(ctx: ChannelHandlerContext, evt: Any) {
                    if (evt is RakNetEvent.Connected && !connectPromise.isDone) {
                        ctx.fireChannelInboundEvent(evt)
                        connectPromise.setSuccess(null)
                        ctx.pipeline().remove(this)
                        return
                    }
                    ctx.fireChannelInboundEvent(evt)
                }
            }
        )

        channel.executor().schedule({
            if (!connectPromise.isDone) {
                connectPromise.setFailure(ConnectException("RakNet handshake timed out after ${config.connectionTimeout} ms"))
                channel.close()
                if (ownedGroup) resolvedGroup.shutdownGracefully()
            }
        }, config.connectionTimeout, TimeUnit.MILLISECONDS)

        channel.executor().execute {
            handshakeHandler.connect(
                channel.pipeline().context(handshakeHandler),
                address,
                mtu,
            )
        }

        return connectPromise as Future<Void>
    }
}
