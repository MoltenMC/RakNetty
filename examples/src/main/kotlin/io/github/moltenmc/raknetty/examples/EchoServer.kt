package io.github.moltenmc.raknetty.examples

import io.github.moltenmc.raknetty.core.connection.DisconnectReason
import io.github.moltenmc.raknetty.core.connection.RakNetConnection
import io.github.moltenmc.raknetty.core.protocol.Reliability
import io.github.moltenmc.raknetty.transport.RakNetServerBootstrap
import io.github.moltenmc.raknetty.transport.SimpleRakNetHandler
import io.netty5.buffer.Buffer
import io.netty5.channel.ChannelHandlerContext
import kotlin.random.Random

fun main() {
    val serverGuid = Random.nextLong()

    val future = RakNetServerBootstrap()
        .serverGuid(serverGuid)
        // Minecraft Bedrock 스펙에 필요한 서버 정보 문자열 지정
        .serverInfo { "MCPE;My Dedicated Server;594;1.20.80;0;20;${serverGuid};Survival;1;19132;19133;" }
        .maxConnections(50)
        .handler(object : SimpleRakNetHandler() {
            override fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {
                println("[Server] 새로운 클라이언트 접속 완료: ${connection.remoteAddress}")
            }

            override fun onMessage(ctx: ChannelHandlerContext, connection: RakNetConnection, payload: Buffer) {
                val readable = payload.readableBytes()
                println("[Server] ${connection.remoteAddress}로부터 ${readable} 바이트 수신 완료.")

                // Echo back: 송신할 때 버퍼 복사본을 생성해 보냄 (payload는 콜백 반환 후 자동 close되므로)
                val echoPayload = payload.copy()
                connection.send(echoPayload, Reliability.RELIABLE_ORDERED)
            }

            override fun onDisconnect(ctx: ChannelHandlerContext, connection: RakNetConnection, reason: DisconnectReason) {
                println("[Server] 클라이언트 연결 종료: ${connection.remoteAddress} (이유: $reason)")
            }
        })
        .bind(19132)
        .asStage().sync()

    val channel = future.getNow()
    println("[Server] RakNet UDP 서버 바인딩 성공 (Port: ${(channel.localAddress() as java.net.InetSocketAddress).port})")
    channel.closeFuture().asStage().sync()
}
