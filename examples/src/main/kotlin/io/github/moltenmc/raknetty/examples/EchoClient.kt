package io.github.moltenmc.raknetty.examples

import io.github.moltenmc.raknetty.core.connection.DisconnectReason
import io.github.moltenmc.raknetty.core.connection.RakNetConnection
import io.github.moltenmc.raknetty.core.protocol.Reliability
import io.github.moltenmc.raknetty.transport.RakNetClientBootstrap
import io.github.moltenmc.raknetty.transport.SimpleRakNetHandler
import io.netty5.buffer.Buffer
import io.netty5.channel.ChannelHandlerContext
import java.nio.charset.StandardCharsets
import kotlin.random.Random

fun main() {
    val connectFuture = RakNetClientBootstrap()
        .clientGuid(Random.nextLong())
        .handler(object : SimpleRakNetHandler() {
            override fun onConnect(ctx: ChannelHandlerContext, connection: RakNetConnection) {
                println("[Client] 서버 연결에 성공했습니다! (지연 속도(RTT): ${connection.ping} ms)")

                // 문자열 버퍼 생성 및 데이터 전송
                val allocator = ctx.bufferAllocator()
                val messageBytes = "Hello RakNet Server!".toByteArray(StandardCharsets.UTF_8)
                val buffer = allocator.allocate(messageBytes.size)
                buffer.writeBytes(messageBytes)

                // connection.send 호출 시 버퍼의 소유권은 라이브러리로 전관됩니다.
                connection.send(buffer, Reliability.RELIABLE_ORDERED)
                println("[Client] 메시지 전송 성공: 'Hello RakNet Server!'")
            }

            override fun onMessage(ctx: ChannelHandlerContext, connection: RakNetConnection, payload: Buffer) {
                val readable = payload.readableBytes()
                val bytes = ByteArray(readable)
                payload.readBytes(bytes, 0, readable)
                val responseMsg = String(bytes, StandardCharsets.UTF_8)
                println("[Client] 서버로부터 응답 수신: '$responseMsg'")

                // 응답을 받았으므로 연결 종료 진행
                println("[Client] 요청 처리가 완료되어 연결을 종료합니다.")
                connection.disconnect()
            }

            override fun onDisconnect(ctx: ChannelHandlerContext, connection: RakNetConnection, reason: DisconnectReason) {
                println("[Client] 연결이 해제되었습니다. (이유: $reason)")
            }
        })
        .connect("127.0.0.1", 19132)

    // 핸드셰이크가 완성될 때까지 차단식 대기 진행
    connectFuture.asStage().sync()
    println("[Client] 핸드셰이크 절차 완료!")
}
