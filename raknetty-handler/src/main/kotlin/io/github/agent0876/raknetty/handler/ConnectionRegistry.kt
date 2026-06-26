package io.github.agent0876.raknetty.handler

import io.github.agent0876.raknetty.handler.connection.RakNetConnectionImpl
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

/**
 * Thread-safe map from remote address to active [RakNetConnectionImpl].
 *
 * All per-connection operations (send, receive) happen on the EventLoop thread,
 * but the registry itself may be queried from other threads for management.
 */
class ConnectionRegistry {
    private val map = ConcurrentHashMap<InetSocketAddress, RakNetConnectionImpl>()

    fun add(conn: RakNetConnectionImpl) { map[conn.remoteAddress] = conn }
    fun get(address: InetSocketAddress): RakNetConnectionImpl? = map[address]
    fun remove(address: InetSocketAddress): RakNetConnectionImpl? = map.remove(address)

    val size: Int get() = map.size
    fun all(): Collection<RakNetConnectionImpl> = map.values
}
