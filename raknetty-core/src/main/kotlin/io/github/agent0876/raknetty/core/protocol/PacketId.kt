package io.github.agent0876.raknetty.core.protocol

object PacketId {

    // ── Offline (unconnected) ─────────────────────────────────────────────────
    const val UNCONNECTED_PING: Int                    = 0x01
    const val UNCONNECTED_PING_OPEN_CONNECTIONS: Int   = 0x02
    const val UNCONNECTED_PONG: Int                    = 0x1C
    const val OPEN_CONNECTION_REQUEST_1: Int           = 0x05
    const val OPEN_CONNECTION_REPLY_1: Int             = 0x06
    const val OPEN_CONNECTION_REQUEST_2: Int           = 0x07
    const val OPEN_CONNECTION_REPLY_2: Int             = 0x08
    const val INCOMPATIBLE_PROTOCOL_VERSION: Int       = 0x19
    const val IP_RECENTLY_CONNECTED: Int               = 0x1A

    // ── Connection handshake ─────────────────────────────────────────────────
    const val CONNECTION_REQUEST: Int                  = 0x09
    const val CONNECTION_REQUEST_ACCEPTED: Int         = 0x10
    const val NEW_INCOMING_CONNECTION: Int             = 0x13
    const val NO_FREE_INCOMING_CONNECTIONS: Int        = 0x14
    const val ALREADY_CONNECTED: Int                   = 0x11
    const val CONNECTION_ATTEMPT_FAILED: Int           = 0x17
    const val CONNECTION_BANNED: Int                   = 0x1B
    const val INVALID_PASSWORD: Int                    = 0x18

    // ── Connected lifecycle ──────────────────────────────────────────────────
    const val DISCONNECTION_NOTIFICATION: Int          = 0x15
    const val DETECT_LOST_CONNECTION: Int              = 0x04
    const val CONNECTED_PING: Int                      = 0x00
    const val CONNECTED_PONG: Int                      = 0x03

    // ── Datagram header flags (first byte of every RakNet UDP payload) ───────
    /** Must be set on all outgoing RakNet datagrams. */
    const val FLAG_VALID: Int           = 0x80  // 1000 0000
    const val FLAG_ACK: Int             = 0x40  // 0100 0000  → ACK  = 0xC0
    const val FLAG_NAK: Int             = 0x20  // 0010 0000  → NAK  = 0xA0
    const val FLAG_PACKET_PAIR: Int     = 0x10  // for bandwidth estimation
    const val FLAG_CONTINUOUS_SEND: Int = 0x08
    const val FLAG_NEEDS_B_AND_AS: Int  = 0x04

    /**
     * 16-byte magic that follows the message_id in every offline packet.
     * Lets the server filter stray UDP traffic from non-RakNet clients.
     */
    @JvmField
    val OFFLINE_MESSAGE_ID: ByteArray = byteArrayOf(
        0x00, 0xFF.toByte(), 0xFF.toByte(), 0x00,
        0xFE.toByte(), 0xFE.toByte(), 0xFE.toByte(), 0xFE.toByte(),
        0xFD.toByte(), 0xFD.toByte(), 0xFD.toByte(), 0xFD.toByte(),
        0x12, 0x34, 0x56, 0x78,
    )
}
