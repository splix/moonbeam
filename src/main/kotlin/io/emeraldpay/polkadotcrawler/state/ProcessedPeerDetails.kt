package io.emeraldpay.polkadotcrawler.state

import io.libp2p.core.multiformats.Multiaddr
import io.libp2p.core.multiformats.Protocol
import java.net.InetAddress
import java.time.Instant

open class ProcessedPeerDetails(
        val address: Multiaddr,
        val timestamp: Instant = Instant.now()
) {

    var peerId: String? = null
    var agent: Agent? = null
    var host: Host? = null
    var connection: ConnectionDetails? = null
    var blockchain: Blockchain? = null
    var protocols: Protocols? = null

    class Host(
            val address: String,
            val hostname: String?,
            val ip: String?,
            val port: Int?,
            val type: String?
    ) {
        companion object {
            fun from(peer: PeerDetails): Host {
                var type: String? = null
                val ip = if (peer.address.has(Protocol.IP4)) {
                    type = "IP"
                    peer.address.getStringComponent(Protocol.IP4)
                } else if (peer.address.has(Protocol.IP6)) {
                    type = "IP"
                    peer.address.getStringComponent(Protocol.IP6)
                } else {
                    peer.address.getStringComponent(Protocol.DNS4)?.let { host ->
                        type = "DNS"
                        InetAddress.getByName(host).hostAddress
                    }
                }
                val port = peer.address.getStringComponent(Protocol.TCP)?.toIntOrNull()
                return Host(
                        peer.address.toString(),
                        peer.address.getStringComponent(Protocol.DNS4),
                        ip,
                        port,
                        type
                )
            }
        }
    }
}