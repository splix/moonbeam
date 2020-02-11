package io.emeraldpay.polkadotcrawler.state

import io.libp2p.core.multiformats.Protocol
import java.net.InetAddress
import java.time.Instant

open class ProcessedPeerDetails(
        val timestamp: Instant = Instant.now()
) {

    var agent: Agent? = null
    var host: Host? = null

    class Host(
            val address: String,
            val hostname: String?,
            val ip: String?,
            val port: Int?
    ) {
        companion object {
            fun from(peer: PeerDetails): Host {
                val ip = if (peer.address.has(Protocol.IP4)) {
                    peer.address.getStringComponent(Protocol.IP4)
                } else if (peer.address.has(Protocol.IP6)) {
                    peer.address.getStringComponent(Protocol.IP6)
                } else {
                    peer.address.getStringComponent(Protocol.DNS4)?.let { host ->
                        InetAddress.getByName(host).hostAddress
                    }
                }
                val port = peer.address.getStringComponent(Protocol.TCP)?.toIntOrNull()
                return Host(
                        peer.address.toString(),
                        peer.address.getStringComponent(Protocol.DNS4),
                        ip,
                        port
                )
            }
        }
    }
}