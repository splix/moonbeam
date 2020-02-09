package io.emeraldpay.polkadotcrawler.export.json

import io.emeraldpay.polkadotcrawler.state.PeerDetails
import io.libp2p.core.multiformats.Protocol
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.time.Instant

/**
 * JSON representation of Peer Details
 */
class PeerDetailsJson() {

    companion object {
        private val log = LoggerFactory.getLogger(PeerDetailsJson::class.java)

        fun from(peer: PeerDetails): PeerDetailsJson {
            val result = PeerDetailsJson()
            result.agent = Agent.from(peer)
            result.host = Host.from(peer)
            return result
        }
    }

    val version: String = "https://schema.emeraldpay.io/polkadbot#v0"
    val timestamp: Instant = Instant.now()

    var agent: Agent? = null
    var host: Host? = null

    class Agent(
            val full: String
    ) {
        companion object {
            fun from(peer: PeerDetails): Agent? {
                return peer.agent?.let {
                    Agent(it)
                }
            }
        }
    }

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