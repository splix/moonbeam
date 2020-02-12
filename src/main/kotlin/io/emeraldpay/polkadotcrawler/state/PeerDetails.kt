package io.emeraldpay.polkadotcrawler.state

import identify.pb.IdentifyOuterClass
import io.emeraldpay.polkadotcrawler.proto.Dht
import io.libp2p.core.PeerId
import io.libp2p.core.multiformats.Multiaddr
import io.libp2p.core.multiformats.Protocol
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import java.lang.StringBuilder
import java.net.InetAddress

class PeerDetails(
        val address: Multiaddr
) {

    companion object {
        private val log = LoggerFactory.getLogger(PeerDetails::class.java)
    }

    var agent: String? = null
    var peers: Int = 0
        private set
    var peerId: PeerId? = null

    private lateinit var host: InetAddress
    private var port: Int? = null

    init {
        listOf(Protocol.IP4, Protocol.IP6, Protocol.DNS4).find { protocol ->
            address.has(protocol)
        }?.let { protocol ->
            host = InetAddress.getByName(address.getStringComponent(protocol))
        }
        if (address.has(Protocol.TCP)) {
            port = address.getStringComponent(Protocol.TCP)?.toIntOrNull()
        }
        // setup default PeerId, maybe replaced later after establishing Secio
        address.getStringComponent(Protocol.P2P)?.let {
            peerId = PeerId.fromBase58(it)
        }
    }

    fun add(dht: Dht.Message) {
        peers += dht.closerPeersCount
    }

    fun add(id: IdentifyOuterClass.Identify) {
        agent = id.agentVersion
    }

    fun dump() {
        if (!filled()) {
            return
        }
        val buf = StringBuilder()
        buf.append("Peer ${host}:${port} uses ${agent}")
        if (peers > 0) {
            buf.append(", knows at least ${peers} peers")
        }
        log.info(buf.toString())
    }

    fun filled(): Boolean {
        return StringUtils.isNoneEmpty(agent)
    }
}