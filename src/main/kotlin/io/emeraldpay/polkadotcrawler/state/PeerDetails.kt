package io.emeraldpay.polkadotcrawler.state

import identify.pb.IdentifyOuterClass
import io.emeraldpay.polkadotcrawler.polkadot.StatusProtocol
import io.emeraldpay.polkadotcrawler.proto.Dht
import io.libp2p.core.PeerId
import io.libp2p.core.multiformats.Multiaddr
import io.libp2p.core.multiformats.Protocol
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import java.lang.StringBuilder
import java.net.InetAddress
import java.net.UnknownHostException
import java.time.Instant

class PeerDetails(
        val address: Multiaddr,
        val incoming: Boolean
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
    val connectedAt = Instant.now()
    var disconnectedAt: Instant? = null
        private set
    var status: StatusProtocol.Status? = null
    var protocols: List<String>? = null

    init {
        listOf(Protocol.IP4, Protocol.IP6, Protocol.DNS4).find { protocol ->
            address.has(protocol)
        }?.let { protocol ->
            val addressValue = address.getStringComponent(protocol)
            try {
                host = InetAddress.getByName(addressValue)
            } catch (e: UnknownHostException) {
                log.debug("Invalid host: $addressValue")
            }
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

    fun close() {
        disconnectedAt = Instant.now()
    }
}