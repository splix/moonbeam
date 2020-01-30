package io.emeraldpay.polkadotcrawler.discover

import io.libp2p.core.multiformats.Multiaddr
import io.libp2p.core.multiformats.Protocol
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.function.Predicate
import kotlin.experimental.and


class PublicPeersOnly: Predicate<Multiaddr> {

    companion object {
        private val log = LoggerFactory.getLogger(PublicPeersOnly::class.java)
    }

    override fun test(t: Multiaddr): Boolean {
        if (t.has(Protocol.DNS4)) {
            val host = t.getStringComponent(Protocol.DNS4)!!
            if (host.endsWith(".local")) {
                return false
            }
            return true
        }
        if (t.has(Protocol.IP4)) {
            val ip4 = t.getStringComponent(Protocol.IP4)!!
            try {
                val addr = InetAddress.getByName(ip4)
                if (addr.isLoopbackAddress || addr.isSiteLocalAddress || addr.isLinkLocalAddress) {
                    return false;
                }
                if (addr.isMCGlobal || addr.isMCLinkLocal || addr.isMCOrgLocal) {
                    return false;
                }
                if (addr.isMulticastAddress) {
                    return false;
                }
                val ip: ByteArray = addr.address
                if (ip[0] == 100.toByte()) {
                    if (ip[1] and 0b01000000 > 0) { //64
                        return false
                    }
                } else if (ip[0] == 0.toByte()) {
                    return false
                } else if (ip[0] == 0xff.toByte()) {
                    return false
                }
                return true
            } catch (e: UnknownHostException) {
                return false
            }
        }
        return false
    }


}