package io.emeraldpay.moonbeam.crawler

import identify.pb.IdentifyOuterClass
import io.emeraldpay.moonbeam.polkadot.StatusProtocol
import io.emeraldpay.moonbeam.proto.Dht
import io.libp2p.core.PeerId

class CrawlerData {

    enum class Type(val clazz: Class<out Any>) {
        IDENTIFY(IdentifyOuterClass.Identify::class.java),
        DHT_NODES(Dht.Message::class.java),
        PEER_ID(PeerId::class.java),
        STATUS(StatusProtocol.Status::class.java),
        PROTOCOLS(StringList::class.java)
    }

    data class StringList(
            val values: List<String>
    )

    class Value<T>(
            val dataType: Type,
            val data: T
    ) {
        fun <Z> cast(clazz: Class<Z>): Value<Z> {
            if (!clazz.isAssignableFrom(this.dataType.clazz)) {
                throw ClassCastException("Cannot cast ${this.dataType.clazz} to $clazz")
            }
            return this as Value<Z>;
        }
    }
}