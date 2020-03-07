package io.emeraldpay.polkadotcrawler.crawler

import io.emeraldpay.polkadotcrawler.libp2p.DhtProtocol
import io.emeraldpay.polkadotcrawler.proto.Dht
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import java.nio.ByteBuffer
import java.util.function.Function

/**
 * Request other peers known by remote, by using DHT protocol
 */
class RequestDhtDetails: RequestDetails<Dht.Message>("/ipfs/kad/1.0.0", CrawlerData.Type.DHT_NODES) {

    companion object {
        private val log = LoggerFactory.getLogger(RequestDhtDetails::class.java)
        private val dht = DhtProtocol()
    }

    override fun start(): Publisher<ByteBuffer> {
        return dht.start()
    }

    override fun process(): Function<Flux<ByteBuffer>, Flux<Dht.Message>> {
        return Function { flux ->
            flux
                .filter { it.remaining() > 5 } //skip empty responses
                .map(dht::parse)
        }
    }

}