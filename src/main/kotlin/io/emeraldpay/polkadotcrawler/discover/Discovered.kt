package io.emeraldpay.polkadotcrawler.discover

import io.libp2p.core.multiformats.Multiaddr
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.extra.processor.TopicProcessor

@Repository
class Discovered {

    companion object {
        private val log = LoggerFactory.getLogger(Discovered::class.java)
    }

    private val bus = TopicProcessor.create<Multiaddr>()

    fun submit(address: Multiaddr) {
        bus.onNext(address)
    }

    fun listen(): Publisher<Multiaddr> {
        return Flux.from(bus)
    }

}