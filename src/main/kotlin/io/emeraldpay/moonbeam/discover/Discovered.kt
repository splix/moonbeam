package io.emeraldpay.moonbeam.discover

import io.emeraldpay.moonbeam.monitoring.PrometheusMetric
import io.libp2p.core.multiformats.Multiaddr
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.extra.processor.TopicProcessor
import java.util.concurrent.atomic.AtomicReference

@Repository
class Discovered {

    companion object {
        private val log = LoggerFactory.getLogger(Discovered::class.java)
    }

    private val bus = TopicProcessor.builder<List<Multiaddr>>()
            .name("discovered-buffer")
            .bufferSize(1024)
            .build()

    //use buffer because spikes in discovery cause performance degradation, TopicProcessor becomes too busy dispatching just accepted items
    private val buf = AtomicReference(ArrayList<Multiaddr>())


    fun submit(address: Multiaddr) {
        log.debug("Address to check $address")
        submit(listOf(address))
    }

    fun submit(addresses: List<Multiaddr>) {
        log.debug("Addresses to check ${addresses.size}")
        PrometheusMetric.reportDiscovered(addresses.size)
        buf.updateAndGet {
            it.addAll(addresses)
            it
        }
    }

    @Scheduled(fixedRate = 1000)
    fun send() {
        val current = buf.getAndSet(ArrayList())
        if (current.isNotEmpty()) {
            bus.onNext(current)
        }
    }

    fun listen(): Publisher<Multiaddr> {
        return Flux.from(bus).flatMap { Flux.fromIterable(it) }
    }

}