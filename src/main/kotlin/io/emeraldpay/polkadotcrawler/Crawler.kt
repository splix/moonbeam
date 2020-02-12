package io.emeraldpay.polkadotcrawler

import com.google.protobuf.ByteString
import identify.pb.IdentifyOuterClass
import io.emeraldpay.polkadotcrawler.crawler.CrawlerClient
import io.emeraldpay.polkadotcrawler.discover.Discovered
import io.emeraldpay.polkadotcrawler.discover.NoRecentChecks
import io.emeraldpay.polkadotcrawler.discover.PublicPeersOnly
import io.emeraldpay.polkadotcrawler.export.FileJsonExport
import io.emeraldpay.polkadotcrawler.monitoring.Monitoring
import io.emeraldpay.polkadotcrawler.processing.FullProcessor
import io.emeraldpay.polkadotcrawler.proto.Dht
import io.emeraldpay.polkadotcrawler.state.PeerDetails
import io.libp2p.core.PeerId
import io.libp2p.core.crypto.KEY_TYPE
import io.libp2p.core.crypto.generateKeyPair
import io.libp2p.core.multiformats.Multiaddr
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Duration

@Service
class Crawler(
        @Autowired private val discovered: Discovered,
        @Autowired private val noRecentChecks: NoRecentChecks,
        @Autowired private val fileJsonExport: FileJsonExport,
        @Autowired private val monitoring: Monitoring,
        @Autowired private val processor: FullProcessor
): Runnable {

    companion object {
        private val log = LoggerFactory.getLogger(Crawler::class.java)
    }

    private val keys = generateKeyPair(KEY_TYPE.ED25519)

    private val agent = IdentifyOuterClass.Identify.newBuilder()
            .setAgentVersion("substrate-bot/0.1.0")
            .setProtocolVersion("/substrate/1.0")
//            .addProtocols("/substrate/ksmcc3/5")
//            .addProtocols("/substrate/sup/5")
            .addProtocols("/ipfs/ping/1.0.0")
            .addProtocols("/ipfs/id/1.0.0")
            .addProtocols("/ipfs/kad/1.0.0")
            .addListenAddrs(
                    ByteString.copyFrom(
                            Multiaddr(
                                    Multiaddr.fromString("/ip4/127.0.0.1/tcp/0"),
                                    PeerId.fromPubKey(keys.second)
                            ).getBytes()
                    )
            )
            .build()

    private val publicPeersOnly = PublicPeersOnly()

    override fun run() {
        Flux.from(discovered.listen())
                .subscribeOn(Schedulers.newSingle("crawler"))
                .filter(noRecentChecks)
                .flatMap({
                    connect(it).retry(3) { t ->
                        t is NotLoadedException
                    }.onErrorResume { t ->
                        if (t is NotLoadedException) {
                            log.debug("Peer not loaded. ${t.peer.address}")
                            noRecentChecks.forget(t.peer.address)
                        } else {
                            log.warn("Failed to connect", t)
                        }
                        Mono.empty()
                    }.subscribeOn(Schedulers.elastic())
                }, 32)
                .doOnNext { monitoring.onPeerProcessed(it) }
                .map(processor)
                .subscribe(fileJsonExport)
    }

    fun connect(address: Multiaddr): Mono<PeerDetails> {
        try {
            var crawler: CrawlerClient? = null
            val result = Mono.just(address)
                    .map { CrawlerClient(it, agent, keys) }
                    .doOnNext { crawler = it }
                    .flatMapMany { it.connect() }
                    .take(Duration.ofSeconds(15))
                    .doFinally {
                        crawler?.disconnect()
                    }
                    .reduce(PeerDetails(address)) { details, it ->
                        log.debug("Received ${it.dataType} from $address")

                        when (it.dataType) {

                            CrawlerClient.DataType.DHT_NODES -> {
                                val dht = it.cast(Dht.Message::class.java)
                                details.add(dht.data)

                                listOf(dht.data.closerPeersList, dht.data.providerPeersList).forEach { peers ->
                                    peers.flatMap {
                                        it.addrsList
                                    }.mapNotNull {
                                        try {
                                            Multiaddr(it.toByteArray())
                                        } catch (e: java.lang.IllegalArgumentException) {
                                            log.debug("Invalid address")
                                            null
                                        }
                                    }.filter {
                                        publicPeersOnly.test(it)
                                    }.forEach {
                                        discovered.submit(it)
                                    }
                                }
                            }

                            CrawlerClient.DataType.IDENTIFY -> {
                                val id = it.cast(IdentifyOuterClass.Identify::class.java)
                                details.add(id.data)
                            }

                            CrawlerClient.DataType.PEER_ID -> {
                                details.peerId = it.cast(PeerId::class.java).data
                            }
                        }

                        details
                    }
                    .onErrorResume {
                        log.debug("Connection failure. ${it.javaClass}: ${it.message}")
                        Mono.empty()
                    }
                    .map {
                        if (!it.filled()) {
                            throw NotLoadedException(it)
                        }
                        it
                    }
            return result
        } catch (e: Exception) {
            log.error("Failed to setup crawler connection", e)
            throw e
        }

    }

    class NotLoadedException(val peer: PeerDetails): Exception()
}