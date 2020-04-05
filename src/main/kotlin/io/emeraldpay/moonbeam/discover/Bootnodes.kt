package io.emeraldpay.moonbeam.discover

import io.libp2p.core.multiformats.Multiaddr
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import java.time.Duration

@Repository
class Bootnodes(
        @Autowired private val discovered: Discovered
): Runnable {

    companion object {
        private val log = LoggerFactory.getLogger(Bootnodes::class.java)

        private val NODES = listOf(
                // try localhost
                "/ip4/127.0.0.1/tcp/30333",

                // try kusame bootnodes
                "/dns4/p2p.cc3-0.kusama.network/tcp/30100/p2p/QmeCit3Nif4VfNqrEJsdYHZGcKzRCnZvGxg6hha1iNj4mk",
                "/dns4/p2p.cc3-1.kusama.network/tcp/30100/p2p/QmchDJtEGiEWf7Ag58HNoTg9jSGzxkSZ23VgmF6xiLKKsZ",
                "/dns4/p2p.cc3-2.kusama.network/tcp/30100/p2p/QmYG1YUekKETmD68yFKbjXDRbSAFULRRJpb1SbQPuSKA87",
                "/dns4/p2p.cc3-3.kusama.network/tcp/30100/p2p/QmQv5EXUAfVt4gbupiuLDZP2Gd7ykK6YuXoYPkyLfLtJch",
                "/dns4/p2p.cc3-4.kusama.network/tcp/30100/p2p/QmP3zYRhAxxw4fDf6Vq5agM8AZt1m2nKpPAEDmyEHPK5go",
                "/dns4/p2p.cc3-5.kusama.network/tcp/30100/p2p/QmdePe9MiAJT4yHT2tEwmazCsckAZb19uaoSUgRDffPq3G",
                "/dns4/kusama-bootnode-0.paritytech.net/tcp/30333/p2p/QmTFUXWi98EADXdsUxvv7t9fhJG1XniRijahDXxdv1EbAW",
                "/dns4/kusama-bootnode-1.paritytech.net/tcp/30333/p2p/Qmf58BhdDSkHxGy1gX5YUuHCpdYYGACxQM3nGWa7xJa5an"
        )
    }

    override fun run() {
        // resend bootnodes to discover queue, to always have nodes to start discover from
        Flux.interval(Duration.ZERO, Duration.ofMinutes(5))
                .doOnNext {
                    NODES.forEach {
                        discovered.submit(Multiaddr.fromString(it))
                    }
                }.subscribe()
    }
}