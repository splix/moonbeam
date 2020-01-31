package io.emeraldpay.polkadotcrawler.discover

import io.libp2p.core.multiformats.Multiaddr
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Predicate

@Service
class NoRecentChecks: Predicate<Multiaddr> {

    companion object {
        private val log = LoggerFactory.getLogger(NoRecentChecks::class.java)
        private val TTL = Duration.ofMinutes(15)
    }

    private val history = ConcurrentHashMap<Multiaddr, Instant>()

    override fun test(address: Multiaddr): Boolean {
        val current = history[address]
        return if (current == null || current.isBefore(Instant.now().minus(TTL))) {
            history[address] = Instant.now()
            true
        } else {
            false
        }
    }

    @Scheduled(fixedRate = 5 * 60_000, initialDelay = 15 * 60_000) // run every 5 minutes, initial run after 15 minutes
    fun cleanup() {
        val recent = Instant.now().minus(TTL)
        history.forEach { (k, v) ->
            if (v.isBefore(recent)) {
                history.remove(k)
            }
        }
    }

}