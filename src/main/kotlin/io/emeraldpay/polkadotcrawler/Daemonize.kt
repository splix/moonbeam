package io.emeraldpay.polkadotcrawler

import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

/**
 * Stub for Spring Boot to force avoiding app shutdown immediately after finishing main thread
 */
@Service
class Daemonize {

    @Scheduled(fixedRate = 1_000_000)
    fun daemon() {}
}