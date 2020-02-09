package io.emeraldpay.polkadotcrawler

import io.emeraldpay.polkadotcrawler.discover.Bootnodes
import io.emeraldpay.polkadotcrawler.monitoring.Monitoring
import org.slf4j.LoggerFactory
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Import
import reactor.core.publisher.Hooks
import java.util.concurrent.CancellationException

@SpringBootApplication(scanBasePackages = [ "io.emeraldpay.polkadotcrawler" ])
@Import(Config::class)
open class Starter

private val log = LoggerFactory.getLogger(Starter::class.java)

fun main(args: Array<String>) {
    val app = SpringApplication(Starter::class.java)
    val ctx = app.run(*args)

    ctx.getBean(Crawler::class.java).run()
    ctx.getBean(Bootnodes::class.java).run()
    ctx.getBean(Monitoring::class.java).run()

    Hooks.onErrorDropped { t ->
        if (t is CancellationException) {
            //just cancelled, do nothing
        } else {
            log.warn("Dropped exception", t)
        }
    }
}