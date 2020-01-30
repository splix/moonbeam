package io.emeraldpay.polkadotcrawler

import io.emeraldpay.polkadotcrawler.discover.Bootnodes
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Import

@SpringBootApplication(scanBasePackages = [ "io.emeraldpay.polkadotcrawler" ])
@Import(Config::class)
open class Starter

fun main(args: Array<String>) {
    val app = SpringApplication(Starter::class.java)
    val ctx = app.run(*args)

    ctx.getBean(Crawler::class.java).run()
    ctx.getBean(Bootnodes::class.java).run()
}