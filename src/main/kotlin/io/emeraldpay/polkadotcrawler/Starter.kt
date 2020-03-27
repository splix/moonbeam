package io.emeraldpay.polkadotcrawler

import io.emeraldpay.polkadotcrawler.discover.Bootnodes
import io.emeraldpay.polkadotcrawler.monitoring.Monitoring
import org.slf4j.LoggerFactory
import org.springframework.boot.ResourceBanner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.springframework.transaction.annotation.EnableTransactionManagement
import reactor.core.publisher.Hooks
import java.util.concurrent.CancellationException

@SpringBootApplication(
        scanBasePackages = [ "io.emeraldpay.polkadotcrawler" ],
        // disable automatic spring jdbc configuration
        exclude = [
            DataSourceAutoConfiguration::class
        ]
)
@Import(Config::class)
open class Starter

private val log = LoggerFactory.getLogger(Starter::class.java)

fun main(args: Array<String>) {
    val app = SpringApplication(Starter::class.java)
    app.setBanner(ResourceBanner(ClassPathResource("banner.txt")))
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