package io.emeraldpay.polkadotcrawler

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling

@Configuration
@EnableScheduling
open class Config {

    private val log = LoggerFactory.getLogger(Config::class.java)

}