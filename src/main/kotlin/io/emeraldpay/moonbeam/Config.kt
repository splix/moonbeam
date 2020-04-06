package io.emeraldpay.moonbeam

import com.fasterxml.jackson.core.Version
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import java.text.SimpleDateFormat
import java.util.*

@Configuration
@EnableScheduling
open class Config {

    companion object {
        const val NET_DEBUG = false
    }

    private val log = LoggerFactory.getLogger(Config::class.java)

    @Bean
    open fun objectMapper(): ObjectMapper {
        val module = SimpleModule("Moonbeam",
                Version(1, 0, 0, null, null, null))

        val objectMapper = ObjectMapper()
        objectMapper
                .registerModule(module)
                .registerModule(Jdk8Module())
                .registerModule(JavaTimeModule())
                .setDateFormat(SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSS"))
                .setTimeZone(TimeZone.getTimeZone("UTC"))

        return objectMapper
    }
}