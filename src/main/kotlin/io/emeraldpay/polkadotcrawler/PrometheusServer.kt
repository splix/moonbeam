package io.emeraldpay.polkadotcrawler

import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import javax.annotation.PostConstruct


@Service
class PrometheusServer(
        @Value("\${prometheus.host:127.0.0.1}") private val host: String,
        @Value("\${prometheus.port:1234}") private val port: Int
) {

    companion object {
        private val log = LoggerFactory.getLogger(PrometheusServer::class.java)
    }

    @PostConstruct
    fun start() {
        log.info("Start Prometheus server on $host:$port")
        val server = HTTPServer(host,port)
    }

}