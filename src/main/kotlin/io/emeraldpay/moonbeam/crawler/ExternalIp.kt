package io.emeraldpay.moonbeam.crawler

import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.URL

class ExternalIp {

    companion object {
        private val log = LoggerFactory.getLogger(ExternalIp::class.java)
        private val providers = listOf("https://ifconfig.co/ip", "https://ifconfig.me/ip")
    }

    fun requestIp(): String? {
        var ip: String? = null
        var i = 0
        while (ip == null) {
            if (i > 10) {
                return null
            } else if (i > 0) {
                Thread.sleep(100)
            }
            val url = providers[i % providers.size]
            ip = request(url)
            i++
        }
        return ip
    }

    private fun request(url: String): String? {
        return try {
            URL(url).readText().trim()
        } catch (e: Exception) {
            log.debug("Failed to read external ip from $url")
            null
        }
    }

}