package io.emeraldpay.moonbeam.state

import org.slf4j.LoggerFactory

/**
 * List of supported libp2p protocols
 */
class Protocols(
        protocols: List<String>
) {

    companion object {
        private val log = LoggerFactory.getLogger(Protocols::class.java)
        val PROTOCOL = Regex("^(.+)/(\\d.*)$")
    }

    val versions: List<ProtocolVersions>

    init {
        val allVersions = protocols.mapNotNull {
            PROTOCOL.matchEntire(it)?.let {
                Version(it.groupValues[1], it.groupValues[2])
            }
        }
        this.versions = allVersions.groupBy { it.id }
                .map {
                    ProtocolVersions(it.key, it.value.map(Version::version))
                }
    }

    data class ProtocolVersions(val id: String, val versions: List<String>)

    data class Version(val id: String, val version: String) {

        companion object {
            val VER = Regex("^(\\d+)(\\.(\\d+))?(\\.(\\d+))?$")
        }

        fun getMajor(): Int {
            return VER.matchEntire(version)?.let {
                if (it.groups.size > 1) {
                    it.groups[1]!!.value.toInt()
                } else {
                    0
                }
            } ?: 0
        }
    }

}