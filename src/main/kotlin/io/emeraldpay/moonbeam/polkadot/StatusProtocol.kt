package io.emeraldpay.moonbeam.polkadot

import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

/**
 * Protocol for main Polkadot messages
 * See Polkadot RE Spec (https://github.com/w3f/polkadot-spec) Appendix D. Currently it supports only Status message
 *
 */
class StatusProtocol {

    companion object {
        private val log = LoggerFactory.getLogger(StatusProtocol::class.java)
    }

    /**
     * Parse Status message
     */
    fun parse(data: ByteBuffer): Status {
        val rdr = ScaleCodec.Reader(data.array())

        val version = rdr.getUint32() //Protocol version
        val minVersion = rdr.getUint32() //Minimum supported version
        val roles = rdr.getByte() //Roles
        val unknown = rdr.getByte() //TODO what this byte means?

        val height = rdr.getUint32()
        val bestHash = rdr.getByteArray(32)
        val genesis = rdr.getByteArray(32)

        return Status(height, bestHash, genesis)
    }

    /**
     * Subset of Status message relevant to the bot.
     */
    data class Status(
            /**
             * Best block number
             */
            val height: Long,
            /**
             * Best block hash
             */
            val bestHash: ByteArray,
            /**
             * Genesis hash
             */
            val genesis: ByteArray
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Status) return false

            if (height != other.height) return false
            if (!bestHash.contentEquals(other.bestHash)) return false
            if (!genesis.contentEquals(other.genesis)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = height.hashCode()
            result = 31 * result + bestHash.contentHashCode()
            result = 31 * result + genesis.contentHashCode()
            return result
        }
    }
}