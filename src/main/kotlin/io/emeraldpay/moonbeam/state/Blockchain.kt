package io.emeraldpay.moonbeam.state

/**
 * Peer blockchain details
 */
data class Blockchain(
        val height: Long,
        val bestHash: String,
        val genesis: String
) {

}