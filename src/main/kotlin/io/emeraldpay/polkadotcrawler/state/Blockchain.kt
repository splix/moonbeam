package io.emeraldpay.polkadotcrawler.state

/**
 * Peer blockchain details
 */
data class Blockchain(
        val height: Long,
        val bestHash: String,
        val genesis: String
) {

}