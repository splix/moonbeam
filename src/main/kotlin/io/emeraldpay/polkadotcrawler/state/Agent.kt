package io.emeraldpay.polkadotcrawler.state

import org.slf4j.LoggerFactory

class Agent(
        val fullName: String
) {

    companion object {
        private val log = LoggerFactory.getLogger(Agent::class.java)
    }

    //parity-polkadot/v0.7.19-d12575b9-x86_64-linux-gnu (unknown)
    //parity-polkadot
    var software: String? = null
    //v0.7.19
    var version: String? = null
    //d12575b9
    var commit: String? = null
    //x86_64-linux-gnu
    var platformFull: String? = null
    //linux
    var platform: String? = null

}