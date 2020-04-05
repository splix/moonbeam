package io.emeraldpay.moonbeam.processing

import io.emeraldpay.moonbeam.state.Agent
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

/**
 * Parse agent name
 *
 * Example Agent: parity-polkadot/v0.7.19-d12575b9-x86_64-linux-gnu (unknown)
 */
class AgentParser {

    companion object {
        private val log = LoggerFactory.getLogger(AgentParser::class.java)
        private val RE_FULL = Regex("^(.+?)/(.+?)\\s(\\(.+?\\))$")
        private val RE_COMMIT_VERSION = Regex("v?(\\d+\\.\\d+\\.\\d+)(-([a-f0-9]{7,8}))?-(.+)")
    }

    /**
     * Extracts platform id from full platform name. I.e. x86_64-linux-gnu -> linux
     */
    fun extractPlatform(agent: Agent) {
        val platformFull = agent.platformFull ?: return
        if (platformFull.contains("linux")) {
            agent.platform = "linux"
        } else if (platformFull.contains("darwin") || platformFull.contains("osx") || platformFull.contains("macos")) {
            agent.platform = "darwin"
        } else if (platformFull.contains("windows")) {
            agent.platform = "windows"
        }
    }

    fun extractVersion(agent: Agent, version: String) {
        RE_COMMIT_VERSION.matchEntire(version)?.let { m ->
            agent.version = "v${m.groupValues[1]}"
            agent.commit = StringUtils.trimToNull(m.groupValues[3])
            agent.platformFull = m.groupValues[4]
        }
    }

    fun parse(full: String): Agent {
        val result = Agent(full)
        RE_FULL.matchEntire(full)?.let { m ->
            result.software = m.groupValues[1]
            extractVersion(result, m.groupValues[2])
            extractPlatform(result)
        }
        return result
    }

}