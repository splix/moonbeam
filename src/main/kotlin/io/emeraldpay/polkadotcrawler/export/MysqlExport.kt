package io.emeraldpay.polkadotcrawler.export

import io.emeraldpay.polkadotcrawler.state.ProcessedPeerDetails
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.commons.lang3.StringUtils
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.simple.SimpleJdbcInsert
import org.springframework.stereotype.Service
import java.sql.Connection
import java.sql.SQLException
import java.sql.Timestamp
import javax.annotation.PostConstruct
import javax.sql.DataSource

/**
 * Export data to MySQL.
 */
@Service
class MysqlExport(
        @Autowired private val env: Environment
) {

    companion object {
        private val log = LoggerFactory.getLogger(MysqlExport::class.java)
    }

    private var instance: Subscriber<ProcessedPeerDetails>? = null

    @PostConstruct
    fun start() {
        val enabled = env.getProperty("export.mysql.enabled", Boolean::class.java, false)
        if (!enabled) {
            return
        }
        log.info("Setup export to MySQL")
        val jdbc = JdbcTemplate(createMysql())
        instance = MysqlSubscriber(jdbc)
    }

    fun normalizeUrl(url: String): String {
        if (url.startsWith("jdbc:mysql://")) {
            return url
        }
        if (url.startsWith("mysql://")) {
            return "jdbc:$url"
        }
        return "jdbc:mysql://$url"
    }

    fun createMysql(): DataSource {
        val dataSource = BasicDataSource()
        dataSource.initialSize = 1
        dataSource.maxTotal = 4
        dataSource.maxWaitMillis = 3000

        dataSource.validationQuery = "SELECT 1"
        dataSource.validationQueryTimeout = 2
        dataSource.testOnCreate = true

        val opts = HashMap<String, Any>()

        opts["connectTimeout"] = 3000
        opts["socketTimeout"] = 5000
        opts["autoReconnect"] = true
        opts["serverTimezone"] = "UTC"
        opts["useLegacyDatetimeCode"] = false

        if (env.getProperty("export.mysql.useSSL", Boolean::class.java, false)) {
            opts["useSSL"] = true
            opts["requireSSL"] = true
        }

        val params = opts.map { kv ->
            kv.key + "=" + kv.value
        }.joinToString("&")

        dataSource.url = normalizeUrl(env.getProperty("export.mysql.url", "jdbc:mysql://localhost:3306/polkadot")) + "?" + params

        dataSource.username = env.getProperty("export.mysql.username", "polkadot")
        dataSource.password = env.getProperty("export.mysql.password")
        dataSource.defaultAutoCommit = true

        log.info("Init MySQL DB {}", dataSource.url)

        log.debug("Validate connection...")
        var conn: Connection? = null
        try {
            conn = dataSource.getConnection()
            val st = conn.createStatement()
            val rs = st.executeQuery("SELECT 1")
            if (rs.next()) {
                log.info("Connected: {}", rs.getInt(1))
            } else {
                log.warn("Cannot get data from the server")
            }
            rs.close()
            st.close()
        } catch (e: SQLException) {
            log.error("SQL connection is not working. Msg: {}, Url: {}", e.message, dataSource.getUrl())
            throw IllegalStateException("No MySQL connection available")
        } finally {
            conn?.close()
        }

        return dataSource
    }

    class MysqlSubscriber(jdbc: JdbcTemplate): Subscriber<ProcessedPeerDetails> {

        private val insert = SimpleJdbcInsert(jdbc)
                .withTableName("nodes")
                .usingGeneratedKeyColumns("id")

        override fun onComplete() {
        }

        override fun onSubscribe(s: Subscription) {
            s.request(Long.MAX_VALUE)
        }

        override fun onNext(value: ProcessedPeerDetails) {
            insert.executeAndReturnKey(mapOf(
                    "ip" to value.host?.ip,
                    "found_at" to Timestamp.from(value.timestamp),
                    "peer_id" to value.peerId,
                    "agent_full" to value.agent?.fullName,
                    "agent_app" to value.agent?.software,
                    "agent_version" to value.agent?.version,
                    "genesis" to value.blockchain?.genesis
            ))
        }

        override fun onError(t: Throwable) {
        }
    }

    fun getInstance(): Subscriber<ProcessedPeerDetails>? {
        return instance
    }

}