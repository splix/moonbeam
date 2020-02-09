package io.emeraldpay.polkadotcrawler.export

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.polkadotcrawler.export.json.PeerDetailsJson
import io.emeraldpay.polkadotcrawler.state.PeerDetails
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Repository
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*

/**
 * Subscriber that export peers to a file on local filesystem
 */
@Repository
class FileJsonExport(
        @Value("\${export.file.targetdir:./log}") private val dir: File,
        @Autowired private val objectMapper: ObjectMapper
): Subscriber<PeerDetails> {

    companion object {
        private val log = LoggerFactory.getLogger(FileJsonExport::class.java)

        private val NL = "\n".toByteArray()
        private val FILE_DATE_FORMAT = DateTimeFormatter
                .ofPattern("yyyy-MM-dd'T'HH-mm-ss") // use '-' as separator for compatibility with Windows filesystem (no ':')
                .withLocale( Locale.ENGLISH ) // just to make sure
                .withZone( ZoneId.systemDefault() )
    }

    private var out: BufferedOutputStream? = null

    override fun onComplete() {
        out?.close()
        out = null
    }

    /**
     * Setup output file
     */
    fun createOutput() {
        if (out != null) {
            return
        }
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw IllegalArgumentException("Unable to create ${dir.absolutePath} directory")
            }
        }
        val path = File(dir, "peers."+FILE_DATE_FORMAT.format(Instant.now()) + ".json.log")
        if (path.exists()) {
            if (path.isDirectory) {
                throw IllegalStateException("Path ${path.absolutePath} is already exists and is a directory")
            }
            log.warn("Deleting existing file ${path.absolutePath}")
            if (!path.delete()) {
                throw IllegalStateException("Path ${path.absolutePath} is protected from writing")
            }
        }
        log.info("Save results to: ${path.absolutePath}")
        out = BufferedOutputStream(FileOutputStream(path))
    }

    override fun onSubscribe(s: Subscription) {
        createOutput()
        s.request(Long.MAX_VALUE)
    }

    override fun onNext(t: PeerDetails) {
        out?.let { w ->
            val json = PeerDetailsJson.from(t)
            val line = objectMapper.writeValueAsBytes(json)
            w.write(line)
            w.write(NL)
            w.flush()
        }
    }

    override fun onError(t: Throwable) {
    }

}