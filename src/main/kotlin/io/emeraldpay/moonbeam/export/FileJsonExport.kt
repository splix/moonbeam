package io.emeraldpay.moonbeam.export

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.moonbeam.export.json.PeerDetailsJson
import io.emeraldpay.moonbeam.state.ProcessedPeerDetails
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Repository
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.time.Duration
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
        @Value("\${export.file.timelimit:60m}") val timeLimitStr: String,
        @Autowired private val objectMapper: ObjectMapper,
        @Autowired private val filePostprocessing: FilePostprocessing
): Subscriber<ProcessedPeerDetails> {

    companion object {
        private val log = LoggerFactory.getLogger(FileJsonExport::class.java)

        private val NL = "\n".toByteArray()
        private val FILE_DATE_FORMAT = DateTimeFormatter
                .ofPattern("yyyy-MM-dd'T'HH-mm-ss") // use '-' as separator for compatibility with Windows filesystem (no ':')
                .withLocale( Locale.ENGLISH ) // just to make sure
                .withZone( ZoneId.systemDefault() )

        private val SUFFIX_CHARS = "1234567890qwertyuiopasdfghjklzxcvbnm".toCharArray()
        private const val SUFFIX_LEN = 8
        private val RND = Random()
    }

    private var out: ExportFile? = null
    private val timeLimit: Duration = parseTimeLimit(timeLimitStr)

    /**
     * Random suffix uniq per instance, to avoid collision if multiple crawlers are running
     */
    private val suffix: String = createSuffix()

    init {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw IllegalArgumentException("Unable to create ${dir.absolutePath} directory")
            }
        }
    }

    override fun onComplete() {
        out?.close()
        out = null
    }

    /**
     * Setup output file
     */
    fun ensureOutput() {
        //
        // Subscription must be single-threaded (default) so the method doesn't need any locks/synchronization/etc
        //
        out?.let {
            if (it.limit <= Instant.now()) {
                it.close()
                out = null
            }
        }
        if (out != null) {
            return
        }
        val path = File(dir, "moonbeam." + FILE_DATE_FORMAT.format(Instant.now()) + "." + suffix + ".json.log")
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
        out = ExportFile(
                BufferedOutputStream(FileOutputStream(path)),
                Instant.now().plus(timeLimit),
                path,
                filePostprocessing
        )
    }

    private fun createSuffix(): String {
        val result = CharArray(SUFFIX_LEN)
        for (i in 0 until SUFFIX_LEN) {
            result[i] = SUFFIX_CHARS[RND.nextInt(SUFFIX_CHARS.size)]
        }
        return String(result)
    }

    override fun onSubscribe(s: Subscription) {
        s.request(Long.MAX_VALUE)
    }

    override fun onNext(t: ProcessedPeerDetails) {
        ensureOutput()
        out?.out?.let { w ->
            val json = PeerDetailsJson.from(t)
            val line = objectMapper.writeValueAsBytes(json)
            w.write(line)
            w.write(NL)
            w.flush()
        } ?: log.warn("JSON output is not ready")
    }

    override fun onError(t: Throwable) {
    }

    fun parseTimeLimit(value: String): Duration {
        val r = Regex("^(\\d+)(\\w?)$")
        val d = r.matchEntire(value.trim())?.let { m ->
            val num = m.groupValues[1].toLong()
            if (m.groupValues.size == 3) {
                when (val t = m.groupValues[2]) {
                    "m", "M", "" -> Duration.ofMinutes(num)
                    "h", "H" -> Duration.ofHours(num)
                    else -> {
                        log.error("Invalid time limit: $value. Correct is ${num}m for $num minutes, or ${num}h for $num hours")
                        null
                    }
                }
            } else {
                Duration.ofMinutes(num)
            }
        } ?: Duration.ofHours(1)
        return minOf(Duration.ofHours(24), maxOf(Duration.ofMinutes(1), d))
    }

    class ExportFile(val out: BufferedOutputStream, val limit: Instant,
                     private val file: File, private val postprocessing: FilePostprocessing) {

        fun close() {
            out.close()
            postprocessing.submit(file)
        }

    }
}