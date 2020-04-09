package io.emeraldpay.moonbeam.export

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import java.time.Duration
import javax.annotation.PreDestroy

@Service
class Archiver(
        @Autowired private val s3Export: S3Export,
        @Autowired private val googleStorageExport: GoogleStorageExport,
        @Autowired private val fileJsonExport: FileJsonExport,
        @Autowired private val filePostprocessing: FilePostprocessing
) {

    companion object {
        private val log = LoggerFactory.getLogger(Archiver::class.java)
    }

    private val enabled = s3Export.enabled || googleStorageExport.enabled

    /**
     * When any archive export is configured wait for it to finish upload
     */
    @PreDestroy
    fun destroy() {
        if (!enabled) {
            return
        }
        log.info("Closing the archive...")
        fileJsonExport.stop()
        filePostprocessing.subscribe().last()
                .then().delaySubscription(Duration.ofSeconds(3))
                .block()
    }

}