package io.emeraldpay.moonbeam.export

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.stereotype.Service
import java.io.File
import java.io.FileInputStream
import java.util.function.Consumer
import javax.annotation.PostConstruct


@Service
class GoogleStorageExport(
        @Autowired private val env: Environment,
        @Autowired private val filePostprocessing: FilePostprocessing
) {

    companion object {
        private val log = LoggerFactory.getLogger(GoogleStorageExport::class.java)
    }

    val enabled = env.getProperty("export.gs.enabled", Boolean::class.java, false)

    @PostConstruct
    fun start() {
        if (!enabled) {
            return
        }
        val jsonPath = env.getProperty("export.gs.credentials")
        var credentials: GoogleCredentials = if (StringUtils.isNotEmpty(jsonPath)) {
            GoogleCredentials.fromStream(FileInputStream(jsonPath!!))
        } else {
            GoogleCredentials.getApplicationDefault()
        }
        credentials = credentials
                .createScoped(listOf("https://www.googleapis.com/auth/cloud-platform"))

        val storage = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .build().service

        val exporter = Exporter(
                storage,
                env.getProperty("export.gs.bucket")!!,
                env.getProperty("export.gs.path") ?: ""
        )

        log.info("Export finished log file to GCP Storage bucket: gs://${exporter.bucket}/${exporter.path}")

        filePostprocessing.subscribe()
                .subscribe(exporter)

    }

    class Exporter(val storage: Storage, val bucket: String, val path: String): Consumer<File> {

        override fun accept(file: File) {
            val targetPath = path + file.name
            val blobId = BlobId.of(bucket, targetPath)
            val blobInfo = BlobInfo.newBuilder(blobId).build()
            val blob = storage.create(blobInfo, file.readBytes())
            log.info("File ${file.name} uploaded to GCP Storage at gs://${bucket}/${targetPath}")
        }

    }

}