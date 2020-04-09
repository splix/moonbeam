package io.emeraldpay.moonbeam.export

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.stereotype.Service
import java.io.File
import java.util.function.Consumer
import javax.annotation.PostConstruct


@Service
class S3Export(
        @Autowired private val env: Environment,
        @Autowired private val filePostprocessing: FilePostprocessing
) {

    companion object {
        private val log = LoggerFactory.getLogger(S3Export::class.java)
    }

    val enabled = env.getProperty("export.s3.enabled", Boolean::class.java, false)

    @PostConstruct
    fun start() {
        if (!enabled) {
            return
        }
        val credentials: AWSCredentials = BasicAWSCredentials(
                env.getProperty("export.s3.accesskey"),
                env.getProperty("export.s3.secretkey")
        )
        val region = Regions.fromName(env.getProperty("export.s3.region", "us-east-1"))
        val s3client = AmazonS3ClientBuilder
                .standard()
                .withRegion(region)
                .withCredentials(AWSStaticCredentialsProvider(credentials))
                .build()

        val exporter = Exporter(
                s3client,
                env.getProperty("export.s3.bucket")!!,
                env.getProperty("export.s3.path") ?: ""
        )

        log.info("Export finished log file to AWS S3 bucket: ${exporter.bucket} at ${region.getName()}")

        filePostprocessing.subscribe()
                .subscribe(exporter)
    }


    class Exporter(
            private val s3client: AmazonS3,
            val bucket: String,
            private val path: String
    ): Consumer<File> {

        override fun accept(file: File) {
            val targetPath = path + file.name
            s3client.putObject(bucket, targetPath, file)
            log.info("File ${file.name} uploaded to AWS S3 at ${bucket}/${targetPath}")
        }
    }
}