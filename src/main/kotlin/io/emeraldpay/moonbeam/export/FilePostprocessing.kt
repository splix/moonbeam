package io.emeraldpay.moonbeam.export

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.extra.processor.TopicProcessor
import java.io.File

@Service
class FilePostprocessing {

    companion object {
        private val log = LoggerFactory.getLogger(FilePostprocessing::class.java)
    }

    private val files = TopicProcessor.create<File>()

    fun submit(file: File) {
        files.onNext(file)
    }

    fun subscribe(): Flux<File> {
        return Flux.from(files)
    }

    fun stop() {
        files.onComplete()
    }

}