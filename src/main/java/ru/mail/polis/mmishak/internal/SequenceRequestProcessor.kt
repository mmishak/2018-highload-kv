package ru.mail.polis.mmishak.internal

import org.slf4j.LoggerFactory
import ru.mail.polis.mmishak.internal.request.InternalRequest
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService

class SequenceRequestProcessor<T>(private val executorService: ExecutorService) : RequestProcessor<T> {

    override fun process(requests: List<InternalRequest<T>>): List<T> {
        try {
            return executorService.invokeAll(requests).map {it.get() }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (e: ExecutionException) {
            logger.error("Internal request failed", e)
        }
        return emptyList()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(SequenceRequestProcessor::class.java)
    }
}