package ru.mail.polis.mmishak.internal

import java.util.concurrent.ExecutorService

object RequestProcessorFactory {
    fun <T> newRequestProcessor(executorService: ExecutorService) = SequenceRequestProcessor<T>(executorService)
}