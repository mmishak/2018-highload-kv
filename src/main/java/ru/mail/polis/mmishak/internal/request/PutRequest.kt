package ru.mail.polis.mmishak.internal.request

import one.nio.http.HttpClient
import org.slf4j.LoggerFactory
import ru.mail.polis.mmishak.KVServiceImpl.Companion.ENTITY_PATH
import ru.mail.polis.mmishak.KVServiceImpl.Companion.INTERNAL_HEADER

class PutRequest(id: String, client: HttpClient, val value: ByteArray) : InternalRequest<Boolean>(id, client) {

    override fun call(): Boolean = try {
        client.put("$ENTITY_PATH?id=$id", value, INTERNAL_HEADER).status == 201
    } catch (e: Exception) {
        logger.error("Put internal request failed, id = $id")
        false
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PutRequest::class.java)
    }
}