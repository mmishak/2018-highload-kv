package ru.mail.polis.mmishak.internal.request

import one.nio.http.HttpClient
import org.slf4j.LoggerFactory
import ru.mail.polis.mmishak.KVServiceImpl.Companion.ENTITY_PATH
import ru.mail.polis.mmishak.KVServiceImpl.Companion.INTERNAL_HEADER

class DeleteRequest(id: String, client: HttpClient) : InternalRequest<Boolean>(id, client) {

    override fun call(): Boolean = try {
        client.delete("$ENTITY_PATH?id=$id", INTERNAL_HEADER).status == 202
    } catch (e: Exception) {
        logger.error("Delete internal request failed, id = $id", e)
        false
    }

    companion object {
        private val logger = LoggerFactory.getLogger(DeleteRequest::class.java)
    }
}