package ru.mail.polis.mmishak.internal.request

import one.nio.http.HttpClient
import one.nio.serial.Serializer
import org.slf4j.LoggerFactory
import ru.mail.polis.mmishak.Data
import ru.mail.polis.mmishak.ErrorData
import ru.mail.polis.mmishak.KVServiceImpl.Companion.ENTITY_PATH
import ru.mail.polis.mmishak.KVServiceImpl.Companion.INTERNAL_HEADER

class GetRequest(id: String, client: HttpClient) : InternalRequest<Data>(id, client) {

    override fun call(): Data = try {
        val body = client.get("$ENTITY_PATH?id=$id", INTERNAL_HEADER).body
        Serializer.deserialize(body) as Data
    } catch (e: Exception) {
        logger.error("Get internal request failed, id = $id")
        ErrorData
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GetRequest::class.java)
    }
}