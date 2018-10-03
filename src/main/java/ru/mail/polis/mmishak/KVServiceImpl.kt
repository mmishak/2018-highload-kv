package ru.mail.polis.mmishak

import one.nio.http.*
import one.nio.server.AcceptorConfig
import ru.mail.polis.KVDao
import ru.mail.polis.KVService
import java.io.IOException
import java.util.*

class KVServiceImpl(port: Int, private val dao: KVDao) : HttpServer(buildConfig(port)), KVService {

    @Path("/v0/status")
    fun status() = Response.ok(MESSAGE_OK)

    @Path("/v0/entity")
    @Throws(IOException::class)
    fun entity(request: Request, session: HttpSession) {
        val id = request.getParameter(PARAM_ID_PREFIX)
        if (id == null || id.isBlank()) {
            session.sendResponse(Response(Response.BAD_REQUEST, Response.EMPTY))
            return
        }
        when (request.method) {
            Request.METHOD_GET -> session.getEntity(id)
            Request.METHOD_PUT -> session.putEntity(id, value = request.body)
            Request.METHOD_DELETE -> session.deleteEntity(id)
            else -> session.sendResponse(Response(Response.BAD_REQUEST, Response.EMPTY))
        }
    }

    @Throws(IOException::class)
    private fun HttpSession.getEntity(id: String) {
        try {
            val data = dao.get(id.toByteArray())
            sendResponse(Response.ok(data))
        } catch (e: NoSuchElementException) {
            sendResponse(Response(Response.NOT_FOUND, Response.EMPTY))
        }
    }

    @Throws(IOException::class)
    private fun HttpSession.putEntity(id: String, value: ByteArray) {
        dao.upsert(id.toByteArray(), value)
        sendResponse(Response(Response.CREATED, Response.EMPTY))
    }

    @Throws(IOException::class)
    private fun HttpSession.deleteEntity(id: String) {
        dao.remove(id.toByteArray())
        sendResponse(Response(Response.ACCEPTED, Response.EMPTY))
    }

    companion object {

        private const val MESSAGE_OK = "OK"
        private const val PARAM_ID_PREFIX = "id="

        private fun buildConfig(port: Int) = HttpServerConfig().apply {
            acceptors = arrayOf(
                AcceptorConfig().apply { this.port = port }
            )
        }
    }
}
