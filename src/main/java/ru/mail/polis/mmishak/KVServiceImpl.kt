package ru.mail.polis.mmishak

import one.nio.http.*
import one.nio.server.AcceptorConfig
import ru.mail.polis.KVDao
import ru.mail.polis.KVService
import java.io.IOException
import java.util.*

class KVServiceImpl(port: Int, private val dao: KVDao) : HttpServer(buildConfig(port)), KVService {

    @Path("/v0/status")
    fun handleRequestStatus() = Response.ok("OK")

    @Path("/v0/entity")
    @Throws(IOException::class)
    fun handleRequestEntity(request: Request, session: HttpSession) {
        val entityId = request.getParameter(PARAM_ID_PREFIX)
        if (entityId == null || entityId.isBlank()) {
            session.sendResponse(Response(Response.BAD_REQUEST, Response.EMPTY))
            return
        }
        when (request.method) {
            Request.METHOD_GET -> handleRequestGetEntity(session, entityId)
            Request.METHOD_PUT -> handleRequestPutEntity(request, session, entityId)
            Request.METHOD_DELETE -> handleRequestDeleteEntity(session, entityId)
            else -> session.sendResponse(Response(Response.BAD_REQUEST, Response.EMPTY))
        }
    }

    @Throws(IOException::class)
    private fun handleRequestGetEntity(session: HttpSession, entityId: String) {
        try {
            val data = dao.get(entityId.toByteArray())
            session.sendResponse(Response.ok(data))
        } catch (e: NoSuchElementException) {
            session.sendResponse(Response(Response.NOT_FOUND, Response.EMPTY))
        }
    }

    @Throws(IOException::class)
    private fun handleRequestPutEntity(request: Request, session: HttpSession, entityId: String) {
        dao.upsert(entityId.toByteArray(), request.body)
        session.sendResponse(Response(Response.CREATED, Response.EMPTY))
    }

    @Throws(IOException::class)
    private fun handleRequestDeleteEntity(session: HttpSession, entityId: String) {
        dao.remove(entityId.toByteArray())
        session.sendResponse(Response(Response.ACCEPTED, Response.EMPTY))
    }

    companion object {

        private const val PARAM_ID_PREFIX = "id="

        private fun buildConfig(port: Int) = HttpServerConfig().apply {
            acceptors = arrayOf(
                AcceptorConfig().apply { this.port = port }
            )
        }
    }
}
