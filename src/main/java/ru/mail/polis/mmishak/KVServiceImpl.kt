package ru.mail.polis.mmishak

import one.nio.http.*
import one.nio.server.AcceptorConfig
import org.slf4j.LoggerFactory
import ru.mail.polis.KVDao
import ru.mail.polis.KVService
import java.io.IOException
import java.util.*

class KVServiceImpl(port: Int, private val dao: KVDao) : HttpServer(buildConfig(port)), KVService {

    override fun handleRequest(request: Request?, session: HttpSession?) {
        request?.also { logger.info(it.logString) }
        super.handleRequest(request, session)
    }

    @Path("/v0/status")
    fun status() = Response.ok(MESSAGE_OK)

    @Path("/v0/entity")
    @Throws(IOException::class)
    fun entity(@Param("id") id: String?, request: Request): Response {
        if (id == null || id.isBlank()) {
            return Response(Response.BAD_REQUEST, Response.EMPTY)
        }
        return when (request.method) {
            Request.METHOD_GET -> getEntity(id)
            Request.METHOD_PUT -> putEntity(id, value = request.body)
            Request.METHOD_DELETE -> deleteEntity(id)
            else -> Response(Response.BAD_REQUEST, Response.EMPTY)
        }
    }

    @Throws(IOException::class)
    private fun getEntity(id: String) = try {
        val data = dao.get(id.toByteArray())
        Response.ok(data)
    } catch (e: NoSuchElementException) {
        Response(Response.NOT_FOUND, Response.EMPTY)
    }


    @Throws(IOException::class)
    private fun putEntity(id: String, value: ByteArray): Response {
        dao.upsert(id.toByteArray(), value)
        return Response(Response.CREATED, Response.EMPTY)
    }

    @Throws(IOException::class)
    private fun deleteEntity(id: String): Response {
        dao.remove(id.toByteArray())
        return Response(Response.ACCEPTED, Response.EMPTY)
    }

    private val Request.logString: String
        get() = "Request: [$methodName] $host$path${queryString?.let { "?$it" } ?: ""}"

    private val Request.methodName: String
        get() = when (method) {
            Request.METHOD_GET -> "GET"
            Request.METHOD_PUT -> "PUT"
            Request.METHOD_DELETE -> "DELETE"
            Request.METHOD_CONNECT -> "CONNECT"
            Request.METHOD_HEAD -> "HEAD"
            Request.METHOD_OPTIONS -> "OPTIONS"
            Request.METHOD_PATCH -> "PATH"
            Request.METHOD_POST -> "POST"
            Request.METHOD_TRACE -> "TRACE"
            else -> "OTHER"
        }

    companion object {

        private const val MESSAGE_OK = "OK"

        private val logger = LoggerFactory.getLogger(KVServiceImpl::class.java)

        private fun buildConfig(port: Int) = HttpServerConfig().apply {
            acceptors = arrayOf(
                AcceptorConfig().apply { this.port = port }
            )
        }
    }
}
