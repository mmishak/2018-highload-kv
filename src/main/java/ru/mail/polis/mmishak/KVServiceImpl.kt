package ru.mail.polis.mmishak

import one.nio.http.*
import one.nio.net.ConnectionString
import one.nio.server.AcceptorConfig
import org.slf4j.LoggerFactory
import ru.mail.polis.KVDao
import ru.mail.polis.KVService
import ru.mail.polis.mmishak.internal.RequestProcessorFactory
import java.io.IOException
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.collections.ArrayList
import kotlin.collections.HashMap


class KVServiceImpl(port: Int, private val dao: KVDao, topology: Set<String>) : HttpServer(buildConfig(port)),
    KVService {

    private val quorum = ReplicasParam.byNodeCount(topology.size)
    private val clients = HashMap<String, HttpClient>(topology.size - 1)
    private val hosts = ArrayList<String>(topology.size - 1)
    private val executor = Executors.newWorkStealingPool()
    private val putAndDeleteProcessor = RequestProcessorFactory.newRequestProcessor<Boolean>(executor)
    private val getProcessor = RequestProcessorFactory.newRequestProcessor<Data>(executor)

    init {
        topology.forEach { host ->
            hosts.add(host)
            clients[host] = HttpClient(ConnectionString(host))
        }
    }

    override fun handleRequest(request: Request?, session: HttpSession?) {
        request?.let { logger.info(it.logString) }
        super.handleRequest(request, session)
    }

    override fun handleDefault(request: Request?, session: HttpSession?) {
        session?.sendResponse(Response(Response.BAD_REQUEST, Response.EMPTY))
    }

    @Path(STATUS_PATH)
    fun status() = Response.ok(MESSAGE_OK)

    @Path(ENTITY_PATH)
    @Throws(IOException::class)
    fun entity(@Param("id") id: String?, @Param("replicas") requestReplicas: String?, request: Request): Response {
        if (id == null || id.isBlank()) {
            return Response(Response.BAD_REQUEST, Response.EMPTY)
        }

        val replicas = try {
            requestReplicas?.let { ReplicasParam.byRequestParam(it) } ?: quorum
        } catch (e: IllegalArgumentException) {
            return Response(Response.BAD_REQUEST, Response.EMPTY)
        }

        val internal = request.getHeader(INTERNAL_HEADER) != null

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

    override fun stop() {
        super.stop()
        executor.shutdown()
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }
        executor.shutdownNow()
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
            else -> "UNDEFINED"
        }

    companion object {

        const val INTERNAL_HEADER = "X-Internal-header: true"

        const val STATUS_PATH = "/v0/status"
        const val ENTITY_PATH = "/v0/entity"

        private const val MESSAGE_OK = "OK"

        private val logger = LoggerFactory.getLogger(KVServiceImpl::class.java)

        private fun buildConfig(port: Int) = HttpServerConfig().apply {
            acceptors = arrayOf(
                AcceptorConfig().apply { this.port = port }
            )
        }
    }
}
