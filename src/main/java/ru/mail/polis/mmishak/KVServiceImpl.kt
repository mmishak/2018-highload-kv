package ru.mail.polis.mmishak

import one.nio.http.*
import one.nio.net.ConnectionString
import one.nio.serial.Serializer
import one.nio.server.AcceptorConfig
import org.slf4j.LoggerFactory
import ru.mail.polis.KVService
import ru.mail.polis.mmishak.internal.RequestProcessorFactory
import ru.mail.polis.mmishak.internal.request.DeleteRequest
import ru.mail.polis.mmishak.internal.request.GetRequest
import ru.mail.polis.mmishak.internal.request.PutRequest
import java.io.IOException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


class KVServiceImpl(port: Int, private val dao: KVDaoImpl, topology: Set<String>) : HttpServer(buildConfig(port)),
    KVService {

    private val quorum = ReplicasParam.byNodeCount(topology.size)
    private val clients = HashMap<String, HttpClient>(topology.size - 1)
    private val addresses = ArrayList<String>(topology.size - 1)
    private val executor = Executors.newWorkStealingPool()
    private val putAndDeleteProcessor = RequestProcessorFactory.newRequestProcessor<Boolean>(executor)
    private val getProcessor = RequestProcessorFactory.newRequestProcessor<Data>(executor)

    init {
        topology.forEach { address ->
            if (address.endsWith(":$port")) return@forEach
            addresses.add(address)
            clients[address] = HttpClient(ConnectionString(address))
        }
        addresses.sort()
        logger.info("Init: port = $port")
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

        return when (request.method) {
            Request.METHOD_GET -> getEntity(id, request.internal, replicas)
            Request.METHOD_PUT -> putEntity(id, request.body, request.internal, replicas)
            Request.METHOD_DELETE -> deleteEntity(id, request.internal, replicas)
            else -> Response(Response.BAD_REQUEST, Response.EMPTY)
        }
    }

    @Throws(IOException::class)
    private fun getEntity(id: String, internal: Boolean, replicas: ReplicasParam): Response {
        return try {
            when {
                internal -> getEntityInternal(id)
                else -> getEntityExternal(id, replicas)
            }
        } catch (e: Exception) {
            Response(Response.INTERNAL_ERROR, Response.EMPTY)
        }
    }

    @Throws(IOException::class)
    private fun putEntity(id: String, value: ByteArray, internal: Boolean, replicas: ReplicasParam): Response {
        return try {
            when {
                internal -> putEntityInternal(id, value)
                else -> putEntityExternal(id, value, replicas)
            }
        } catch (e: Exception) {
            Response(Response.INTERNAL_ERROR, Response.EMPTY)
        }
    }

    @Throws(IOException::class)
    private fun deleteEntity(id: String, internal: Boolean, replicas: ReplicasParam): Response {
        return try {
            when {
                internal -> deleteEntityInternal(id)
                else -> deleteEntityExternal(id, replicas)
            }
        } catch (e: Exception) {
            Response(Response.INTERNAL_ERROR, Response.EMPTY)
        }
    }

    @Throws(IOException::class)
    private fun getEntityInternal(id: String): Response {
        val data = dao.getInternal(id.toByteArray())
        return when (data) {
            is AbsentData, is DeletedData -> Response(Response.NOT_FOUND, Serializer.serialize(data))
            is ExistsData -> Response(Response.OK, Serializer.serialize(data))
            is ErrorData -> Response(Response.GATEWAY_TIMEOUT, Serializer.serialize(data))
        }
    }

    @Throws(IOException::class)
    private fun getEntityExternal(id: String, replicas: ReplicasParam): Response {
        val results = mutableListOf(dao.getInternal(id.toByteArray()))
        if (replicas.from > 1) {
            results += getProcessor.process(
                addresses.take(replicas.from - 1).mapNotNull {
                    clients[it]?.let { client -> GetRequest(id, client) }
                }
            )
        }
        val data = results
            .filter { it is ExistsData || it is DeletedData }
            .maxBy {
                when (it) {
                    is ExistsData -> it.time
                    is DeletedData -> it.time
                    else -> -1
                }
            } ?: AbsentData
        val ask = results.filter { it !is ErrorData }.count()
        return when {
            ask < replicas.ask -> Response(Response.GATEWAY_TIMEOUT, Response.EMPTY)
            else -> when (data) {
                is ExistsData -> Response(Response.OK, data.value)
                is AbsentData, is DeletedData -> Response(Response.NOT_FOUND, Response.EMPTY)
                is ErrorData -> Response(Response.GATEWAY_TIMEOUT, Response.EMPTY)
            }
        }
    }

    @Throws(IOException::class)
    private fun putEntityInternal(id: String, value: ByteArray): Response {
        dao.upsertInternal(id.toByteArray(), value)
        return Response(Response.CREATED, Response.EMPTY)
    }

    @Throws(IOException::class)
    private fun putEntityExternal(
        id: String,
        value: ByteArray,
        replicas: ReplicasParam
    ): Response {
        val results = mutableListOf(dao.upsertInternal(id.toByteArray(), value))
        if (replicas.from > 1) {
            results += putAndDeleteProcessor.process(
                addresses.take(replicas.from - 1).mapNotNull {
                    clients[it]?.let { client -> PutRequest(id, client, value) }
                }
            )
        }
        val ask = results.filter { it }.count()
        return when {
            ask < replicas.ask -> Response(Response.GATEWAY_TIMEOUT, Response.EMPTY)
            else -> Response(Response.CREATED, Response.EMPTY)
        }
    }

    @Throws(IOException::class)
    private fun deleteEntityInternal(id: String): Response {
        dao.removeInternal(id.toByteArray())
        return Response(Response.ACCEPTED, Response.EMPTY)
    }

    private fun deleteEntityExternal(
        id: String,
        replicas: ReplicasParam
    ): Response {
        val results = mutableListOf(dao.removeInternal(id.toByteArray()))
        if (replicas.from > 1) {
            results += putAndDeleteProcessor.process(
                addresses.take(replicas.from - 1).mapNotNull {
                    clients[it]?.let { client -> DeleteRequest(id, client) }
                }
            )
        }
        val ask = results.filter { it }.count()
        return when {
            ask < replicas.ask -> Response(Response.GATEWAY_TIMEOUT, Response.EMPTY)
            else -> Response(Response.ACCEPTED, Response.EMPTY)
        }
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
        logger.info("Stop: port = $port")
    }

    private val Request.logString: String
        get() = "Request: [$methodName] $host:$port$path${queryString?.let { "?$it" }
            ?: ""} ${if (internal) "(internal)" else ""}"

    private val Request.internal: Boolean
        get() = this.getHeader(INTERNAL_HEADER) != null

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
