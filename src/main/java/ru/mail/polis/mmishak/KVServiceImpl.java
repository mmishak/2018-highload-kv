package ru.mail.polis.mmishak;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

public class KVServiceImpl implements KVService {

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    private static final String PARAM_ID_KEY = "id";

    @NotNull
    private final KVDao dao;
    private final HttpServer server;

    public KVServiceImpl(int port, @NotNull KVDao dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext(PATH_STATUS, this::handleRequestStatus);
        this.server.createContext(PATH_ENTITY, this::handleRequestEntity);
    }

    @Override
    public void start() {
        this.server.start();
    }

    @Override
    public void stop() {
        this.server.stop(0);
    }

    private void sendHttpResponse(HttpExchange httpExchange, int code, byte[] data) throws IOException {
        httpExchange.sendResponseHeaders(code, data.length);
        httpExchange.getResponseBody().write(data);
        httpExchange.getResponseBody().close();
    }

    private void sendHttpResponse(HttpExchange httpExchange, int code, String message) throws IOException {
        sendHttpResponse(httpExchange, code, message.getBytes());
    }

    @NotNull
    private static Map<String, String> queryToMap(String query) {
        final Map<String, String> result = new HashMap<>();
        if (query == null) return result;
        Pattern.compile("&").splitAsStream(query)
                .map(param -> param.split("="))
                .filter(pair -> pair.length > 0)
                .map(pair -> pair.length >= 2 ? pair : new String[]{pair[0], ""})
                .forEach(pair -> result.put(pair[0], pair[1]));
        return result;
    }

    private void handleRequestStatus(HttpExchange http) throws IOException {
        sendHttpResponse(http, 200, "OK");
    }

    private void handleRequestEntity(HttpExchange http) throws IOException {
        Map<String, String> params = KVServiceImpl.queryToMap(http.getRequestURI().getQuery());

        if (!params.containsKey(PARAM_ID_KEY)) {
            sendHttpResponse(http, 404, "Param 'id' not found");
            return;
        }

        final String entityId = params.get(PARAM_ID_KEY);
        if (entityId.isEmpty()) {
            sendHttpResponse(http, 400, "Param 'id' value is empty");
            return;
        }

        switch (http.getRequestMethod().toUpperCase()) {
            case "GET":
                handleRequestGetEntity(http, entityId);
                break;
            case "PUT":
                handleRequestPutEntity(http, entityId);
                break;
            case "DELETE":
                handleRequestDeleteEntity(http, entityId);
                break;
            default:
                sendHttpResponse(http, 400, "Bad request");
        }
    }

    private void handleRequestDeleteEntity(HttpExchange http, String entityId) throws IOException {
        dao.remove(entityId.getBytes());
        sendHttpResponse(http, 202, "Data with key '" + entityId + "' has been deleted");
    }

    private void handleRequestPutEntity(HttpExchange http, String entityId) throws IOException {
        final int contentLength =
                Integer.valueOf(http.getRequestHeaders().getFirst("Content-Length"));
        final byte[] data = new byte[contentLength];
        final int readSize = http.getRequestBody().read(data);
        if (readSize != contentLength && contentLength != 0) {
            sendHttpResponse(http, 400, "Could not read data from request");
            return;
        }
        dao.upsert(entityId.getBytes(), data);
        sendHttpResponse(http, 201, "Data with key'" + entityId + "' created");
    }

    private void handleRequestGetEntity(HttpExchange http, String entityId) throws IOException {
        try {
            sendHttpResponse(http, 200, dao.get(entityId.getBytes()));
        } catch (NoSuchElementException e) {
            sendHttpResponse(http, 404, "Data with key '" + entityId + "' not found");
        }
    }
}
