package rpc;

import api.RpcEndpoint;
import rpc.generated.ChatWire.Reply;
import rpc.generated.ChatWire.Request;

/** Отдельный краткоживущий канал на вызов: запросы разных клиентов не сериализуются здесь. */
public final class RemoteEndpoint implements RpcEndpoint {
    private final String host;
    private final int port;
    public RemoteEndpoint(String host, int port) { this.host = host; this.port = port; }
    @Override
    public Reply handle(Request request) {
        try (ProtoConnection connection = new ProtoConnection(host, port)) {
            return connection.handle(request);
        }
    }
}
