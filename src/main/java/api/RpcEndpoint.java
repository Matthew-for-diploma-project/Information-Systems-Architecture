package api;

import rpc.generated.ChatWire.Reply;
import rpc.generated.ChatWire.Request;

/** Типизированный внутренний контракт. Сеанс хранилища живёт до закрытия канала. */
@FunctionalInterface
public interface RpcEndpoint extends AutoCloseable {
    Reply handle(Request request);

    @Override
    default void close() {
    }
}
