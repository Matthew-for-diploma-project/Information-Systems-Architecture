package rpc;

import api.RpcEndpoint;
import rpc.generated.ChatWire.Reply;
import rpc.generated.ChatWire.Request;
import java.io.*;
import java.net.*;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Supplier;

/** Внутренний сервер; обработчик хранилища принадлежит ровно одному соединению. */
public final class ProtoServer implements AutoCloseable {
    private final ServerSocket listener = new ServerSocket();
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private final ExecutorService workers;
    private final Thread acceptor;
    private final Supplier<RpcEndpoint> handlers;
    private volatile boolean closed;

    public ProtoServer(String host, int port, int count, Supplier<RpcEndpoint> handlers) throws IOException {
        if (count <= 0) throw new IllegalArgumentException("Число обработчиков должно быть положительным");
        this.handlers = handlers;
        workers = Executors.newFixedThreadPool(count);
        try { listener.bind(new InetSocketAddress(host, port)); }
        catch (IOException exception) { workers.shutdownNow(); listener.close(); throw exception; }
        acceptor = new Thread(this::accept, "protobuf-accept");
        acceptor.start();
    }
    public int getPort() { return listener.getLocalPort(); }
    private void accept() {
        while (!closed) {
            try {
                Socket socket = listener.accept();
                socket.setSoTimeout(10000);
                socket.setTcpNoDelay(true);
                clients.add(socket);
                if (closed) { clients.remove(socket); socket.close(); }
                else workers.execute(() -> serve(socket));
            } catch (IOException | RejectedExecutionException exception) {
                if (!closed) close();
            }
        }
    }
    private void serve(Socket socket) {
        try (socket; RpcEndpoint endpoint = handlers.get();
             var input = new BufferedInputStream(socket.getInputStream());
             var output = new BufferedOutputStream(socket.getOutputStream())) {
            while (!closed) {
                Request request = Request.parseDelimitedFrom(input);
                if (request == null) break;
                Reply reply;
                try { reply = endpoint.handle(request); }
                catch (RuntimeException exception) { reply = ProtoMapper.error(exception); }
                reply.writeDelimitedTo(output);
                output.flush();
            }
        } catch (IOException ignored) {
            // Закрытие канала закрывает незавершённую транзакцию через endpoint.close().
        } finally { clients.remove(socket); }
    }
    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try { listener.close(); } catch (IOException ignored) { }
        for (Socket socket : clients) {
            try { socket.close(); } catch (IOException ignored) { }
        }
        // Не прерываем JDBC commit/rollback: interrupt может закрыть файловый канал H2.
        // Закрытые выше сокеты разблокируют чтение; обработчики сами откатывают сеансы.
        workers.shutdown();
        try {
            if (!workers.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Внутренние обработчики не завершились");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Остановка прервана", exception);
        }
    }
}
