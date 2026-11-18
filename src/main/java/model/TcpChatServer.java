package model;

import api.Facade;
import api.ChatService;
import distributed.Wire;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public final class TcpChatServer implements AutoCloseable {
    private final ServerSocket listener;
    private final ChatService owner;
    private final Facade facade;

    private final ExecutorService workers =
            Executors.newCachedThreadPool();

    private final Set<Socket> clients =
            ConcurrentHashMap.newKeySet();

    private volatile boolean closed;

    public TcpChatServer(Path database, int port) throws IOException {
        this(database, "127.0.0.1", port);
    }
    public TcpChatServer(Path database, String host, int port) throws IOException {
        this(new ChatServer(database), host, port);
    }
    private TcpChatServer(ChatService chat, String host, int port) throws IOException {
        this(new ChatFacade(packet -> Wire.dispatch(chat, packet)), chat, host, port);
    }
    public TcpChatServer(Facade facade, String host, int port) throws IOException {
        this(facade, null, host, port);
    }
    private TcpChatServer(Facade facade, ChatService owner, String host, int port) throws IOException {
        this.facade = facade;
        this.owner = owner;
        listener = new ServerSocket();
        try {
            listener.setReuseAddress(true);
            listener.bind(new InetSocketAddress(host, port));
        } catch (IOException | RuntimeException error) {
            closeSocket(listener); workers.shutdown();
            if (owner != null) owner.close();
            throw error;
        }
        workers.execute(this::acceptClients);
    }

    public int getPort() {
        return listener.getLocalPort();
    }

    private void acceptClients() {
        while (!closed) {
            try {
                Socket socket = listener.accept();
                clients.add(socket);

                if (closed) {
                    clients.remove(socket);
                    closeSocket(socket);
                } else {
                    try {
                        workers.execute(() -> serve(socket));
                    } catch (RejectedExecutionException exception) {
                        clients.remove(socket);
                        closeSocket(socket);
                    }
                }
            } catch (IOException exception) {
                if (!closed) {
                    throw new IllegalStateException(
                            "Ошибка приёма TCP-соединения",
                            exception
                    );
                }
            }
        }
    }

    private void serve(Socket socket) {
        try (socket;
             BufferedReader input = new BufferedReader(
                     new InputStreamReader(
                             socket.getInputStream(),
                             StandardCharsets.UTF_8
                     )
             );
             BufferedWriter output = new BufferedWriter(
                     new OutputStreamWriter(
                             socket.getOutputStream(),
                             StandardCharsets.UTF_8
                     )
             )) {
            while (!closed) {
                List<String> response;

                try {
                    List<String> request = ChatProtocol.read(input);

                    if (request == null) {
                        break;
                    }

                    response = facade.handle(request);
                } catch (IllegalArgumentException exception) {
                    response = List.of(
                            "ERROR",
                            "ARGUMENT",
                            errorText(exception)
                    );
                } catch (IllegalStateException exception) {
                    response = List.of(
                            "ERROR",
                            "STATE",
                            errorText(exception)
                    );
                } catch (RuntimeException exception) {
                    response = List.of(
                            "ERROR",
                            "SERVER",
                            errorText(exception)
                    );
                }

                ChatProtocol.write(output, response);
            }
        } catch (IOException exception) {
            // Обрыв связи не удаляет сообщения
            // и неподтверждённые уведомления.
        } finally {
            clients.remove(socket);
        }
    }

    private String errorText(RuntimeException exception) {
        return exception.getMessage() == null
                ? "Ошибка операции"
                : exception.getMessage();
    }

    private static void closeSocket(AutoCloseable socket) {
        try {
            socket.close();
        } catch (Exception exception) {
            // Остальные ресурсы также должны быть закрыты.
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        closed = true;
        closeSocket(listener);

        for (Socket socket : clients) {
            closeSocket(socket);
        }

        workers.shutdown();

        try {
            if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                        "TCP-потоки не завершились"
                );
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException(
                    "Остановка сервера прервана",
                    exception
            );
        } finally {
            if (owner != null) owner.close();
        }
    }
}