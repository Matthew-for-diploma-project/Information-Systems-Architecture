package transport;

import api.TextEndpoint;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

public final class TcpChatServer implements AutoCloseable {
    private final ServerSocket listener;
    private final TextEndpoint facade;
    private final ExecutorService operations;

    private final ExecutorService workers =
            Executors.newCachedThreadPool();

    private final Set<Socket> clients =
            ConcurrentHashMap.newKeySet();

    private volatile boolean closed;

    public TcpChatServer(TextEndpoint facade, String host, int port, int count) throws IOException {
        if (count <= 0) throw new IllegalArgumentException("Число обработчиков должно быть положительным");
        this.facade = facade;
        operations = Executors.newFixedThreadPool(count);
        listener = new ServerSocket();
        try {
            listener.bind(new InetSocketAddress(host, port));
        } catch (IOException exception) {
            closeSocket(listener);
            workers.shutdownNow();
            operations.shutdownNow();
            throw exception;
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

                    response = execute(request);
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

    private List<String> execute(List<String> request) {
        // Чтение постоянных соединений не занимает ограниченный пул операций.
        try {
            return operations.submit(() -> facade.handle(request)).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Ожидание фасада прервано", exception);
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof RuntimeException runtime) throw runtime;
            throw new IllegalStateException("Ошибка фасада", exception.getCause());
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

        // Доводим уже принятые команды до конца: shutdownNow оставил бы
        // ожидающие Future из очереди незавершёнными.
        operations.shutdown();
        workers.shutdown();

        try {
            if (!workers.awaitTermination(15, TimeUnit.SECONDS)) {
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
            try {
                operations.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }
}