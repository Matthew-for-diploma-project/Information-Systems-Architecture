package model;

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
    private final ChatServer chat;

    private final ExecutorService workers =
            Executors.newCachedThreadPool();

    private final Set<Socket> clients =
            ConcurrentHashMap.newKeySet();

    private volatile boolean closed;

    public TcpChatServer(Path database, int port) throws IOException {
        this(database, "127.0.0.1", port);
    }

    public TcpChatServer(
            Path database,
            String host,
            int port
    ) throws IOException {
        listener = new ServerSocket();

        try {
            listener.bind(new InetSocketAddress(host, port));
            chat = new ChatServer(database);
        } catch (IOException | RuntimeException exception) {
            closeSocket(listener);
            workers.shutdown();
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
        String command = request.get(0);

        if (command == null) {
            throw new IllegalArgumentException("Команда не задана");
        }

        int size = switch (command) {
            case "CONNECT", "DISCONNECT", "READ",
                 "HISTORY", "STATE" -> 2;
            case "NOTIFICATIONS", "SEND", "ACK" -> 3;
            case "USERS", "METRICS" -> 1;
            default -> throw new IllegalArgumentException(
                    "Неизвестная команда: " + command
            );
        };

        if (request.size() != size) {
            throw new IllegalArgumentException(
                    "Неверное число аргументов"
            );
        }

        List<String> result = new ArrayList<>();
        result.add("OK");

        switch (command) {
            case "CONNECT" -> chat.connect(request.get(1));

            case "DISCONNECT" -> chat.disconnect(request.get(1));

            case "NOTIFICATIONS" -> chat.setNotificationsEnabled(
                    request.get(1),
                    ChatProtocol.booleanValue(request.get(2))
            );

            case "SEND" -> ChatProtocol.addMessage(
                    result,
                    chat.sendMessage(
                            request.get(1),
                            request.get(2)
                    )
            );

            case "READ", "HISTORY" -> {
                List<Message> messages = command.equals("READ")
                        ? chat.readNotifications(request.get(1))
                        : chat.getHistory(request.get(1));

                result.add(Integer.toString(messages.size()));

                for (Message message : messages) {
                    ChatProtocol.addMessage(result, message);
                }
            }

            case "ACK" -> chat.acknowledgeNotification(
                    request.get(1),
                    Long.parseLong(request.get(2))
            );

            case "STATE" -> ChatProtocol.addUser(
                    result,
                    chat.getUserState(request.get(1))
            );

            case "USERS" -> {
                List<UserState> users = chat.getUsers();
                result.add(Integer.toString(users.size()));

                for (UserState user : users) {
                    ChatProtocol.addUser(result, user);
                }
            }

            case "METRICS" -> {
                synchronized (chat) {
                    result.add(
                            Long.toString(
                                    chat.getMetrics().getRequestsCount()
                            )
                    );

                    result.add(
                            Long.toString(
                                    chat.getMetrics()
                                            .getNotificationsCount()
                            )
                    );
                }
            }

            default -> throw new IllegalArgumentException(
                    "Неизвестная команда"
            );
        }

        return result;
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
            chat.close();
        }
    }
}