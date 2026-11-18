package transport;

import model.*;

import api.ChatService;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class TcpChatClient implements ChatService {
    private final Socket socket = new Socket();
    private final BufferedReader input;
    private final BufferedWriter output;

    public TcpChatClient(String host, int port) throws IOException {
        try {
            socket.connect(
                    new InetSocketAddress(host, port),
                    5000
            );

            socket.setSoTimeout(10000);

            input = new BufferedReader(
                    new InputStreamReader(
                            socket.getInputStream(),
                            StandardCharsets.UTF_8
                    )
            );

            output = new BufferedWriter(
                    new OutputStreamWriter(
                            socket.getOutputStream(),
                            StandardCharsets.UTF_8
                    )
            );
        } catch (IOException | RuntimeException exception) {
            try {
                socket.close();
            } catch (IOException closeError) {
                exception.addSuppressed(closeError);
            }

            throw exception;
        }
    }

    private synchronized List<String> request(String... fields) {
        try {
            ChatProtocol.write(output, Arrays.asList(fields));

            List<String> response = ChatProtocol.read(input);

            if (response == null) {
                throw new EOFException("Сервер закрыл соединение");
            }

            if (response.size() == 3
                    && "ERROR".equals(response.get(0))) {
                if ("ARGUMENT".equals(response.get(1))) {
                    throw new IllegalArgumentException(
                            response.get(2)
                    );
                }

                throw new IllegalStateException(response.get(2));
            }

            if (!"OK".equals(response.get(0))) {
                throw new IOException("Неверный ответ сервера");
            }

            return response.subList(1, response.size());
        } catch (IOException exception) {
            try {
                socket.close();
            } catch (IOException closeError) {
                exception.addSuppressed(closeError);
            }

            throw new IllegalStateException(
                    "Ошибка TCP-соединения",
                    exception
            );
        }
    }

    @Override
    public void connect(String name) {
        request("CONNECT", name);
    }

    @Override
    public void disconnect(String name) {
        request("DISCONNECT", name);
    }

    @Override
    public void setNotificationsEnabled(
            String name,
            boolean enabled
    ) {
        request(
                "NOTIFICATIONS",
                name,
                Boolean.toString(enabled)
        );
    }

    @Override
    public Message sendMessage(String name, String text) {
        return ChatProtocol.message(
                request("SEND", name, text),
                0
        );
    }

    @Override
    public List<Message> readNotifications(String name) {
        return messages(request("READ", name));
    }

    @Override
    public void acknowledgeNotification(
            String name,
            long messageId
    ) {
        request(
                "ACK",
                name,
                Long.toString(messageId)
        );
    }

    @Override
    public List<Message> getHistory(String name) {
        return messages(request("HISTORY", name));
    }

    @Override
    public UserState getUserState(String name) {
        return ChatProtocol.user(
                request("STATE", name),
                0
        );
    }

    @Override
    public List<UserState> getUsers() {
        List<String> fields = request("USERS");
        int count = count(fields);

        List<UserState> users = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            users.add(
                    ChatProtocol.user(fields, 1 + i * 4)
            );
        }

        return List.copyOf(users);
    }

    @Override
    public ChatMetrics getMetrics() {
        List<String> fields = request("METRICS");

        return new ChatMetrics(
                Long.parseLong(fields.get(0)),
                Long.parseLong(fields.get(1))
        );
    }

    private List<Message> messages(List<String> fields) {
        int count = count(fields);
        List<Message> messages = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            messages.add(
                    ChatProtocol.message(fields, 1 + i * 4)
            );
        }

        return List.copyOf(messages);
    }

    private int count(List<String> fields) {
        int count = Integer.parseInt(fields.get(0));

        if (count < 0 || fields.size() != 1L + count * 4L) {
            throw new IllegalStateException(
                    "Неверный размер списка в ответе"
            );
        }

        return count;
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException exception) {
            throw new IllegalStateException(
                    "Не удалось закрыть TCP-клиент",
                    exception
            );
        }
    }
}