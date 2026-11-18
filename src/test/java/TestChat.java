import api.ChatService;
import model.TcpChatClient;
import model.TcpChatServer;

import java.io.IOException;
import java.nio.file.Path;

// Тест: сервер и клиент общаются только через TCP.
final class TestChat implements AutoCloseable {
    private final TcpChatServer server;
    final ChatService chat;

    TestChat(Path database) throws IOException {
        server = new TcpChatServer(database, 0);

        try {
            chat = newClient();
        } catch (IOException | RuntimeException exception) {
            server.close();
            throw exception;
        }
    }

    int port() {
        return server.getPort();
    }

    ChatService newClient() throws IOException {
        return new TcpChatClient("127.0.0.1", port());
    }

    @Override
    public void close() {
        try {
            chat.close();
        } finally {
            server.close();
        }
    }
}
