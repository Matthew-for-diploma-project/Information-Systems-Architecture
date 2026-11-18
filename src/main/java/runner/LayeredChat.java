package runner;

import facade.ChatFacade;
import rpc.ProtoServer;
import rpc.RemoteEndpoint;
import service.ApplicationEndpoint;
import service.ChatServer;
import storage.JdbcChatRepository;
import storage.RemoteChatRepository;
import storage.RepositoryEndpoint;
import transport.TcpChatServer;
import java.io.IOException;
import java.nio.file.Path;

/** Только сборка зависимостей: та же цепочка слоёв для IDEA, тестов и демонстрации. */
public final class LayeredChat implements AutoCloseable {
    private JdbcChatRepository repository;
    private ProtoServer storage;
    private ProtoServer application;
    private TcpChatServer facade;

    public LayeredChat(Path database) throws IOException {
        this(database, 4, 4, 4);
    }
    public LayeredChat(Path database, int facadeWorkers, int appWorkers, int storageWorkers) throws IOException {
        try {
            repository = new JdbcChatRepository(database);
            storage = new ProtoServer("127.0.0.1", 0, storageWorkers, () -> new RepositoryEndpoint(repository));
            var chat = new ChatServer(new RemoteChatRepository("127.0.0.1", storage.getPort()));
            application = new ProtoServer("127.0.0.1", 0, appWorkers, () -> new ApplicationEndpoint(chat));
            facade = new TcpChatServer(new ChatFacade(new RemoteEndpoint("127.0.0.1", application.getPort())),
                    "127.0.0.1", 0, facadeWorkers);
        } catch (IOException | RuntimeException exception) {
            close();
            throw exception;
        }
    }
    public int getPort() { return facade.getPort(); }
    @Override
    public void close() {
        try { if (facade != null) facade.close(); }
        finally {
            try { if (application != null) application.close(); }
            finally {
                try { if (storage != null) storage.close(); }
                finally { if (repository != null) repository.close(); }
            }
        }
    }
}
