package layers;

import model.UserState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import rpc.*;
import rpc.generated.ChatWire;
import storage.JdbcChatRepository;
import storage.RepositoryEndpoint;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class InternalProtocolTest {
    @TempDir Path directory;
    @Test
    void binaryChannelPreservesSpecialCharactersAndOptionalFields() throws Exception {
        String text = "Кириллица\t\n\\\0";
        try (var server = new ProtoServer("127.0.0.1", 0, 1, () -> request -> {
            assertEquals(ChatWire.Request.Operation.SEND, request.getOperation());
            assertFalse(request.hasName());
            assertTrue(request.hasText());
            assertEquals(text, request.getText());
            return ChatWire.Reply.newBuilder().addMessages(ChatWire.Message.newBuilder()
                    .setId(3).setSender("Анна").setText(request.getText()).setCreatedAt("2026-01-01T00:00:00Z")).build();
        }); var channel = new ProtoConnection("127.0.0.1", server.getPort())) {
            var reply = channel.handle(ChatWire.Request.newBuilder().setOperation(ChatWire.Request.Operation.SEND).setText(text).build());
            assertEquals(text, reply.getMessages(0).getText());
            assertEquals(3, reply.getMessages(0).getId());
        }
    }
    @Test
    void mapsRemoteErrorsAndKeepsConnectionUsable() throws Exception {
        try (var server = new ProtoServer("127.0.0.1", 0, 1, () -> request -> {
            if (request.getOperation() != ChatWire.Request.Operation.METRICS) {
                throw new IllegalArgumentException("Неизвестная операция");
            }
            return ChatWire.Reply.newBuilder().setMetrics(ChatWire.Metrics.newBuilder().setRequests(10)).build();
        }); var channel = new ProtoConnection("127.0.0.1", server.getPort())) {
            assertThrows(IllegalArgumentException.class, () -> channel.handle(ChatWire.Request.newBuilder().setOperationValue(999).build()));
            assertEquals(10, channel.handle(ChatWire.Request.newBuilder().setOperation(ChatWire.Request.Operation.METRICS).build())
                    .getMetrics().getRequests());
        }
    }
    @Test
    void lostRepositoryConnectionRollsBackWholeTransaction() throws Exception {
        try (var repository = new JdbcChatRepository(directory.resolve("abandoned"))) {
            try (var server = new ProtoServer("127.0.0.1", 0, 2, () -> new RepositoryEndpoint(repository))) {
                try (var channel = new ProtoConnection("127.0.0.1", server.getPort())) {
                    channel.handle(ChatWire.Request.newBuilder().setOperation(ChatWire.Request.Operation.BEGIN).build());
                    channel.handle(ChatWire.Request.newBuilder().setOperation(ChatWire.Request.Operation.SAVE_USER)
                            .setUser(ProtoMapper.user(new UserState("Анна", true, Instant.now(), true))).build());
                    var reply = channel.handle(ChatWire.Request.newBuilder().setOperation(ChatWire.Request.Operation.ADD_MESSAGE)
                            .setName("Анна").setText("Не подтверждено").setCreatedAt(Instant.now().toString()).build());
                    channel.handle(ChatWire.Request.newBuilder().setOperation(ChatWire.Request.Operation.ENQUEUE)
                            .setMessageId(reply.getMessages(0).getId()).addAllRecipients(List.of("Анна")).build());
                    // Без COMMIT. Закрытие TCP должно откатить всю транзакцию.
                }
            }
            assertEquals(0, repository.getMetrics().getRequestsCount());
            assertEquals(0, repository.getMetrics().getNotificationsCount());
            try (var tx = repository.begin()) {
                assertTrue(tx.users().isEmpty());
                assertTrue(tx.history().isEmpty());
            }
        }
    }
}
