import api.ChatService;
import distributed.*;
import model.*;
import repository.*;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

class DistributedLayersTest {
    @TempDir Path directory;

    @Test
    void facadeValidatesBeforeCallingBusiness() {
        ChatFacade facade = new ChatFacade(packet -> { fail("Некорректный запрос дошёл до бизнеса"); return null; });
        for (var fields : List.of(List.of("BEGIN"), List.of("CONNECT", " "), List.of("SEND", "Анна", " "),
                List.of("ACK", "Анна", "0"), List.of("NOTIFICATIONS", "Анна", "yes"), List.of("USERS", "лишнее"))) {
            assertThrows(IllegalArgumentException.class, () -> facade.handle(fields));
        }
        assertEquals(Packet.of("SEND", "Анна", "Привет\nМир"), ChatFacade.parse(List.of("SEND", "Анна", "Привет\nМир")));
    }

    @Test
    void businessSelectsEnabledRecipientsIncludingOfflineWithoutDatabase() {
        Instant time = Instant.now();
        AtomicReference<List<String>> recipients = new AtomicReference<>();
        List<String> calls = new ArrayList<>();
        ChatRepository stub = (ChatRepository) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ChatRepository.class}, (proxy, method, args) -> {
                    calls.add(method.getName());
                    return switch (method.getName()) {
                        case "begin", "commit", "rollback", "close" -> null;
                        case "findUser" -> new UserState("Анна", true, time, true);
                        case "addMessage" -> new Message(1, (String) args[0], (String) args[1], (Instant) args[2]);
                        case "users" -> List.of(new UserState("Анна", true, time, true),
                                new UserState("Борис", false, time, true), new UserState("Вера", true, time, false));
                        case "enqueue" -> { recipients.set(((List<?>) args[1]).stream().map(String.class::cast).toList()); yield null; }
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                });
        try (var chat = new ChatServer(stub)) {
            assertThrows(IllegalArgumentException.class, () -> chat.sendMessage("Анна", " "));
            assertTrue(calls.isEmpty());
            assertEquals(1, chat.sendMessage("Анна", "Привет").id());
            assertEquals(List.of("Анна", "Борис"), recipients.get());
            assertTrue(calls.contains("commit"));
            assertFalse(calls.contains("rollback"));
        }
    }

    @Test
    void businessRollsBackInvalidStateWithoutCommitting() {
        List<String> calls = new ArrayList<>();
        ChatRepository stub = (ChatRepository) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ChatRepository.class}, (proxy, method, args) -> {
                    calls.add(method.getName());
                    return switch (method.getName()) {
                        case "begin", "rollback", "close" -> null;
                        case "findUser" -> new UserState("Борис", false, Instant.now(), true);
                        default -> throw new AssertionError("Недопустимый вызов " + method.getName());
                    };
                });
        try (var chat = new ChatServer(stub)) {
            assertThrows(IllegalStateException.class, () -> chat.sendMessage("Борис", "Привет"));
            assertEquals(List.of("begin", "findUser", "rollback"), calls);
        }
    }

    @Test
    void repositoryCommitsAndRollsBackWithoutOtherLayers() {
        Path path = directory.resolve("repository");
        try (var repository = new H2ChatRepository(path)) {
            repository.begin();
            repository.saveUser(new UserState("Анна", true, Instant.now(), true));
            Message message = repository.addMessage("Анна", "Сохранено", Instant.now());
            repository.enqueue(message.id(), List.of("Анна"));
            repository.commit();
            repository.begin();
            repository.addMessage("Анна", "Отмена", Instant.now());
            // Закрытие без commit должно откатить последнюю вставку.
        }
        try (var repository = new H2ChatRepository(path)) {
            assertEquals(1, repository.getMetrics().getRequestsCount());
            assertEquals(1, repository.getMetrics().getNotificationsCount());
            repository.begin();
            assertEquals(1, repository.history().size());
            Message message = repository.notifications("Анна").get(0);
            repository.acknowledge("Анна", message.id());
            repository.acknowledge("Анна", message.id());
            assertTrue(repository.notifications("Анна").isEmpty());
            assertEquals(1, repository.history().size());
            repository.commit();
        }
    }

    @Test
    void protobufPacketIsCompatibleWithOfficialDynamicMessage() throws Exception {
        var file = DescriptorProtos.FileDescriptorProto.newBuilder().setName("chat.proto").setSyntax("proto3")
                .addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Packet")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("operation").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("values").setNumber(2)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED))).build();
        var descriptor = Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0]).findMessageTypeByName("Packet");
        Packet source = Packet.of("SEND", "Анна", "Строка\n\t\\\0", "");
        var output = new ByteArrayOutputStream(); source.writeTo(output);
        var input = new ByteArrayInputStream(output.toByteArray());
        int length = CodedInputStream.readRawVarint32(input.read(), input);
        var decoded = DynamicMessage.parseFrom(descriptor, input.readNBytes(length));
        assertEquals(source.operation(), decoded.getField(descriptor.findFieldByNumber(1)));
        assertEquals(source.values(), decoded.getField(descriptor.findFieldByNumber(2)));
        output.reset(); decoded.writeDelimitedTo(output);
        assertEquals(source, Packet.readFrom(new ByteArrayInputStream(output.toByteArray())));
        assertThrows(EOFException.class, () -> Packet.readFrom(new ByteArrayInputStream(new byte[]{4, 10, 1, 65})));
    }

    @Test
    void fullDistributedChainPreservesStateAndAcknowledgements() throws Exception {
        Path path = directory.resolve("chain");
        Message message;
        try (Chain chain = new Chain(path); ChatService chat = chain.client()) {
            chat.connect("Анна"); chat.connect("Борис"); chat.disconnect("Борис");
            message = chat.sendMessage("Анна", "Через все слои\n\t\\\0");
            chat.acknowledgeNotification("Анна", message.id());
            assertThrows(IllegalStateException.class, () -> chat.connect("Анна"));
        }
        try (Chain chain = new Chain(path); ChatService chat = chain.client()) {
            assertTrue(chat.getUserState("Анна").online());
            assertFalse(chat.getUserState("Борис").online());
            chat.connect("Борис");
            assertEquals(List.of(message), chat.getHistory("Борис"));
            assertEquals(List.of(message), chat.readNotifications("Борис"));
            assertEquals(List.of(message), chat.readNotifications("Борис"));
            assertTrue(chat.readNotifications("Анна").isEmpty());
            chat.acknowledgeNotification("Борис", message.id());
            assertTrue(chat.readNotifications("Борис").isEmpty());
        }
    }

    @Test
    void storageReplicasShareDatabaseTransactionLock() throws Exception {
        var database = DatabaseMain.start(directory.resolve("replicas"), 0);
        String url = "jdbc:h2:tcp://127.0.0.1:" + database.getPort() + "/replicas;WRITE_DELAY=0;LOCK_TIMEOUT=30000";
        try (var one = StorageMain.start(url, "127.0.0.1", 0);
             var two = StorageMain.start(url, "127.0.0.1", 0);
             var first = new ChatServer(new BusinessMain.RemoteRepository("127.0.0.1", one.port()));
             var second = new ChatServer(new BusinessMain.RemoteRepository("127.0.0.1", two.port()))) {
            var pool = Executors.newFixedThreadPool(2);
            CountDownLatch ready = new CountDownLatch(2), start = new CountDownLatch(1);
            try {
                List<Future<Boolean>> tasks = new ArrayList<>();
                for (ChatService chat : List.of(first, second)) tasks.add(pool.submit(() -> {
                    ready.countDown();
                    if (!start.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Нет старта");
                    try { chat.connect("Анна"); return true; }
                    catch (IllegalStateException expected) { return false; }
                }));
                assertTrue(ready.await(5, TimeUnit.SECONDS)); start.countDown();
                int connected = 0;
                for (var task : tasks) if (task.get(10, TimeUnit.SECONDS)) connected++;
                assertEquals(1, connected);
                assertEquals(1, first.getMetrics().getRequestsCount());
            } finally { start.countDown(); pool.shutdownNow(); assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS)); }
        } finally { database.stop(); }
    }

    @Test
    void disconnectFromStorageRollsBackUncommittedChanges() throws Exception {
        var database = DatabaseMain.start(directory.resolve("lost"), 0);
        String url = "jdbc:h2:tcp://127.0.0.1:" + database.getPort() + "/lost;WRITE_DELAY=0";
        try {
            try (var storage = StorageMain.start(url, "127.0.0.1", 0)) {
                try (Socket socket = Wire.connect("127.0.0.1", storage.port())) {
                    Wire.exchange(socket, Packet.of("BEGIN"));
                    Wire.exchange(socket, new Packet("SAVE_USER", Wire.user(new UserState("Анна", true, Instant.now(), true)).values()));
                    Wire.exchange(socket, Packet.of("ADD_MESSAGE", "Анна", "Не сохранено", Instant.now().toString()));
                }
            }
            try (var repository = new H2ChatRepository(url)) {
                assertEquals(0, repository.getMetrics().getRequestsCount());
                repository.begin(); assertTrue(repository.users().isEmpty()); assertTrue(repository.history().isEmpty()); repository.rollback();
            }
        } finally { database.stop(); }
    }

    static final class Chain implements AutoCloseable {
        private final org.h2.tools.Server database;
        private Wire.Server storage, business;
        private TcpChatServer facade;
        Chain(Path path) throws Exception {
            database = DatabaseMain.start(path, 0);
            try {
                String url = "jdbc:h2:tcp://127.0.0.1:" + database.getPort() + "/" + path.getFileName() + ";WRITE_DELAY=0";
                storage = StorageMain.start(url, "127.0.0.1", 0);
                business = BusinessMain.start("127.0.0.1", storage.port(), "127.0.0.1", 0);
                facade = FacadeMain.start("127.0.0.1", business.port(), "127.0.0.1", 0);
            } catch (Exception error) { close(); throw error; }
        }
        ChatService client() throws IOException { return new TcpChatClient("127.0.0.1", facade.getPort()); }
        @Override public void close() {
            try { if (facade != null) facade.close(); }
            finally {
                try { if (business != null) business.close(); }
                finally { try { if (storage != null) storage.close(); } finally { database.stop(); } }
            }
        }
    }
}
