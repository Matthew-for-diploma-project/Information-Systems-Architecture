package distributed;

import model.*;
import repository.ChatRepository;
import java.io.IOException;
import java.net.Socket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/** Бизнес-слой использует прежний ChatService и удалённый репозиторий. */
public final class BusinessMain {
    private BusinessMain() { }
    public static Wire.Server start(String storageHost, int storagePort, String host, int port) throws IOException {
        return new Wire.Server(host, port, socket -> Wire.serve(socket, packet -> {
            try (var chat = new ChatServer(new RemoteRepository(storageHost, storagePort))) {
                return Wire.dispatch(chat, packet);
            }
        }));
    }
    public static void main(String[] args) throws Exception {
        try (var server = start(Wire.env("STORAGE_HOST", "storage"), Integer.parseInt(Wire.env("STORAGE_PORT", "6002")),
                Wire.env("BIND_HOST", "0.0.0.0"), Integer.parseInt(Wire.env("BUSINESS_PORT", "6001")))) {
            Runtime.getRuntime().addShutdownHook(new Thread(server::close));
            new CountDownLatch(1).await();
        }
    }
    /** Одно соединение выбирает одну storage-реплику на всю транзакцию. */
    public static final class RemoteRepository implements ChatRepository {
        private final Socket socket;
        public RemoteRepository(String host, int port) {
            try { socket = Wire.connect(host, port); }
            catch (IOException error) { throw new IllegalStateException("Нет связи с хранилищем", error); }
        }
        private Packet call(String operation, String... values) { return Wire.exchange(socket, Packet.of(operation, values)); }
        @Override public void begin() { call("BEGIN"); }
        @Override public void commit() { call("COMMIT"); }
        @Override public void rollback() { call("ROLLBACK"); }
        @Override public UserState findUser(String name) {
            Packet reply = call("FIND_USER", name);
            return reply.values().isEmpty() ? null : ChatProtocol.user(reply.values(), 0);
        }
        @Override public void saveUser(UserState user) { Wire.exchange(socket, new Packet("SAVE_USER", Wire.user(user).values())); }
        @Override public Message addMessage(String sender, String text, Instant time) {
            return ChatProtocol.message(call("ADD_MESSAGE", sender, text, time.toString()).values(), 0);
        }
        @Override public List<UserState> users() { return Wire.users(call("USERS")); }
        @Override public List<Message> history() { return Wire.messages(call("HISTORY")); }
        @Override public List<Message> notifications(String name) { return Wire.messages(call("READ", name)); }
        @Override public void acknowledge(String name, long id) { call("ACK", name, "" + id); }
        @Override public void enqueue(long id, List<String> recipients) {
            List<String> values = new ArrayList<>(); values.add("" + id); values.addAll(recipients);
            Wire.exchange(socket, new Packet("ENQUEUE", values));
        }
        @Override public ChatMetrics getMetrics() {
            List<String> values = call("METRICS").values();
            return new ChatMetrics(Long.parseLong(values.get(0)), Long.parseLong(values.get(1)));
        }
        @Override public void close() {
            try { socket.close(); } catch (IOException ignored) { }
        }
    }
}
