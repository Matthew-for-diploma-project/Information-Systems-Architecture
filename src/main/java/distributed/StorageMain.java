package distributed;

import model.ChatProtocol;
import repository.ChatRepository;
import repository.H2ChatRepository;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/** SQL-слой. Ни правил чата, ни внешнего текстового протокола здесь нет. */
public final class StorageMain {
    private StorageMain() { }
    public static Packet process(ChatRepository repository, Packet packet) {
        List<String> a = packet.values();
        switch (packet.operation()) {
            case "BEGIN" -> repository.begin();
            case "COMMIT" -> repository.commit();
            case "ROLLBACK" -> repository.rollback();
            case "FIND_USER" -> { return Wire.user(repository.findUser(a.get(0))); }
            case "SAVE_USER" -> repository.saveUser(ChatProtocol.user(a, 0));
            case "ADD_MESSAGE" -> { return Wire.message(repository.addMessage(a.get(0), a.get(1), Instant.parse(a.get(2)))); }
            case "USERS" -> { return Wire.users(repository.users()); }
            case "HISTORY" -> { return Wire.messages(repository.history()); }
            case "READ" -> { return Wire.messages(repository.notifications(a.get(0))); }
            case "ENQUEUE" -> repository.enqueue(Long.parseLong(a.get(0)), a.subList(1, a.size()));
            case "ACK" -> repository.acknowledge(a.get(0), Long.parseLong(a.get(1)));
            case "METRICS" -> {
                var metrics = repository.getMetrics();
                return Packet.of("OK", "" + metrics.getRequestsCount(), "" + metrics.getNotificationsCount());
            }
            default -> throw new IllegalArgumentException("Неизвестная операция хранилища");
        }
        return Packet.of("OK");
    }
    public static Wire.Server start(String url, String host, int port) throws IOException {
        try (var repository = new H2ChatRepository(url)) { repository.getMetrics(); }
        return new Wire.Server(host, port, socket -> {
            try (var repository = new H2ChatRepository(url)) {
                Wire.serve(socket, packet -> process(repository, packet));
            }
        });
    }
    public static void main(String[] args) throws Exception {
        String url = Wire.env("H2_URL", "jdbc:h2:tcp://database:9092/chat;WRITE_DELAY=0;LOCK_TIMEOUT=30000");
        try (var server = start(url, Wire.env("BIND_HOST", "0.0.0.0"), Integer.parseInt(Wire.env("STORAGE_PORT", "6002")))) {
            Runtime.getRuntime().addShutdownHook(new Thread(server::close));
            new CountDownLatch(1).await();
        }
    }
}
