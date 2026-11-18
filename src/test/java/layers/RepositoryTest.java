package layers;

import model.Message;
import model.UserState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import storage.JdbcChatRepository;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class RepositoryTest {
    @TempDir Path directory;
    private final Instant time = Instant.parse("2026-01-01T00:00:00Z");
    @Test
    void committedDataSurvivesReopenAndRepeatedAckIsSafe() {
        Path database = directory.resolve("committed");
        Message saved;
        try (var repository = new JdbcChatRepository(database)) {
            try (var tx = repository.begin()) {
                tx.saveUser(new UserState("Анна", true, time, true));
                saved = tx.addMessage("Анна", "Привет", time);
                tx.enqueue(saved.id(), List.of("Анна"));
                tx.commit();
            }
        }
        try (var repository = new JdbcChatRepository(database)) {
            assertEquals(1L, repository.getMetrics().getRequestsCount());
            assertEquals(1L, repository.getMetrics().getNotificationsCount());
            try (var tx = repository.begin()) {
                assertEquals(List.of(saved), tx.history());
                assertEquals(List.of(saved), tx.notifications("Анна"));
                tx.acknowledge("Анна", saved.id());
                tx.acknowledge("Анна", saved.id());
                assertTrue(tx.notifications("Анна").isEmpty());
                assertEquals(List.of(saved), tx.history());
                tx.commit();
            }
        }
    }
    @Test
    void closeWithoutCommitAndFailedEnqueueRollBackAllChanges() throws Exception {
        Path database = directory.resolve("rollback");
        try (var repository = new JdbcChatRepository(database)) {
            try (var tx = repository.begin()) { tx.saveUser(new UserState("Отмена", true, time, true)); }
            assertEquals(0, repository.getMetrics().getRequestsCount());
            try (var tx = repository.begin()) {
                tx.saveUser(new UserState("Анна", true, time, true));
                tx.commit();
            }
            try (var connection = DriverManager.getConnection("jdbc:h2:file:" + database, "sa", "");
                 var statement = connection.createStatement()) {
                statement.executeUpdate("ALTER TABLE chat_notifications ADD CHECK(recipient <> 'Анна')");
            }
            try (var tx = repository.begin()) {
                Message message = tx.addMessage("Анна", "Откат", time);
                assertThrows(IllegalStateException.class, () -> tx.enqueue(message.id(), List.of("Анна")));
            }
            assertEquals(1, repository.getMetrics().getRequestsCount());
            assertEquals(0, repository.getMetrics().getNotificationsCount());
            try (var tx = repository.begin()) {
                assertNull(tx.findUser("Отмена"));
                assertTrue(tx.history().isEmpty());
                assertTrue(tx.notifications("Анна").isEmpty());
            }
        }
    }
    @Test
    void parallelTransactionsDoNotLoseUpdates() throws Exception {
        try (var repository = new JdbcChatRepository(directory.resolve("parallel"))) {
            ExecutorService pool = Executors.newFixedThreadPool(3);
            CountDownLatch ready = new CountDownLatch(3), start = new CountDownLatch(1);
            try {
                var tasks = new java.util.ArrayList<Future<?>>();
                for (String name : List.of("Анна", "Вера", "Борис")) {
                    tasks.add(pool.submit(() -> {
                        ready.countDown();
                        if (!start.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Нет старта");
                        try (var tx = repository.begin()) {
                            tx.saveUser(new UserState(name, true, time, true));
                            tx.commit();
                        }
                        return null;
                    }));
                }
                assertTrue(ready.await(5, TimeUnit.SECONDS)); start.countDown();
                for (var task : tasks) task.get(5, TimeUnit.SECONDS);
            } finally { start.countDown(); pool.shutdownNow(); assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS)); }
            assertEquals(3, repository.getMetrics().getRequestsCount());
            try (var tx = repository.begin()) { assertEquals(3, tx.users().size()); }
        }
    }
}
