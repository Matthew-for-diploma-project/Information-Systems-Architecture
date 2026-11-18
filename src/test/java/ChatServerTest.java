import api.ChatService;
import model.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ChatServerTest {
    @TempDir
    Path directory;

    @Test
    void borisScenarioAndExplicitAcknowledgement() throws Exception {
        try (TestChat test =
                     new TestChat(directory.resolve("scenario"))) {
            ChatService chat = test.chat;

            chat.connect("Анна");
            chat.connect("Вера");

            Message first = chat.sendMessage(
                    "Анна",
                    "Всем привет!"
            );

            assertEquals(
                    List.of(first),
                    chat.readNotifications("Анна")
            );

            assertEquals(
                    List.of(first),
                    chat.readNotifications("Анна")
            );

            chat.acknowledgeNotification("Анна", first.id());
            chat.acknowledgeNotification("Вера", first.id());

            chat.connect("Борис");
            chat.setNotificationsEnabled("Борис", false);

            Message second = chat.sendMessage(
                    "Вера",
                    "Продолжим обсуждение."
            );

            assertTrue(
                    chat.readNotifications("Борис").isEmpty()
            );

            assertEquals(
                    List.of(first, second),
                    chat.getHistory("Борис")
            );

            chat.setNotificationsEnabled("Борис", true);

            assertTrue(
                    chat.readNotifications("Борис").isEmpty()
            );

            Message third = chat.sendMessage(
                    "Борис",
                    "Я включил уведомления."
            );

            assertEquals(
                    List.of(second, third),
                    chat.readNotifications("Анна")
            );

            assertEquals(
                    List.of(second, third),
                    chat.readNotifications("Вера")
            );

            assertEquals(
                    List.of(third),
                    chat.readNotifications("Борис")
            );

            assertEquals(
                    7L,
                    chat.getMetrics().getNotificationsCount()
            );

            assertEquals(3, chat.getUsers().size());

            long requests =
                    chat.getMetrics().getRequestsCount();

            assertThrows(
                    IllegalArgumentException.class,
                    () -> chat.sendMessage("Анна", " ")
            );

            assertThrows(
                    IllegalStateException.class,
                    () -> chat.connect("Анна")
            );

            assertEquals(
                    requests,
                    chat.getMetrics().getRequestsCount()
            );
        }
    }

    @Test
    void failedDeliveryRollsBackMessageAndCounters() throws Exception {
        Path database = directory.resolve("rollback");

        try (TestChat test = new TestChat(database)) {
            ChatService chat = test.chat;

            chat.connect("Анна");
            chat.connect("Борис");

            try (Connection connection =
                         DriverManager.getConnection(
                                 "jdbc:h2:file:" + database,
                                 "sa",
                                 ""
                         );
                 Statement statement =
                         connection.createStatement()) {
                statement.executeUpdate(
                        """
                        ALTER TABLE chat_notifications
                        ADD CHECK(recipient <> 'Борис')
                        """
                );
            }

            assertThrows(
                    IllegalStateException.class,
                    () -> chat.sendMessage("Анна", "Откат")
            );

            assertEquals(
                    2L,
                    chat.getMetrics().getRequestsCount()
            );

            assertEquals(
                    0L,
                    chat.getMetrics().getNotificationsCount()
            );

            assertTrue(
                    chat.getHistory("Анна").isEmpty()
            );

            assertTrue(
                    chat.readNotifications("Анна").isEmpty()
            );
        }
    }

    @Test
    void concurrentMessagesSurviveRestart() throws Exception {
        Path database = directory.resolve("concurrent");

        ExecutorService pool =
                Executors.newFixedThreadPool(3);

        CountDownLatch ready = new CountDownLatch(3);
        CountDownLatch start = new CountDownLatch(1);

        try (TestChat test = new TestChat(database)) {
            try {
                List<Future<?>> tasks = new ArrayList<>();

                for (String name : List.of(
                        "Анна",
                        "Вера",
                        "Борис"
                )) {
                    tasks.add(pool.submit(() -> {
                        try (ChatService client =
                                     test.newClient()) {
                            client.connect(name);
                            ready.countDown();

                            if (!start.await(
                                    10,
                                    TimeUnit.SECONDS
                            )) {
                                throw new IllegalStateException(
                                        "Не дождались старта"
                                );
                            }

                            for (int i = 0; i < 10; i++) {
                                client.sendMessage(
                                        name,
                                        name + "-" + i
                                );
                            }
                        }

                        return null;
                    }));
                }

                assertTrue(
                        ready.await(10, TimeUnit.SECONDS)
                );

                start.countDown();

                for (Future<?> task : tasks) {
                    task.get(20, TimeUnit.SECONDS);
                }
            } finally {
                start.countDown();
                pool.shutdownNow();

                assertTrue(
                        pool.awaitTermination(
                                5,
                                TimeUnit.SECONDS
                        )
                );
            }
        }

        try (TestChat test = new TestChat(database)) {
            ChatService chat = test.chat;

            assertEquals(
                    33L,
                    chat.getMetrics().getRequestsCount()
            );

            assertEquals(
                    90L,
                    chat.getMetrics().getNotificationsCount()
            );

            List<Message> history =
                    chat.getHistory("Анна");

            assertEquals(30, history.size());

            HashSet<String> expected = new HashSet<>();

            for (String name : List.of(
                    "Анна",
                    "Вера",
                    "Борис"
            )) {
                for (int i = 0; i < 10; i++) {
                    expected.add(name + "-" + i);
                }

                assertEquals(
                        history,
                        chat.readNotifications(name)
                );
            }

            HashSet<String> actual = new HashSet<>();

            for (int i = 0; i < history.size(); i++) {
                assertEquals(
                        i + 1L,
                        history.get(i).id()
                );

                actual.add(history.get(i).text());
            }

            assertEquals(expected, actual);
        }
    }
}