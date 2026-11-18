import api.ChatService;
import model.Message;
import model.TcpChatClient;
import model.UserState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class ContainerTest {
    @TempDir
    Path directory;

    @Test
    void stateAndNotificationsSurviveRestart() throws Exception {
        String phase = System.getProperty(
                "chat.phase",
                "roundtrip"
        );

        if (!List.of(
                "roundtrip",
                "prepare",
                "verify"
        ).contains(phase)) {
            throw new IllegalArgumentException(
                    "Неизвестный chat.phase"
            );
        }

        String marker = System.getProperty("chat.marker");

        if (marker == null) {
            if (!phase.equals("roundtrip")) {
                throw new IllegalArgumentException(
                        "Укажи одинаковый chat.marker "
                                + "для prepare и verify"
                );
            }

            marker = UUID.randomUUID().toString();
        }

        String port = System.getProperty(
                "chat.port",
                System.getenv("CHAT_PORT")
        );

        if (port == null) {
            if (!phase.equals("roundtrip")) {
                throw new IllegalArgumentException(
                        "Укажи chat.port или CHAT_PORT"
                );
            }

            Path database = directory.resolve("container");

            try (TestChat test = new TestChat(database)) {
                prepare(test.chat, marker);
            }

            try (TestChat test = new TestChat(database)) {
                verify(test.chat, marker);
            }
        } else {
            String host = System.getProperty(
                    "chat.host",
                    System.getenv().getOrDefault(
                            "CHAT_HOST",
                            "127.0.0.1"
                    )
            );

            try (ChatService chat = new TcpChatClient(
                    host,
                    Integer.parseInt(port)
            )) {
                if (!phase.equals("verify")) {
                    prepare(chat, marker);
                }

                if (!phase.equals("prepare")) {
                    verify(chat, marker);
                }
            }
        }
    }

    private void prepare(ChatService chat, String marker) {
        String anna = "Анна-" + marker;
        String boris = "Борис-" + marker;

        chat.connect(anna);
        chat.connect(boris);
        chat.disconnect(boris);

        Message message = chat.sendMessage(
                anna,
                "Проверка сохранения: " + marker
        );

        chat.acknowledgeNotification(
                anna,
                message.id()
        );

        assertTrue(
                chat.getHistory(anna).contains(message)
        );

        assertTrue(
                chat.readNotifications(anna).isEmpty()
        );

        assertTrue(
                chat.getUserState(anna).online()
        );

        assertFalse(
                chat.getUserState(boris).online()
        );
    }

    private void verify(ChatService chat, String marker) {
        String anna = "Анна-" + marker;
        String boris = "Борис-" + marker;

        assertTrue(
                chat.getMetrics().getRequestsCount() >= 9
        );

        assertTrue(
                chat.getMetrics().getNotificationsCount() >= 2
        );

        UserState saved = chat.getUserState(anna);

        assertTrue(saved.online());
        assertNotNull(saved.lastConnected());

        assertFalse(
                chat.getUserState(boris).online()
        );

        assertTrue(
                chat.getUserState(boris)
                        .notificationsEnabled()
        );

        chat.connect(boris);

        List<Message> messages = chat.getHistory(boris)
                .stream()
                .filter(message -> message.text().equals(
                        "Проверка сохранения: " + marker
                ))
                .toList();

        assertEquals(1, messages.size());

        Message message = messages.get(0);

        assertEquals(anna, message.sender());
        assertTrue(message.id() > 0);

        assertFalse(
                saved.lastConnected()
                        .isAfter(message.createdAt())
        );

        assertFalse(
                chat.readNotifications(anna)
                        .contains(message)
        );

        assertTrue(
                chat.readNotifications(boris)
                        .contains(message)
        );

        chat.acknowledgeNotification(
                boris,
                message.id()
        );

        assertFalse(
                chat.readNotifications(boris)
                        .contains(message)
        );

        chat.setNotificationsEnabled(anna, false);
        chat.setNotificationsEnabled(boris, false);

        chat.disconnect(anna);
        chat.disconnect(boris);
    }
}