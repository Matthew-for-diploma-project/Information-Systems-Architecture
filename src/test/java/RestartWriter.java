import api.ChatService;
import model.Message;
import model.TcpChatClient;
import model.TcpChatServer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

// Тест запускает сервер и TCP-клиент в отдельном процессе JVM.
public final class RestartWriter {
    private RestartWriter() {
    }

    public static void main(String[] args) throws Exception {
        Path database = Path.of(args[0])
                .toAbsolutePath()
                .normalize();

        try (TcpChatServer server =
                     new TcpChatServer(database, 0);
             ChatService chat =
                     new TcpChatClient(
                             "127.0.0.1",
                             server.getPort()
                     )) {
            chat.connect("Анна");
            chat.connect("Вера");
            chat.connect("Борис");

            chat.setNotificationsEnabled("Вера", false);
            chat.disconnect("Борис");

            Message message = chat.sendMessage(
                    "Анна",
                    "До перезапуска."
            );

            chat.acknowledgeNotification(
                    "Анна",
                    message.id()
            );

            chat.connect("Борис");

            // Прочитано без подтверждения.
            chat.readNotifications("Борис");

            chat.disconnect("Борис");

            if (args[1].equals("uncommitted")) {
                try (Connection dirty =
                             DriverManager.getConnection(
                                     "jdbc:h2:file:" + database,
                                     "sa",
                                     ""
                             );
                     Statement statement =
                             dirty.createStatement()) {
                    dirty.setAutoCommit(false);

                    statement.executeUpdate(
                            """
                            UPDATE chat_users
                            SET online = FALSE
                            WHERE name = 'Анна'
                            """
                    );

                    statement.executeUpdate(
                            """
                            INSERT INTO chat_messages(
                                sender,
                                text,
                                created_at
                            )
                            VALUES (
                                'Анна',
                                'Незавершённое',
                                '2026-01-01T00:00:00Z'
                            )
                            """
                    );

                    statement.executeUpdate(
                            """
                            INSERT INTO chat_notifications(
                                recipient,
                                message_id
                            )
                            SELECT 'Борис', id
                            FROM chat_messages
                            WHERE text = 'Незавершённое'
                            """
                    );

                    statement.executeUpdate(
                            """
                            UPDATE chat_stats
                            SET requests = 999, notifications = 999
                            """
                    );

                    Runtime.getRuntime().halt(0);
                }
            }

            if (args[1].equals("crash")) {
                Runtime.getRuntime().halt(0);
            }
        }
    }
}