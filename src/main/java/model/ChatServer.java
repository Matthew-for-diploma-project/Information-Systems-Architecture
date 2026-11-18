package model;

import api.ChatService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ChatServer implements ChatService {
    private final Connection connection;
    private final ChatMetrics metrics;
    private boolean closed;

    public ChatServer(Path database) {
        Connection opened = null;

        try {
            Path path = database.toAbsolutePath().normalize();
            Files.createDirectories(path.getParent());

            opened = DriverManager.getConnection(
                    "jdbc:h2:file:" + path
                            + ";DB_CLOSE_ON_EXIT=FALSE;WRITE_DELAY=0",
                    "sa",
                    ""
            );

            connection = opened;
            connection.setAutoCommit(false);

            createTables();

            long[] counts = readCounts();
            connection.commit();

            metrics = new ChatMetrics(counts[0], counts[1]);
        } catch (IOException | SQLException exception) {
            if (opened != null) {
                try {
                    opened.close();
                } catch (SQLException closeError) {
                    exception.addSuppressed(closeError);
                }
            }

            throw new IllegalStateException(
                    "Не удалось открыть БД чата",
                    exception
            );
        }
    }

    @Override
    public synchronized void connect(String name) {
        checkName(name);

        transaction(() -> {
            UserState user = findUser(name);

            if (user != null && user.online()) {
                throw new IllegalStateException(
                        "Пользователь уже в сети: " + name
                );
            }

            String time = Instant.now().toString();

            if (user == null) {
                update(
                        "INSERT INTO chat_users VALUES (?, TRUE, ?, TRUE)",
                        name,
                        time
                );
            } else {
                update(
                        """
                        UPDATE chat_users
                        SET online = TRUE, last_connected = ?
                        WHERE name = ?
                        """,
                        time,
                        name
                );
            }

            return null;
        });
    }

    @Override
    public synchronized void disconnect(String name) {
        transaction(() -> {
            requireUser(name, true);

            update(
                    "UPDATE chat_users SET online = FALSE WHERE name = ?",
                    name
            );

            return null;
        });
    }

    @Override
    public synchronized void setNotificationsEnabled(
            String name,
            boolean enabled
    ) {
        transaction(() -> {
            requireUser(name, true);

            update(
                    """
                    UPDATE chat_users
                    SET notifications_enabled = ?
                    WHERE name = ?
                    """,
                    enabled,
                    name
            );

            return null;
        });
    }

    @Override
    public synchronized Message sendMessage(String name, String text) {
        return transaction(() -> {
            requireUser(name, true);

            if (text == null || text.isBlank()) {
                throw new IllegalArgumentException(
                        "Сообщение не должно быть пустым"
                );
            }

            Instant time = Instant.now();
            long id;

            try (PreparedStatement statement = connection.prepareStatement(
                    """
                    INSERT INTO chat_messages(sender, text, created_at)
                    VALUES (?, ?, ?)
                    """,
                    Statement.RETURN_GENERATED_KEYS
            )) {
                statement.setString(1, name);
                statement.setString(2, text);
                statement.setString(3, time.toString());
                statement.executeUpdate();

                try (ResultSet keys = statement.getGeneratedKeys()) {
                    if (!keys.next()) {
                        throw new SQLException(
                                "БД не вернула номер сообщения"
                        );
                    }

                    id = keys.getLong(1);
                }
            }

            // Для пользователей вне сети уведомления остаются в БД.
            int count = update(
                    """
                    INSERT INTO chat_notifications(recipient, message_id)
                    SELECT name, ?
                    FROM chat_users
                    WHERE notifications_enabled = TRUE
                    """,
                    id
            );

            update(
                    """
                    UPDATE chat_stats
                    SET notifications = notifications + ?
                    WHERE id = 1
                    """,
                    count
            );

            return new Message(id, name, text, time);
        });
    }

    @Override
    public synchronized List<Message> readNotifications(String name) {
        return transaction(() -> {
            requireUser(name, true);

            return readMessages(
                    """
                    SELECT m.*
                    FROM chat_messages m
                    JOIN chat_notifications n ON n.message_id = m.id
                    WHERE n.recipient = ?
                    ORDER BY m.id
                    """,
                    name
            );
        });
    }

    @Override
    public synchronized void acknowledgeNotification(
            String name,
            long messageId
    ) {
        transaction(() -> {
            requireUser(name, true);

            if (messageId <= 0) {
                throw new IllegalArgumentException(
                        "Номер сообщения должен быть положительным"
                );
            }

            // Повторное подтверждение безопасно:
            // уже удалённой строки просто нет.
            update(
                    """
                    DELETE FROM chat_notifications
                    WHERE recipient = ? AND message_id = ?
                    """,
                    name,
                    messageId
            );

            return null;
        });
    }

    @Override
    public synchronized List<Message> getHistory(String name) {
        return transaction(() -> {
            requireUser(name, true);

            return readMessages(
                    "SELECT * FROM chat_messages ORDER BY id"
            );
        });
    }

    @Override
    public synchronized UserState getUserState(String name) {
        return transaction(() -> requireUser(name, false));
    }

    @Override
    public synchronized List<UserState> getUsers() {
        return transaction(() -> {
            List<UserState> users = new ArrayList<>();

            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(
                         "SELECT * FROM chat_users ORDER BY name"
                 )) {
                while (result.next()) {
                    users.add(readUser(result));
                }
            }

            return List.copyOf(users);
        });
    }

    @Override
    public ChatMetrics getMetrics() {
        return metrics;
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            try {
                connection.close();
                closed = true;
            } catch (SQLException exception) {
                throw new IllegalStateException(
                        "Не удалось закрыть БД",
                        exception
                );
            }
        }
    }

    private void createTables() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                    """
                    CREATE TABLE IF NOT EXISTS chat_users (
                        name VARCHAR(255) PRIMARY KEY,
                        online BOOLEAN NOT NULL,
                        last_connected VARCHAR(40) NOT NULL,
                        notifications_enabled BOOLEAN NOT NULL
                    )
                    """
            );

            statement.executeUpdate(
                    """
                    CREATE TABLE IF NOT EXISTS chat_messages (
                        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                        sender VARCHAR(255) NOT NULL REFERENCES chat_users(name),
                        text CHARACTER LARGE OBJECT NOT NULL,
                        created_at VARCHAR(40) NOT NULL
                    )
                    """
            );

            statement.executeUpdate(
                    """
                    CREATE TABLE IF NOT EXISTS chat_notifications (
                        recipient VARCHAR(255) NOT NULL REFERENCES chat_users(name),
                        message_id BIGINT NOT NULL REFERENCES chat_messages(id),
                        PRIMARY KEY(recipient, message_id)
                    )
                    """
            );

            statement.executeUpdate(
                    """
                    CREATE TABLE IF NOT EXISTS chat_stats (
                        id INT PRIMARY KEY,
                        requests BIGINT NOT NULL,
                        notifications BIGINT NOT NULL
                    )
                    """
            );

            statement.executeUpdate(
                    """
                    INSERT INTO chat_stats SELECT 1, 0, 0
                    WHERE NOT EXISTS (
                        SELECT 1 FROM chat_stats WHERE id = 1
                    )
                    """
            );
        }
    }

    // Все изменения одной операции и её счётчики фиксируются вместе.
    private <T> T transaction(SqlTask<T> task) {
        if (closed) {
            throw new IllegalStateException("Сервер закрыт");
        }

        try {
            T result = task.run();

            update(
                    """
                    UPDATE chat_stats
                    SET requests = requests + 1
                    WHERE id = 1
                    """
            );

            long[] counts = readCounts();
            connection.commit();

            metrics.update(counts[0], counts[1]);

            return result;
        } catch (SQLException | RuntimeException exception) {
            try {
                connection.rollback();
            } catch (SQLException rollbackError) {
                exception.addSuppressed(rollbackError);
            }

            if (exception instanceof RuntimeException runtime) {
                throw runtime;
            }

            throw new IllegalStateException(
                    "Ошибка операции с БД",
                    exception
            );
        }
    }

    private int update(String sql, Object... parameters)
            throws SQLException {
        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            bind(statement, parameters);
            return statement.executeUpdate();
        }
    }

    private List<Message> readMessages(
            String sql,
            Object... parameters
    ) throws SQLException {
        List<Message> messages = new ArrayList<>();

        try (PreparedStatement statement =
                     connection.prepareStatement(sql)) {
            bind(statement, parameters);

            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    messages.add(
                            new Message(
                                    result.getLong("id"),
                                    result.getString("sender"),
                                    result.getString("text"),
                                    Instant.parse(
                                            result.getString("created_at")
                                    )
                            )
                    );
                }
            }
        }

        return List.copyOf(messages);
    }

    private UserState findUser(String name) throws SQLException {
        try (PreparedStatement statement =
                     connection.prepareStatement(
                             "SELECT * FROM chat_users WHERE name = ?"
                     )) {
            statement.setString(1, name);

            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? readUser(result) : null;
            }
        }
    }

    private UserState requireUser(
            String name,
            boolean mustBeOnline
    ) throws SQLException {
        checkName(name);

        UserState user = findUser(name);

        if (user == null) {
            throw new IllegalArgumentException(
                    "Неизвестный пользователь: " + name
            );
        }

        if (mustBeOnline && !user.online()) {
            throw new IllegalStateException(
                    "Пользователь не в сети: " + name
            );
        }

        return user;
    }

    private UserState readUser(ResultSet result) throws SQLException {
        return new UserState(
                result.getString("name"),
                result.getBoolean("online"),
                Instant.parse(result.getString("last_connected")),
                result.getBoolean("notifications_enabled")
        );
    }

    private long[] readCounts() throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     """
                     SELECT requests, notifications
                     FROM chat_stats
                     WHERE id = 1
                     """
             )) {
            if (!result.next()) {
                throw new SQLException("Не найдены счётчики чата");
            }

            return new long[]{
                    result.getLong(1),
                    result.getLong(2)
            };
        }
    }

    private void bind(
            PreparedStatement statement,
            Object[] parameters
    ) throws SQLException {
        for (int i = 0; i < parameters.length; i++) {
            statement.setObject(i + 1, parameters[i]);
        }
    }

    private void checkName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(
                    "Имя не должно быть пустым"
            );
        }
    }

    @FunctionalInterface
    private interface SqlTask<T> {
        T run() throws SQLException;
    }
}