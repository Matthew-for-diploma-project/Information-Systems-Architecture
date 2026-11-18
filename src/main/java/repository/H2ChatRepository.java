package repository;

import model.ChatMetrics;
import model.Message;
import model.UserState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Только JDBC/SQL. В распределённом режиме соединяется с единственным H2 TCP server. */
public final class H2ChatRepository implements ChatRepository {
    private final Connection connection;
    private boolean transaction;

    public H2ChatRepository(Path database) {
        this(fileUrl(database));
        try { createTables(); connection.commit(); }
        catch (SQLException error) { close(); throw new IllegalStateException("Не удалось создать таблицы", error); }
    }
    public H2ChatRepository(String url) {
        try {
            connection = DriverManager.getConnection(url, "sa", "");
            connection.setAutoCommit(false);
        } catch (SQLException error) { throw new IllegalStateException("Не удалось открыть БД", error); }
    }
    private static String fileUrl(Path database) {
        try {
            Path path = database.toAbsolutePath().normalize();
            Files.createDirectories(path.getParent());
            return "jdbc:h2:file:" + path + ";DB_CLOSE_ON_EXIT=FALSE;WRITE_DELAY=0;LOCK_TIMEOUT=30000";
        } catch (IOException error) { throw new IllegalStateException("Не удалось создать папку БД", error); }
    }
    private <T> T sql(SqlTask<T> task) {
        try { return task.run(); }
        catch (SQLException error) { throw new IllegalStateException("Ошибка операции с БД", error); }
    }
    private void requireTransaction() {
        if (!transaction) throw new IllegalStateException("Сначала BEGIN");
    }
    @Override
    public void begin() {
        if (transaction) throw new IllegalStateException("Транзакция уже открыта");
        sql(() -> {
            // Одна строка БД согласует операции ВСЕХ реплик storage/business.
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery("SELECT id FROM chat_stats WHERE id = 1 FOR UPDATE")) {
                if (!result.next()) throw new SQLException("Не найдена строка счётчиков");
            }
            return null;
        });
        transaction = true;
    }
    @Override
    public UserState findUser(String name) { requireTransaction(); return sql(() -> readUserByName(name)); }
    @Override
    public void saveUser(UserState user) {
        requireTransaction();
        sql(() -> update("MERGE INTO chat_users KEY(name) VALUES (?, ?, ?, ?)",
                user.name(), user.online(), user.lastConnected().toString(), user.notificationsEnabled()));
    }
    @Override
    public Message addMessage(String sender, String text, Instant time) {
        requireTransaction();
        return sql(() -> {
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO chat_messages(sender, text, created_at) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {
                statement.setString(1, sender); statement.setString(2, text); statement.setString(3, time.toString());
                statement.executeUpdate();
                try (ResultSet keys = statement.getGeneratedKeys()) {
                    if (!keys.next()) throw new SQLException("БД не вернула номер сообщения");
                    return new Message(keys.getLong(1), sender, text, time);
                }
            }
        });
    }
    @Override
    public void enqueue(long id, List<String> recipients) {
        requireTransaction();
        sql(() -> {
            for (String name : recipients) update("INSERT INTO chat_notifications VALUES (?, ?)", name, id);
            return update("UPDATE chat_stats SET notifications = notifications + ? WHERE id = 1", recipients.size());
        });
    }
    @Override
    public List<Message> notifications(String name) {
        requireTransaction();
        return sql(() -> readMessages("""
                SELECT m.* FROM chat_messages m JOIN chat_notifications n ON n.message_id = m.id
                WHERE n.recipient = ? ORDER BY m.id
                """, name));
    }
    @Override
    public void acknowledge(String name, long id) {
        requireTransaction();
        sql(() -> update("DELETE FROM chat_notifications WHERE recipient = ? AND message_id = ?", name, id));
    }
    @Override
    public List<Message> history() {
        requireTransaction();
        return sql(() -> readMessages("SELECT * FROM chat_messages ORDER BY id"));
    }
    @Override
    public List<UserState> users() {
        requireTransaction();
        return sql(() -> {
            List<UserState> users = new ArrayList<>();
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery("SELECT * FROM chat_users ORDER BY name")) {
                while (result.next()) users.add(readUser(result));
            }
            return List.copyOf(users);
        });
    }
    @Override
    public ChatMetrics getMetrics() {
        if (transaction) throw new IllegalStateException("Сначала завершите транзакцию");
        return sql(() -> {
            long[] counts = readCounts(); connection.rollback();
            return new ChatMetrics(counts[0], counts[1]);
        });
    }
    @Override
    public void commit() {
        requireTransaction();
        sql(() -> { update("UPDATE chat_stats SET requests = requests + 1 WHERE id = 1"); connection.commit(); return null; });
        transaction = false;
    }
    @Override
    public void rollback() {
        sql(() -> { connection.rollback(); return null; });
        transaction = false;
    }
    @Override
    public void close() {
        sql(() -> { connection.close(); return null; });
        transaction = false;
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

    private UserState readUserByName(String name) throws SQLException {
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

    @FunctionalInterface
    private interface SqlTask<T> { T run() throws SQLException; }
}
