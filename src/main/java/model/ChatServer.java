package model;

import api.ChatService;
import repository.ChatRepository;
import repository.H2ChatRepository;
import java.nio.file.Path;

import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

/** Правила чата используют контракт ChatRepository. Конструктор Path сохранён для работы №4. */
public final class ChatServer implements ChatService {
    private final ChatRepository repository;

    public ChatServer(Path database) {
        this(new H2ChatRepository(database));
    }
    public ChatServer(ChatRepository repository) {
        this.repository = repository;
    }
    private synchronized <T> T execute(Supplier<T> action) {
        repository.begin();
        try {
            T result = action.get();
            repository.commit();
            return result;
        } catch (RuntimeException error) {
            try { repository.rollback(); }
            catch (RuntimeException rollbackError) { error.addSuppressed(rollbackError); }
            throw error;
        }
    }
    private UserState require(String name, boolean online) {
        UserState user = repository.findUser(name);
        if (user == null) {
            throw new IllegalArgumentException("Неизвестный пользователь: " + name);
        }
        if (online && !user.online()) {
            throw new IllegalStateException("Пользователь не в сети: " + name);
        }
        return user;
    }
    @Override
    public void connect(String name) {
        validateName(name);
        execute(() -> {
            UserState previous = repository.findUser(name);
            if (previous != null && previous.online()) {
                throw new IllegalStateException("Пользователь уже в сети: " + name);
            }
            repository.saveUser(new UserState(name, true, Instant.now(),
                    previous == null || previous.notificationsEnabled()));
            return null;
        });
    }
    @Override
    public void disconnect(String name) {
        validateName(name);
        execute(() -> {
            UserState user = require(name, true);
            repository.saveUser(new UserState(name, false, user.lastConnected(), user.notificationsEnabled()));
            return null;
        });
    }
    @Override
    public void setNotificationsEnabled(String name, boolean enabled) {
        validateName(name);
        execute(() -> {
            UserState user = require(name, true);
            repository.saveUser(new UserState(name, true, user.lastConnected(), enabled));
            return null;
        });
    }
    @Override
    public Message sendMessage(String name, String text) {
        validateName(name);
        validateText(text);
        return execute(() -> {
            require(name, true);
            Message message = repository.addMessage(name, text, Instant.now());
            List<String> recipients = repository.users().stream()
                    .filter(UserState::notificationsEnabled).map(UserState::name).toList();
            repository.enqueue(message.id(), recipients);
            return message;
        });
    }
    @Override
    public List<Message> readNotifications(String name) {
        validateName(name);
        return execute(() -> {
            require(name, true);
            return repository.notifications(name);
        });
    }
    @Override
    public void acknowledgeNotification(String name, long messageId) {
        validateName(name);
        if (messageId <= 0) throw new IllegalArgumentException("Номер сообщения должен быть положительным");
        execute(() -> {
            require(name, true);
            repository.acknowledge(name, messageId);
            return null;
        });
    }
    @Override
    public List<Message> getHistory(String name) {
        validateName(name);
        return execute(() -> {
            require(name, true);
            return repository.history();
        });
    }
    @Override
    public UserState getUserState(String name) {
        validateName(name);
        return execute(() -> require(name, false));
    }
    @Override
    public List<UserState> getUsers() {
        return execute(repository::users);
    }
    @Override
    public synchronized ChatMetrics getMetrics() {
        return repository.getMetrics();
    }
    @Override
    public synchronized void close() {
        repository.close();
    }
    public static void validateName(String name) {
        if (name == null || name.isBlank() || name.length() > 255) {
            throw new IllegalArgumentException("Имя должно содержать от 1 до 255 символов");
        }
    }
    public static void validateText(String text) {
        if (text == null || text.isBlank()) throw new IllegalArgumentException("Сообщение не должно быть пустым");
    }
}
