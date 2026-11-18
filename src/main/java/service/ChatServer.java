package service;

import api.ChatService;
import model.ChatMetrics;
import model.Message;
import model.UserState;
import storage.ChatRepository;
import storage.ChatTransaction;
import java.time.Clock;
import java.util.List;
import java.util.function.Function;

/** Правила чата. Не знает JDBC, TCP, текстовый протокол и способ размещения БД. */
public final class ChatServer implements ChatService {
    private final ChatRepository repository;
    private final Clock clock;

    public ChatServer(ChatRepository repository) {
        this(repository, Clock.systemUTC());
    }
    public ChatServer(ChatRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }
    private <T> T execute(Function<ChatTransaction, T> action) {
        try (ChatTransaction transaction = repository.begin()) {
            T result = action.apply(transaction);
            transaction.commit();
            return result;
        }
    }
    private UserState require(ChatTransaction tx, String name, boolean online) {
        UserState user = tx.findUser(name);
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
        Validation.name(name);
        execute(tx -> {
            UserState previous = tx.findUser(name);
            if (previous != null && previous.online()) {
                throw new IllegalStateException("Пользователь уже в сети: " + name);
            }
            tx.saveUser(new UserState(name, true, clock.instant(),
                    previous == null || previous.notificationsEnabled()));
            return null;
        });
    }
    @Override
    public void disconnect(String name) {
        Validation.name(name);
        execute(tx -> {
            UserState user = require(tx, name, true);
            tx.saveUser(new UserState(name, false, user.lastConnected(), user.notificationsEnabled()));
            return null;
        });
    }
    @Override
    public void setNotificationsEnabled(String name, boolean enabled) {
        Validation.name(name);
        execute(tx -> {
            UserState user = require(tx, name, true);
            tx.saveUser(new UserState(name, true, user.lastConnected(), enabled));
            return null;
        });
    }
    @Override
    public Message sendMessage(String name, String text) {
        Validation.name(name);
        Validation.text(text);
        return execute(tx -> {
            require(tx, name, true);
            Message message = tx.addMessage(name, text, clock.instant());
            List<String> recipients = tx.users().stream()
                    .filter(UserState::notificationsEnabled).map(UserState::name).toList();
            tx.enqueue(message.id(), recipients);
            return message;
        });
    }
    @Override
    public List<Message> readNotifications(String name) {
        Validation.name(name);
        return execute(tx -> {
            require(tx, name, true);
            return tx.notifications(name);
        });
    }
    @Override
    public void acknowledgeNotification(String name, long messageId) {
        Validation.name(name);
        Validation.id(messageId);
        execute(tx -> {
            require(tx, name, true);
            tx.acknowledge(name, messageId);
            return null;
        });
    }
    @Override
    public List<Message> getHistory(String name) {
        Validation.name(name);
        return execute(tx -> {
            require(tx, name, true);
            return tx.history();
        });
    }
    @Override
    public UserState getUserState(String name) {
        Validation.name(name);
        return execute(tx -> require(tx, name, false));
    }
    @Override
    public List<UserState> getUsers() {
        return execute(ChatTransaction::users);
    }
    @Override
    public ChatMetrics getMetrics() {
        return repository.getMetrics();
    }
    @Override
    public void close() {
        repository.close();
    }
}
