package repository;

import model.ChatMetrics;
import model.Message;
import model.UserState;
import java.time.Instant;
import java.util.List;

/** Контракт хранения. Одна операция чата выполняется между begin и commit/rollback. */
public interface ChatRepository extends AutoCloseable {
    void begin();
    UserState findUser(String name);
    void saveUser(UserState user);
    Message addMessage(String sender, String text, Instant time);
    List<UserState> users();
    List<Message> history();
    List<Message> notifications(String name);
    void enqueue(long messageId, List<String> recipients);
    void acknowledge(String name, long messageId);
    ChatMetrics getMetrics();
    void commit();
    void rollback();
    @Override
    void close();
}
