package storage;

import model.Message;
import model.UserState;
import java.time.Instant;
import java.util.List;

/** CRUD в одной транзакции; close без commit выполняет откат. */
public interface ChatTransaction extends AutoCloseable {
    UserState findUser(String name);
    void saveUser(UserState user);
    Message addMessage(String sender, String text, Instant time);
    void enqueue(long messageId, List<String> recipients);
    List<Message> notifications(String name);
    void acknowledge(String name, long messageId);
    List<Message> history();
    List<UserState> users();
    void commit();
    @Override
    void close();
}
