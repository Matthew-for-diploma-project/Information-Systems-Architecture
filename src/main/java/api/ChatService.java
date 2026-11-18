package api;

import model.ChatMetrics;
import model.Message;
import model.UserState;

import java.util.List;

public interface ChatService extends AutoCloseable {
    void connect(String name);

    void disconnect(String name);

    void setNotificationsEnabled(String name, boolean enabled);

    Message sendMessage(String name, String text);

    List<Message> readNotifications(String name);

    void acknowledgeNotification(String name, long messageId);

    List<Message> getHistory(String name);

    UserState getUserState(String name);

    List<UserState> getUsers();

    ChatMetrics getMetrics();

    @Override
    void close();
}