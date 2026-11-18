package rpc;

import model.ChatMetrics;
import model.Message;
import model.UserState;
import rpc.generated.ChatWire;
import rpc.generated.ChatWire.Reply;
import java.time.Instant;

public final class ProtoMapper {
    private ProtoMapper() {
    }
    public static ChatWire.Message message(Message value) {
        return ChatWire.Message.newBuilder().setId(value.id()).setSender(value.sender())
                .setText(value.text()).setCreatedAt(value.createdAt().toString()).build();
    }
    public static Message message(ChatWire.Message value) {
        return new Message(value.getId(), value.getSender(), value.getText(), Instant.parse(value.getCreatedAt()));
    }
    public static ChatWire.User user(UserState value) {
        return ChatWire.User.newBuilder().setName(value.name()).setOnline(value.online())
                .setLastConnected(value.lastConnected().toString())
                .setNotificationsEnabled(value.notificationsEnabled()).build();
    }
    public static UserState user(ChatWire.User value) {
        return new UserState(value.getName(), value.getOnline(), Instant.parse(value.getLastConnected()),
                value.getNotificationsEnabled());
    }
    public static ChatWire.Metrics metrics(ChatMetrics value) {
        return ChatWire.Metrics.newBuilder().setRequests(value.getRequestsCount())
                .setNotifications(value.getNotificationsCount()).build();
    }
    public static ChatMetrics metrics(ChatWire.Metrics value) {
        return new ChatMetrics(value.getRequests(), value.getNotifications());
    }
    public static Reply error(RuntimeException error) {
        Reply.Status status = error instanceof IllegalArgumentException ? Reply.Status.ARGUMENT
                : error instanceof IllegalStateException ? Reply.Status.STATE : Reply.Status.SERVER;
        return Reply.newBuilder().setStatus(status)
                .setError(error.getMessage() == null ? "Ошибка операции" : error.getMessage()).build();
    }
    public static Reply checked(Reply reply) {
        if (reply.getStatus() == Reply.Status.ARGUMENT) throw new IllegalArgumentException(reply.getError());
        if (reply.getStatus() != Reply.Status.OK) throw new IllegalStateException(reply.getError());
        return reply;
    }
}
