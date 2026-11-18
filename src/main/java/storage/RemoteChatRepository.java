package storage;

import model.ChatMetrics;
import model.Message;
import model.UserState;
import rpc.ProtoConnection;
import rpc.ProtoMapper;
import rpc.generated.ChatWire.Request;
import java.time.Instant;
import java.util.List;
import static rpc.generated.ChatWire.Request.Operation.*;

/** Реализация того же репозитория через protobuf, без JDBC в слое логики. */
public final class RemoteChatRepository implements ChatRepository {
    private final String host;
    private final int port;
    public RemoteChatRepository(String host, int port) { this.host = host; this.port = port; }
    @Override
    public ChatTransaction begin() {
        ProtoConnection channel = new ProtoConnection(host, port);
        try {
            channel.handle(Request.newBuilder().setOperation(BEGIN).build());
            return new RemoteTransaction(channel);
        } catch (RuntimeException exception) { channel.close(); throw exception; }
    }
    @Override
    public ChatMetrics getMetrics() {
        try (ProtoConnection channel = new ProtoConnection(host, port)) {
            return ProtoMapper.metrics(channel.handle(Request.newBuilder().setOperation(METRICS).build()).getMetrics());
        }
    }
    @Override
    public void close() { }

    private record RemoteTransaction(ProtoConnection channel) implements ChatTransaction {
        private Request.Builder request(Request.Operation operation) { return Request.newBuilder().setOperation(operation); }
        @Override
        public UserState findUser(String name) {
            var reply = channel.handle(request(FIND_USER).setName(name).build());
            return reply.getUsersCount() == 0 ? null : ProtoMapper.user(reply.getUsers(0));
        }
        @Override
        public void saveUser(UserState user) {
            channel.handle(request(SAVE_USER).setUser(ProtoMapper.user(user)).build());
        }
        @Override
        public Message addMessage(String sender, String text, Instant time) {
            return ProtoMapper.message(channel.handle(request(ADD_MESSAGE).setName(sender).setText(text)
                    .setCreatedAt(time.toString()).build()).getMessages(0));
        }
        @Override
        public void enqueue(long id, List<String> recipients) {
            channel.handle(request(ENQUEUE).setMessageId(id).addAllRecipients(recipients).build());
        }
        @Override
        public List<Message> notifications(String name) {
            return channel.handle(request(READ).setName(name).build()).getMessagesList().stream()
                    .map(ProtoMapper::message).toList();
        }
        @Override
        public void acknowledge(String name, long id) {
            channel.handle(request(REMOVE_NOTIFICATION).setName(name).setMessageId(id).build());
        }
        @Override
        public List<Message> history() {
            return channel.handle(request(HISTORY).build()).getMessagesList().stream().map(ProtoMapper::message).toList();
        }
        @Override
        public List<UserState> users() {
            return channel.handle(request(USERS).build()).getUsersList().stream().map(ProtoMapper::user).toList();
        }
        @Override
        public void commit() { channel.handle(request(COMMIT).build()); }
        @Override
        public void close() { channel.close(); }
    }
}
