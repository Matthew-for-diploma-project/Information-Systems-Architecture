package service;

import api.ChatService;
import api.RpcEndpoint;
import rpc.ProtoMapper;
import rpc.generated.ChatWire.Reply;
import rpc.generated.ChatWire.Request;

/** Вход бизнес-слоя. Принимает только пользовательские операции во внутреннем формате. */
public final class ApplicationEndpoint implements RpcEndpoint {
    private final ChatService chat;
    public ApplicationEndpoint(ChatService chat) { this.chat = chat; }
    @Override
    public Reply handle(Request request) {
        String name = request.hasName() ? request.getName() : null;
        Reply.Builder result = Reply.newBuilder();
        switch (request.getOperation()) {
            case CONNECT -> chat.connect(name);
            case DISCONNECT -> chat.disconnect(name);
            case NOTIFICATIONS -> {
                if (!request.hasEnabled()) throw new IllegalArgumentException("Не задана настройка уведомлений");
                chat.setNotificationsEnabled(name, request.getEnabled());
            }
            case SEND -> result.addMessages(ProtoMapper.message(chat.sendMessage(
                    name, request.hasText() ? request.getText() : null)));
            case READ -> chat.readNotifications(name).forEach(m -> result.addMessages(ProtoMapper.message(m)));
            case ACK -> chat.acknowledgeNotification(name, request.getMessageId());
            case HISTORY -> chat.getHistory(name).forEach(m -> result.addMessages(ProtoMapper.message(m)));
            case STATE -> result.addUsers(ProtoMapper.user(chat.getUserState(name)));
            case USERS -> chat.getUsers().forEach(u -> result.addUsers(ProtoMapper.user(u)));
            case METRICS -> result.setMetrics(ProtoMapper.metrics(chat.getMetrics()));
            default -> throw new IllegalArgumentException("Команда не относится к бизнес-слою");
        }
        return result.build();
    }
}
