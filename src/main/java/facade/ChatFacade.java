package facade;

import api.RpcEndpoint;
import api.TextEndpoint;
import rpc.ProtoMapper;
import rpc.generated.ChatWire.Request;
import service.Validation;
import transport.ChatProtocol;
import java.util.ArrayList;
import java.util.List;

/** Фасад: разбор и проверка внешних полей, преобразование в protobuf и обратно. */
public final class ChatFacade implements TextEndpoint {
    private final RpcEndpoint application;
    public ChatFacade(RpcEndpoint application) { this.application = application; }
    @Override
    public List<String> handle(List<String> fields) {
        if (fields.isEmpty() || fields.get(0) == null) {
            throw new IllegalArgumentException("Команда не задана");
        }
        String command = fields.get(0);
        int size = switch (command) {
            case "CONNECT", "DISCONNECT", "READ", "HISTORY", "STATE" -> 2;
            case "NOTIFICATIONS", "SEND", "ACK" -> 3;
            case "USERS", "METRICS" -> 1;
            default -> throw new IllegalArgumentException("Неизвестная команда: " + command);
        };
        if (fields.size() != size) throw new IllegalArgumentException("Неверное число аргументов");
        Request.Builder request = Request.newBuilder().setOperation(Request.Operation.valueOf(command));
        if (size > 1) {
            Validation.name(fields.get(1));
            request.setName(fields.get(1));
        }
        switch (command) {
            case "SEND" -> { Validation.text(fields.get(2)); request.setText(fields.get(2)); }
            case "NOTIFICATIONS" -> request.setEnabled(ChatProtocol.booleanValue(fields.get(2)));
            case "ACK" -> {
                long id = Long.parseLong(fields.get(2));
                Validation.id(id);
                request.setMessageId(id);
            }
        }
        var reply = ProtoMapper.checked(application.handle(request.build()));
        List<String> result = new ArrayList<>();
        result.add("OK");
        switch (command) {
            case "SEND" -> ChatProtocol.addMessage(result, ProtoMapper.message(reply.getMessages(0)));
            case "READ", "HISTORY" -> {
                result.add(Integer.toString(reply.getMessagesCount()));
                reply.getMessagesList().forEach(m -> ChatProtocol.addMessage(result, ProtoMapper.message(m)));
            }
            case "STATE" -> ChatProtocol.addUser(result, ProtoMapper.user(reply.getUsers(0)));
            case "USERS" -> {
                result.add(Integer.toString(reply.getUsersCount()));
                reply.getUsersList().forEach(u -> ChatProtocol.addUser(result, ProtoMapper.user(u)));
            }
            case "METRICS" -> {
                result.add(Long.toString(reply.getMetrics().getRequests()));
                result.add(Long.toString(reply.getMetrics().getNotifications()));
            }
        }
        return result;
    }
}
