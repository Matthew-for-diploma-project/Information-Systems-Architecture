package model;

import api.Facade;
import distributed.Packet;
import distributed.Wire;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Проверка внешнего запроса и преобразование в простой protobuf Packet. */
public final class ChatFacade implements Facade {
    private final Function<Packet, Packet> business;
    public ChatFacade(Function<Packet, Packet> business) { this.business = business; }
    public static Packet parse(List<String> fields) {
        if (fields.isEmpty() || fields.get(0) == null) throw new IllegalArgumentException("Команда не задана");
        String operation = fields.get(0);
        int count = switch (operation) {
            case "CONNECT", "DISCONNECT", "READ", "HISTORY", "STATE" -> 2;
            case "NOTIFICATIONS", "SEND", "ACK" -> 3;
            case "USERS", "METRICS" -> 1;
            default -> throw new IllegalArgumentException("Неизвестная команда: " + operation);
        };
        if (fields.size() != count) throw new IllegalArgumentException("Неверное число аргументов");
        if (count > 1) ChatServer.validateName(fields.get(1));
        switch (operation) {
            case "SEND" -> ChatServer.validateText(fields.get(2));
            case "NOTIFICATIONS" -> ChatProtocol.booleanValue(fields.get(2));
            case "ACK" -> {
                if (Long.parseLong(fields.get(2)) <= 0) throw new IllegalArgumentException("Номер должен быть положительным");
            }
        }
        return new Packet(operation, fields.subList(1, fields.size()));
    }
    @Override
    public List<String> handle(List<String> fields) {
        Packet reply = Wire.checked(business.apply(parse(fields)));
        List<String> result = new ArrayList<>(); result.add("OK"); result.addAll(reply.values());
        return result;
    }
}
