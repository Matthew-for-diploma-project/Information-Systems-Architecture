package storage;

import api.RpcEndpoint;
import rpc.ProtoMapper;
import rpc.generated.ChatWire.Reply;
import rpc.generated.ChatWire.Request;
import java.time.Instant;

/** Разрешён только фиксированный набор CRUD-команд; произвольного SQL в протоколе нет. */
public final class RepositoryEndpoint implements RpcEndpoint {
    private final ChatRepository repository;
    private ChatTransaction transaction;
    public RepositoryEndpoint(ChatRepository repository) { this.repository = repository; }
    @Override
    public Reply handle(Request request) {
        Reply.Builder reply = Reply.newBuilder();
        if (request.getOperation() == Request.Operation.METRICS) {
            if (transaction != null) throw new IllegalStateException("Транзакция ещё открыта");
            return reply.setMetrics(ProtoMapper.metrics(repository.getMetrics())).build();
        }
        if (request.getOperation() == Request.Operation.BEGIN) {
            if (transaction != null) throw new IllegalStateException("Транзакция уже открыта");
            transaction = repository.begin();
            return reply.build();
        }
        if (transaction == null) throw new IllegalStateException("Сначала BEGIN");
        switch (request.getOperation()) {
            case FIND_USER -> {
                var user = transaction.findUser(request.getName());
                if (user != null) reply.addUsers(ProtoMapper.user(user));
            }
            case SAVE_USER -> transaction.saveUser(ProtoMapper.user(request.getUser()));
            case ADD_MESSAGE -> reply.addMessages(ProtoMapper.message(transaction.addMessage(
                    request.getName(), request.getText(), Instant.parse(request.getCreatedAt()))));
            case ENQUEUE -> transaction.enqueue(request.getMessageId(), request.getRecipientsList());
            case READ -> transaction.notifications(request.getName()).forEach(m -> reply.addMessages(ProtoMapper.message(m)));
            case REMOVE_NOTIFICATION -> transaction.acknowledge(request.getName(), request.getMessageId());
            case HISTORY -> transaction.history().forEach(m -> reply.addMessages(ProtoMapper.message(m)));
            case USERS -> transaction.users().forEach(u -> reply.addUsers(ProtoMapper.user(u)));
            case COMMIT -> { transaction.commit(); close(); }
            default -> throw new IllegalArgumentException("Команда не относится к хранилищу");
        }
        return reply.build();
    }
    @Override
    public void close() {
        if (transaction != null) {
            try { transaction.close(); } finally { transaction = null; }
        }
    }
}
