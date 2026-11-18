package storage;

import model.ChatMetrics;

/** Единственный вход в слой хранения. BEGIN/COMMIT охватывают одну операцию API. */
public interface ChatRepository extends AutoCloseable {
    ChatTransaction begin();
    ChatMetrics getMetrics();
    @Override
    void close();
}
