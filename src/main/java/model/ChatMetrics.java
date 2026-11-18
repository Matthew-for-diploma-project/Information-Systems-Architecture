package model;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

public class ChatMetrics {
    private final PrometheusRegistry registry = new PrometheusRegistry();

    private final Counter requests = Counter.builder()
            .name("chat_requests_total")
            .help("Successfully processed chat API requests")
            .register(registry);

    private final Counter notifications = Counter.builder()
            .name("chat_notifications_total")
            .help("Notifications created for chat users")
            .register(registry);

    ChatMetrics(long requestsCount, long notificationsCount) {
        update(requestsCount, notificationsCount);
    }

    void update(long requestsCount, long notificationsCount) {
        requests.inc(requestsCount - getRequestsCount());
        notifications.inc(notificationsCount - getNotificationsCount());
    }

    public long getRequestsCount() {
        return (long) requests.get();
    }

    public long getNotificationsCount() {
        return (long) notifications.get();
    }
}