package runner;

import api.ChatService;
import model.Message;
import model.TcpChatClient;
import model.TcpChatServer;
import model.UserState;

import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class Main {
    private static final int PHASE_COUNT = 11;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        String mode = args.length == 0 ? "demo" : args[0];

        switch (mode) {
            case "facade" -> { distributed.FacadeMain.main(args); return; }
            case "business" -> { distributed.BusinessMain.main(args); return; }
            case "storage" -> { distributed.StorageMain.main(args); return; }
            case "database" -> { distributed.DatabaseMain.main(args); return; }
            case "benchmark" -> { distributed.BenchmarkMain.main(args); return; }
            case "proto-health" -> {
                try { distributed.Wire.call(args[1], Integer.parseInt(args[2]), distributed.Packet.of("METRICS")); }
                catch (RuntimeException error) { System.exit(1); }
                return;
            }
            case "db-health" -> {
                try (var connection = java.sql.DriverManager.getConnection(args[1], "sa", "")) {
                    if (!connection.isValid(2)) System.exit(1);
                } catch (java.sql.SQLException error) { System.exit(1); }
                return;
            }
        }

        if (mode.equals("health")) {
            String host = args.length > 1
                    ? args[1]
                    : "127.0.0.1";

            int port = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 5555;

            try (ChatService client =
                         new TcpChatClient(host, port)) {
                client.getMetrics();
            } catch (Exception exception) {
                System.exit(1);
            }

            return;
        }

        if (mode.equals("client")) {
            String host = args.length > 1
                    ? args[1]
                    : "127.0.0.1";

            int port = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 5555;

            runDemo(host, port);
            return;
        }

        if (mode.equals("server")) {
            Path database = args.length > 1
                    ? Path.of(args[1])
                    : Path.of("data", "chat");

            int port = args.length > 2
                    ? Integer.parseInt(args[2])
                    : 5555;

            String host = args.length > 3
                    ? args[3]
                    : "127.0.0.1";

            try (TcpChatServer server =
                         new TcpChatServer(database, host, port)) {
                Runtime.getRuntime().addShutdownHook(
                        new Thread(server::close)
                );

                new CountDownLatch(1).await();
            }

            return;
        }

        Path database = args.length == 0
                ? Path.of("data", "chat")
                : Path.of(args[0]);

        try (TcpChatServer server =
                     new TcpChatServer(database, 0)) {
            runDemo("127.0.0.1", server.getPort());
        }
    }

    private static void runDemo(
            String host,
            int port
    ) throws Exception {
        try (ChatService service =
                     new TcpChatClient(host, port)) {
            Map<String, UserState> restored = new HashMap<>();

            for (UserState user : service.getUsers()) {
                restored.put(user.name(), user);

                // System.out.println(
                //         "Восстановлено: " + formatState(user)
                // );
            }

            ExecutorService pool =
                    Executors.newFixedThreadPool(3);

            CountDownLatch[] completed =
                    new CountDownLatch[PHASE_COUNT];

            CountDownLatch[] printed =
                    new CountDownLatch[PHASE_COUNT];

            String[] output = new String[3];

            for (int i = 0; i < PHASE_COUNT; i++) {
                completed[i] = new CountDownLatch(3);
                printed[i] = new CountDownLatch(1);
            }

            try {
                List<Future<?>> tasks = new ArrayList<>();
                String[] names = {"Анна", "Вера", "Борис"};

                for (int i = 0; i < names.length; i++) {
                    int index = i;

                    tasks.add(pool.submit(() -> {
                        try (ChatService client =
                                     new TcpChatClient(host, port)) {
                            runClient(
                                    client,
                                    names[index],
                                    index,
                                    restored.get(names[index]),
                                    completed,
                                    printed,
                                    output
                            );
                        }

                        return null;
                    }));
                }

                for (int phase = 0; phase < PHASE_COUNT; phase++) {
                    await(completed[phase]);

                    // for (String text : output) {
                    //     if (!text.isEmpty()) {
                    //         System.out.println(text);
                    //     }
                    // }

                    printed[phase].countDown();
                }

                for (Future<?> task : tasks) {
                    task.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();

                for (CountDownLatch latch : printed) {
                    latch.countDown();
                }

                pool.awaitTermination(5, TimeUnit.SECONDS);
            }

            // System.out.println(
            //         "Обработано запросов: "
            //                 + service.getMetrics().getRequestsCount()
            // );

            // System.out.println(
            //         "Создано уведомлений: "
            //                 + service.getMetrics().getNotificationsCount()
            // );
        }
    }

    private static void runClient(
            ChatService service,
            String name,
            int index,
            UserState previous,
            CountDownLatch[] completed,
            CountDownLatch[] printed,
            String[] output
    ) {
        boolean anna = name.equals("Анна");
        boolean vera = name.equals("Вера");
        boolean boris = name.equals("Борис");

        boolean wasOnline = previous != null && previous.online();

        try {
            for (int phase = 0; phase < PHASE_COUNT; phase++) {
                String text = "";
                List<Message> notifications = List.of();

                switch (phase) {
                    case 0 -> {
                        if (boris) {
                            text = wasOnline
                                    ? "[Борис] Восстановлен: в сети."
                                    : "[Борис] Не в сети.";
                        } else {
                            if (!wasOnline) {
                                service.connect(name);
                            }

                            text = "[" + name + "] В сети.";
                        }
                    }

                    case 1 -> {
                        if (anna) {
                            text = send(
                                    service,
                                    name,
                                    "Всем привет!"
                            );
                        }
                    }

                    case 2, 5, 9 -> {
                        if (phase != 2 || !boris) {
                            notifications =
                                    service.readNotifications(name);

                            text = formatMessages(
                                    name,
                                    "Уведомление",
                                    notifications
                            );
                        }
                    }

                    case 3 -> {
                        if (boris) {
                            if (!wasOnline) {
                                service.connect(name);
                            }

                            service.setNotificationsEnabled(
                                    name,
                                    false
                            );

                            text = "[Борис] В сети. "
                                    + "Уведомления выключены.";
                        }
                    }

                    case 4 -> {
                        if (vera) {
                            text = send(
                                    service,
                                    name,
                                    "Продолжим обсуждение лабораторной."
                            );
                        }
                    }

                    case 6 -> {
                        if (boris) {
                            text = formatMessages(
                                    name,
                                    "История",
                                    service.getHistory(name)
                            );
                        }
                    }

                    case 7 -> {
                        if (boris) {
                            service.setNotificationsEnabled(
                                    name,
                                    true
                            );

                            text = "[Борис] Уведомления включены.";
                        }
                    }

                    case 8 -> {
                        if (boris) {
                            text = send(
                                    service,
                                    name,
                                    "Привет! Я включил уведомления."
                            );
                        }
                    }

                    case 10 -> {
                        if (boris) {
                            text = "Состояние: "
                                    + formatState(
                                    service.getUserState(name)
                            );
                        }
                    }
                }

                output[index] = text;
                completed[phase].countDown();
                await(printed[phase]);

                // Подтверждаем обработанные уведомления
                // после разрешения main.
                // Отключение печати не отменяет обработку.
                for (Message message : notifications) {
                    service.acknowledgeNotification(
                            name,
                            message.id()
                    );
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();

            throw new IllegalStateException(
                    "Поток пользователя прерван: " + name,
                    exception
            );
        }
    }

    private static void await(CountDownLatch latch)
            throws InterruptedException {
        if (!latch.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException(
                    "Не удалось дождаться завершения этапа"
            );
        }
    }

    private static String send(
            ChatService service,
            String name,
            String text
    ) {
        return "[" + name + "] Отправлено: "
                + service.sendMessage(name, text);
    }

    private static String formatMessages(
            String name,
            String label,
            List<Message> messages
    ) {
        if (messages.isEmpty()) {
            return "[" + name + "] " + label
                    + ": новых сообщений нет.";
        }

        StringJoiner lines =
                new StringJoiner(System.lineSeparator());

        for (Message message : messages) {
            lines.add(
                    "[" + name + "] " + label + ": " + message
            );
        }

        return lines.toString();
    }

    private static String formatState(UserState user) {
        String time = DateTimeFormatter
                .ofPattern("dd.MM.yyyy HH:mm:ss")
                .withZone(ZoneId.systemDefault())
                .format(user.lastConnected());

        return "[" + user.name() + "] "
                + (user.online() ? "в сети" : "не в сети")
                + "; уведомления "
                + (user.notificationsEnabled()
                ? "включены"
                : "выключены")
                + "; последнее подключение: " + time;
    }
}