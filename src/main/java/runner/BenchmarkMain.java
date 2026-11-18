package runner;

import api.ChatService;
import rpc.RemoteEndpoint;
import rpc.generated.ChatWire.Request;
import transport.TcpChatClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.net.ServerSocket;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/** Воспроизводимый замер трёх отдельных JVM. Результаты пишутся только в файлы. */
public final class BenchmarkMain {
    private BenchmarkMain() { }
    private record Configuration(String name, int facade, int application, int storage) { }
    private static final List<Configuration> CONFIGURATIONS = List.of(
            new Configuration("baseline", 1, 1, 1),
            new Configuration("facade-1", 1, 4, 4),
            new Configuration("facade-2", 2, 4, 4),
            new Configuration("application-1", 4, 1, 4),
            new Configuration("application-2", 4, 2, 4),
            new Configuration("storage-1", 4, 4, 1),
            new Configuration("storage-2", 4, 4, 2),
            new Configuration("all-4", 4, 4, 4));

    public static void main(String[] args) throws Exception {
        Path report = Path.of(args.length > 0 ? args[0] : "benchmarks/scaling.csv").toAbsolutePath();
        int repeats = args.length > 1 ? Integer.parseInt(args[1]) : 3;
        int clients = args.length > 2 ? Integer.parseInt(args[2]) : 8;
        int messages = args.length > 3 ? Integer.parseInt(args[3]) : 50;
        if (repeats < 1 || clients < 1 || messages < 1) throw new IllegalArgumentException("Параметры должны быть положительными");
        Files.createDirectories(report.getParent());
        Files.writeString(report, "configuration,trial,facade_workers,application_workers,storage_workers,clients,requests,seconds,requests_per_second,p50_ms,p95_ms,errors\n");
        Files.writeString(report.resolveSibling("scaling-environment.txt"),
                "time=" + Instant.now() + "\njava=" + System.getProperty("java.runtime.version")
                + "\nos=" + System.getProperty("os.name") + " " + System.getProperty("os.version")
                + "\narch=" + System.getProperty("os.arch")
                + "\navailable_processors=" + Runtime.getRuntime().availableProcessors()
                + "\nheap_each=-Xms32m -Xmx128m"
                + "\nmode=three independent JVM processes, loopback TCP, fresh H2 per trial"
                + "\nwarmup=5 SEND per client; measured=SEND; no console output\n");
        // Перемешиваем порядок воспроизводимо, чтобы конфигурация не совпадала со временем запуска.
        for (int trial = 1; trial <= repeats; trial++) {
            List<Configuration> order = new ArrayList<>(CONFIGURATIONS);
            Collections.shuffle(order, new Random(2026 + trial));
            for (Configuration configuration : order) {
                measure(report, trial, configuration, clients, messages);
            }
        }
    }
    private static int[] freePorts() throws Exception {
        try (ServerSocket storage = new ServerSocket(0);
             ServerSocket application = new ServerSocket(0);
             ServerSocket facade = new ServerSocket(0)) {
            return new int[]{storage.getLocalPort(), application.getLocalPort(), facade.getLocalPort()};
        }
    }
    private static Process launch(Path directory, String name, String... args) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java").toString();
        List<String> command = new ArrayList<>(List.of(java, "-Xms32m", "-Xmx128m", "-cp", System.getProperty("java.class.path"), Main.class.getName()));
        command.addAll(List.of(args));
        return new ProcessBuilder(command).redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(directory.resolve(name + ".log").toFile()).start();
    }
    private static void ready(Process process, Path directory, String name, BooleanSupplier probe) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline && process.isAlive()) {
            try { if (probe.getAsBoolean()) return; } catch (RuntimeException ignored) { }
            Thread.sleep(50);
        }
        throw new IllegalStateException("Слой не готов: " + name + "\n" + Files.readString(directory.resolve(name + ".log")));
    }
    private static boolean protoReady(int port) {
        new RemoteEndpoint("127.0.0.1", port).handle(Request.newBuilder().setOperation(Request.Operation.METRICS).build());
        return true;
    }
    private static void stop(Process process) throws Exception {
        process.destroy();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            if (!process.waitFor(5, TimeUnit.SECONDS)) throw new IllegalStateException("Процесс не остановился");
        }
    }
    private static void measure(Path report, int trial, Configuration configuration, int clients, int messages) throws Exception {
        Path directory = Files.createTempDirectory("chat-scaling-");
        List<Process> processes = new ArrayList<>();
        try {
            int[] ports = freePorts();
            int storagePort = ports[0], applicationPort = ports[1], facadePort = ports[2];
            Process storage = launch(directory, "storage", "storage", directory.resolve("chat").toString(),
                    "" + storagePort, "127.0.0.1", "" + configuration.storage());
            processes.add(storage);
            ready(storage, directory, "storage", () -> protoReady(storagePort));
            Process application = launch(directory, "application", "application", "" + applicationPort, "127.0.0.1",
                    "127.0.0.1", "" + storagePort, "" + configuration.application());
            processes.add(application);
            ready(application, directory, "application", () -> protoReady(applicationPort));
            Process facade = launch(directory, "facade", "server", "" + facadePort, "127.0.0.1",
                    "127.0.0.1", "" + applicationPort, "" + configuration.facade());
            processes.add(facade);
            ready(facade, directory, "facade", () -> {
                try (ChatService client = new TcpChatClient("127.0.0.1", facadePort)) { client.getMetrics(); return true; }
                catch (Exception exception) { return false; }
            });
            load(report, trial, configuration, facadePort, clients, messages);
        } finally {
            Collections.reverse(processes);
            for (Process process : processes) stop(process);
            try (var files = Files.walk(directory)) {
                for (Path path : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
            }
        }
    }
    private static void load(Path report, int trial, Configuration configuration, int port, int count, int messages) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(count);
        CountDownLatch connected = new CountDownLatch(count), warm = new CountDownLatch(1);
        CountDownLatch warmed = new CountDownLatch(count), start = new CountDownLatch(1);
        long[][] latency = new long[count][messages];
        List<Future<?>> tasks = new ArrayList<>();
        try {
            for (int n = 0; n < count; n++) {
                int index = n;
                tasks.add(pool.submit(() -> {
                    try (ChatService client = new TcpChatClient("127.0.0.1", port)) {
                        String name = "Участник-" + index;
                        client.connect(name);
                        connected.countDown();
                        if (!warm.await(20, TimeUnit.SECONDS)) throw new IllegalStateException("Нет старта прогрева");
                        for (int i = 0; i < 5; i++) client.sendMessage(name, "Прогрев-" + i);
                        warmed.countDown();
                        if (!start.await(20, TimeUnit.SECONDS)) throw new IllegalStateException("Нет старта замера");
                        for (int i = 0; i < messages; i++) {
                            long begin = System.nanoTime();
                            client.sendMessage(name, "Сообщение-" + i);
                            latency[index][i] = System.nanoTime() - begin;
                        }
                    }
                    return null;
                }));
            }
            if (!connected.await(20, TimeUnit.SECONDS)) throw new IllegalStateException("Клиенты не подключились");
            warm.countDown();
            if (!warmed.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("Прогрев не завершился");
            long begin = System.nanoTime();
            start.countDown();
            for (Future<?> task : tasks) task.get(120, TimeUnit.SECONDS);
            double seconds = (System.nanoTime() - begin) / 1_000_000_000.0;
            long[] sorted = Arrays.stream(latency).flatMapToLong(Arrays::stream).sorted().toArray();
            try (ChatService client = new TcpChatClient("127.0.0.1", port)) {
                long total = (long) count * (messages + 5);
                var metrics = client.getMetrics();
                if (metrics.getRequestsCount() != count + total || metrics.getNotificationsCount() != count * total
                        || client.getHistory("Участник-0").size() != total) {
                    throw new IllegalStateException("Нагрузка нарушила целостность данных");
                }
            }
            String row = String.format(Locale.ROOT, "%s,%d,%d,%d,%d,%d,%d,%.6f,%.3f,%.3f,%.3f,0%n",
                    configuration.name(), trial, configuration.facade(), configuration.application(), configuration.storage(),
                    count, sorted.length, seconds, sorted.length / seconds,
                    sorted[(int) Math.ceil(sorted.length * .50) - 1] / 1_000_000.0,
                    sorted[(int) Math.ceil(sorted.length * .95) - 1] / 1_000_000.0);
            Files.writeString(report, row, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        } finally {
            warm.countDown(); start.countDown(); pool.shutdownNow();
            if (!pool.awaitTermination(15, TimeUnit.SECONDS)) throw new IllegalStateException("Нагрузочные потоки не остановились");
        }
    }
}
