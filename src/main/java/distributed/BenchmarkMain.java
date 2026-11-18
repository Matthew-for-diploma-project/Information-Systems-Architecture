package distributed;

import model.TcpChatClient;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/** Одна read-only нагрузка. Реплики масштабирует внешний скрипт, как в примере lab5. */
public final class BenchmarkMain {
    private BenchmarkMain() { }
    public static void main(String[] args) throws Exception {
        String host = Wire.env("CHAT_HOST", "facade");
        int port = Integer.parseInt(Wire.env("CHAT_PORT", "5555"));
        int requests = Integer.parseInt(Wire.env("BENCH_REQUESTS", "600"));
        int threads = Integer.parseInt(Wire.env("BENCH_THREADS", "12"));
        if (requests < threads || threads < 1) throw new IllegalArgumentException("Запросов должно быть не меньше числа потоков");
        var addresses = Wire.addresses(host);
        if (addresses.isEmpty()) throw new IllegalStateException("Нет адресов фасада");
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads), start = new CountDownLatch(1);
        List<Future<?>> jobs = new ArrayList<>();
        long[] latency = new long[requests];
        try {
            for (int n = 0; n < threads; n++) {
                int index = n;
                jobs.add(pool.submit(() -> {
                    try (var chat = new TcpChatClient(addresses.get(index % addresses.size()).getHostAddress(), port)) {
                        for (int warm = 0; warm < 5; warm++) chat.getMetrics();
                        ready.countDown();
                        if (!start.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("Нет старта нагрузки");
                        for (int i = index; i < requests; i += threads) {
                            long begin = System.nanoTime();
                            chat.getMetrics();
                            latency[i] = System.nanoTime() - begin;
                        }
                    }
                    return null;
                }));
            }
            if (!ready.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("Не все клиенты готовы");
            long begin = System.nanoTime(); start.countDown();
            for (var job : jobs) job.get(90, TimeUnit.SECONDS);
            double seconds = (System.nanoTime() - begin) / 1_000_000_000.0;
            Arrays.sort(latency);
            String row = String.format(Locale.ROOT, "%s,%s,%d,%d,%d,0,%.3f,%.3f,%.3f%n",
                    Wire.env("BENCH_LAYER", "local"), Wire.env("BENCH_REPLICAS", "1"), threads,
                    requests, requests, requests / seconds,
                    latency[(int) Math.ceil(requests * .50) - 1] / 1_000_000.0,
                    latency[(int) Math.ceil(requests * .95) - 1] / 1_000_000.0);
            Path report = Path.of(Wire.env("BENCH_RESULT_FILE", "/results/benchmark-last.csv"));
            Files.createDirectories(report.toAbsolutePath().getParent());
            Files.writeString(report, row);
        } finally {
            start.countDown(); pool.shutdownNow();
            if (!pool.awaitTermination(20, TimeUnit.SECONDS)) throw new IllegalStateException("Нагрузка не остановилась");
        }
    }
}
