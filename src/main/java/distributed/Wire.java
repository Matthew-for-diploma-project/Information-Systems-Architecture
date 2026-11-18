package distributed;

import api.ChatService;
import model.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/** Общие функции внутреннего обмена, как Wire в примере лабораторной. */
public final class Wire {
    private static final Map<String, java.util.concurrent.atomic.AtomicInteger> NEXT = new ConcurrentHashMap<>();
    static {
        java.security.Security.setProperty("networkaddress.cache.ttl", "0");
        java.security.Security.setProperty("networkaddress.cache.negative.ttl", "0");
    }
    private Wire() { }
    public static String env(String key, String fallback) { return System.getenv().getOrDefault(key, fallback); }
    public static List<InetAddress> addresses(String hosts) throws UnknownHostException {
        List<InetAddress> result = new ArrayList<>();
        for (String host : hosts.split(",")) result.addAll(List.of(InetAddress.getAllByName(host.trim())));
        // DNS может возвращать адреса в разном порядке; стабилизируем список для round-robin.
        return result.stream().distinct().sorted(Comparator.comparing(InetAddress::getHostAddress)).toList();
    }
    public static Socket connect(String host, int port) throws IOException {
        List<InetAddress> addresses = addresses(host);
        int next = NEXT.computeIfAbsent(host + ":" + port, k -> new java.util.concurrent.atomic.AtomicInteger()).getAndIncrement();
        IOException last = null;
        for (int n = 0; n < addresses.size(); n++) {
            Socket socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(addresses.get(Math.floorMod(next + n, addresses.size())), port), 3000);
                socket.setSoTimeout(15000); socket.setTcpNoDelay(true);
                return socket;
            } catch (IOException error) { last = error; socket.close(); }
        }
        throw new IOException("Недоступен внутренний слой: " + host + ":" + port, last);
    }
    public static Packet exchange(Socket socket, Packet request) {
        try {
            request.writeTo(socket.getOutputStream());
            Packet reply = Packet.readFrom(socket.getInputStream());
            if (reply == null) throw new EOFException("Внутренний слой закрыл соединение");
            return checked(reply);
        } catch (IOException error) { throw new IllegalStateException("Ошибка внутреннего TCP", error); }
    }
    public static Packet call(String host, int port, Packet request) {
        try (Socket socket = connect(host, port)) { return exchange(socket, request); }
        catch (IOException error) { throw new IllegalStateException("Нет связи с внутренним слоем", error); }
    }
    public static Packet checked(Packet reply) {
        if (reply.operation().equals("ERROR")) {
            if (reply.values().get(0).equals("ARGUMENT")) throw new IllegalArgumentException(reply.values().get(1));
            throw new IllegalStateException(reply.values().get(1));
        }
        return reply;
    }
    public static Packet error(RuntimeException error) {
        String code = error instanceof IllegalArgumentException ? "ARGUMENT"
                : error instanceof IllegalStateException ? "STATE" : "SERVER";
        return Packet.of("ERROR", code, Objects.toString(error.getMessage(), "Ошибка операции"));
    }
    public static Packet message(Message message) {
        List<String> values = new ArrayList<>(); ChatProtocol.addMessage(values, message);
        return new Packet("OK", values);
    }
    public static Packet user(UserState user) {
        List<String> values = new ArrayList<>();
        if (user != null) ChatProtocol.addUser(values, user);
        return new Packet("OK", values);
    }
    public static Packet messages(List<Message> messages) {
        List<String> values = new ArrayList<>(); values.add("" + messages.size());
        messages.forEach(m -> ChatProtocol.addMessage(values, m)); return new Packet("OK", values);
    }
    public static Packet users(List<UserState> users) {
        List<String> values = new ArrayList<>(); values.add("" + users.size());
        users.forEach(u -> ChatProtocol.addUser(values, u)); return new Packet("OK", values);
    }
    public static List<Message> messages(Packet packet) {
        List<Message> result = new ArrayList<>();
        int count = Integer.parseInt(packet.values().get(0));
        for (int n = 0; n < count; n++) result.add(ChatProtocol.message(packet.values(), 1 + n * 4));
        return List.copyOf(result);
    }
    public static List<UserState> users(Packet packet) {
        List<UserState> result = new ArrayList<>();
        int count = Integer.parseInt(packet.values().get(0));
        for (int n = 0; n < count; n++) result.add(ChatProtocol.user(packet.values(), 1 + n * 4));
        return List.copyOf(result);
    }
    public static Packet dispatch(ChatService chat, Packet request) {
        List<String> a = request.values();
        switch (request.operation()) {
            case "CONNECT" -> chat.connect(a.get(0));
            case "DISCONNECT" -> chat.disconnect(a.get(0));
            case "NOTIFICATIONS" -> chat.setNotificationsEnabled(a.get(0), ChatProtocol.booleanValue(a.get(1)));
            case "SEND" -> { return message(chat.sendMessage(a.get(0), a.get(1))); }
            case "READ" -> { return messages(chat.readNotifications(a.get(0))); }
            case "ACK" -> chat.acknowledgeNotification(a.get(0), Long.parseLong(a.get(1)));
            case "HISTORY" -> { return messages(chat.getHistory(a.get(0))); }
            case "STATE" -> { return user(chat.getUserState(a.get(0))); }
            case "USERS" -> { return users(chat.getUsers()); }
            case "METRICS" -> {
                var metrics = chat.getMetrics();
                return Packet.of("OK", "" + metrics.getRequestsCount(), "" + metrics.getNotificationsCount());
            }
            default -> throw new IllegalArgumentException("Неизвестная операция чата");
        }
        return Packet.of("OK");
    }
    public static void serve(Socket socket, java.util.function.Function<Packet, Packet> handler) throws IOException {
        var input = new BufferedInputStream(socket.getInputStream());
        var output = new BufferedOutputStream(socket.getOutputStream());
        Packet request;
        while ((request = Packet.readFrom(input)) != null) {
            Packet reply;
            try { reply = handler.apply(request); }
            catch (RuntimeException error) { reply = error(error); }
            reply.writeTo(output);
            output.flush();
        }
    }
    @FunctionalInterface
    public interface ConnectionHandler { void handle(Socket socket) throws IOException; }

    /** Небольшой общий TCP-сервер. Число реплик меняется снаружи, не числом пулов. */
    public static final class Server implements AutoCloseable {
        private final ServerSocket listener = new ServerSocket();
        private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
        private final ExecutorService workers = Executors.newCachedThreadPool();
        private volatile boolean closed;
        public Server(String host, int port, ConnectionHandler handler) throws IOException {
            listener.setReuseAddress(true);
            try { listener.bind(new InetSocketAddress(host, port)); }
            catch (IOException error) { listener.close(); workers.shutdown(); throw error; }
            workers.execute(() -> {
                while (!closed) {
                    try {
                        Socket socket = listener.accept(); clients.add(socket);
                        if (closed) { socket.close(); clients.remove(socket); continue; }
                        workers.execute(() -> {
                            try (socket) { socket.setSoTimeout(15000); socket.setTcpNoDelay(true); handler.handle(socket); }
                            catch (IOException | RuntimeException ignored) { /* Соединение закрыто; JDBC-сеанс откатывается. */ }
                            finally { clients.remove(socket); }
                        });
                    } catch (IOException | RejectedExecutionException error) {
                        if (!closed) throw new IllegalStateException("Ошибка TCP-сервера", error);
                    }
                }
            });
        }
        public int port() { return listener.getLocalPort(); }
        @Override
        public synchronized void close() {
            if (closed) return; closed = true;
            try { listener.close(); } catch (IOException ignored) { }
            for (Socket socket : clients) { try { socket.close(); } catch (IOException ignored) { } }
            // Не interrupt JDBC rollback: interrupt может закрыть файловый канал H2.
            workers.shutdown();
            try { if (!workers.awaitTermination(20, TimeUnit.SECONDS)) throw new IllegalStateException("Обработчики не завершились"); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); throw new IllegalStateException(error); }
        }
    }
}
