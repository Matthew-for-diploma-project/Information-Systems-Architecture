package distributed;

import model.ChatFacade;
import model.TcpChatServer;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/** Единственный внешний вход. Сохраняет текстовый TCP-протокол работы №4. */
public final class FacadeMain {
    private FacadeMain() { }
    public static TcpChatServer start(String businessHost, int businessPort, String host, int port) throws IOException {
        return new TcpChatServer(new ChatFacade(packet -> Wire.call(businessHost, businessPort, packet)), host, port);
    }
    public static void main(String[] args) throws Exception {
        try (var server = start(Wire.env("BUSINESS_HOST", "business"), Integer.parseInt(Wire.env("BUSINESS_PORT", "6001")),
                Wire.env("BIND_HOST", "0.0.0.0"), Integer.parseInt(Wire.env("FACADE_PORT", "5555")))) {
            Runtime.getRuntime().addShutdownHook(new Thread(server::close));
            new CountDownLatch(1).await();
        }
    }
}
