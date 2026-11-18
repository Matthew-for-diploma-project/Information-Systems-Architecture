import api.ChatService;
import model.ChatProtocol;
import model.UserState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TcpProtocolTest {
    @TempDir
    Path directory;

    @Test
    void fragmentedAndCombinedFramesPreserveText() throws Exception {
        try (TestChat test =
                     new TestChat(directory.resolve("frames"));
             Socket socket =
                     new Socket("127.0.0.1", test.port())) {
            socket.setSoTimeout(5000);

            BufferedReader input = new BufferedReader(
                    new InputStreamReader(
                            socket.getInputStream(),
                            StandardCharsets.UTF_8
                    )
            );

            String frames = "CONNECT\tАнна\0"
                    + "SEND\tАнна\tПервая\\nВторая\\tстрока"
                    + "\\\\путь\\0конец\\r\\\\N\0"
                    + "HISTORY\tАнна\0";

            byte[] bytes = frames.getBytes(StandardCharsets.UTF_8);

            int split = "CONNECT\t"
                    .getBytes(StandardCharsets.UTF_8).length + 1;

            socket.getOutputStream().write(
                    bytes,
                    0,
                    split
            );

            socket.getOutputStream().flush();

            socket.getOutputStream().write(
                    bytes,
                    split,
                    bytes.length - split
            );

            socket.getOutputStream().flush();

            assertEquals(
                    List.of("OK"),
                    ChatProtocol.read(input)
            );

            List<String> sent = ChatProtocol.read(input);

            assertEquals("OK", sent.get(0));
            assertEquals("Анна", sent.get(2));

            assertEquals(
                    "Первая\nВторая\tстрока\\путь\0конец\r\\N",
                    sent.get(3)
            );

            List<String> history = ChatProtocol.read(input);

            assertEquals(
                    List.of("OK", "1"),
                    history.subList(0, 2)
            );

            assertEquals(
                    sent.subList(1, 5),
                    history.subList(2, 6)
            );

            assertEquals(
                    ChatProtocol.message(sent, 1),
                    test.chat.getHistory("Анна").get(0)
            );
        }
    }

    @Test
    void invalidCommandsDoNotBreakConnectionOrChangeCounters()
            throws Exception {
        try (TestChat test =
                     new TestChat(directory.resolve("commands"));
             Socket socket =
                     new Socket("127.0.0.1", test.port())) {
            socket.setSoTimeout(5000);

            BufferedReader input = new BufferedReader(
                    new InputStreamReader(
                            socket.getInputStream(),
                            StandardCharsets.UTF_8
                    )
            );

            String frames = "UNKNOWN\0"
                    + "NOTIFICATIONS\tАнна\tнеправда\0"
                    + "ACK\tАнна\tabc\0"
                    + "CONNECT\tАнна\tЛишний\0"
                    + "CONNECT\t\\q\0"
                    + "CONNECT\tАнна\0METRICS\0";

            socket.getOutputStream().write(
                    frames.getBytes(StandardCharsets.UTF_8)
            );

            socket.getOutputStream().flush();

            for (int i = 0; i < 5; i++) {
                assertEquals(
                        List.of("ERROR", "ARGUMENT"),
                        ChatProtocol.read(input).subList(0, 2)
                );
            }

            assertEquals(
                    List.of("OK"),
                    ChatProtocol.read(input)
            );

            assertEquals(
                    List.of("OK", "1", "0"),
                    ChatProtocol.read(input)
            );
        }
    }

    @Test
    void tcpReconnectPreservesStateAndRemoteErrors() throws Exception {
        try (TestChat test =
                     new TestChat(directory.resolve("reconnect"))) {
            try (ChatService client = test.newClient()) {
                client.connect("Анна");
                client.setNotificationsEnabled("Анна", false);
            }

            try (ChatService client = test.newClient()) {
                UserState saved =
                        client.getUserState("Анна");

                assertTrue(saved.online());
                assertFalse(saved.notificationsEnabled());

                long requests =
                        client.getMetrics().getRequestsCount();

                assertThrows(
                        IllegalArgumentException.class,
                        () -> client.connect(null)
                );

                assertThrows(
                        IllegalArgumentException.class,
                        () -> client.sendMessage("Анна", null)
                );

                assertThrows(
                        IllegalStateException.class,
                        () -> client.connect("Анна")
                );

                assertEquals(
                        requests,
                        client.getMetrics().getRequestsCount()
                );

                client.disconnect("Анна");

                assertFalse(
                        client.getUserState("Анна").online()
                );

                assertThrows(
                        IllegalStateException.class,
                        () -> client.sendMessage("Анна", "Ошибка")
                );

                client.connect("Анна");

                assertFalse(
                        client.getUserState("Анна")
                                .notificationsEnabled()
                );
            }
        }
    }

    @Test
    void unfinishedFrameDoesNotExecuteCommand() throws Exception {
        try (TestChat test =
                     new TestChat(directory.resolve("unfinished"))) {
            test.chat.connect("Анна");

            try (Socket socket =
                         new Socket("127.0.0.1", test.port())) {
                socket.getOutputStream().write(
                        "SEND\tАнна\tБез конца кадра"
                                .getBytes(StandardCharsets.UTF_8)
                );

                socket.getOutputStream().flush();
            }

            assertTrue(
                    test.chat.getHistory("Анна").isEmpty()
            );

            assertEquals(
                    0L,
                    test.chat.getMetrics().getNotificationsCount()
            );
        }
    }
}