import api.ChatService;
import model.Message;
import model.UserState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class RestartTest {
    @TempDir
    Path directory;

    @Test
    void normalProcessRestartPreservesState() throws Exception {
        checkRestart("normal");
    }

    @Test
    void abruptProcessRestartPreservesState() throws Exception {
        checkRestart("crash");
    }

    @Test
    void abruptRestartRollsBackUncommittedChanges() throws Exception {
        checkRestart("uncommitted");
    }

    private void checkRestart(String mode) throws Exception {
        Path database = directory.resolve(mode);

        String executable = Path.of(
                System.getProperty("java.home"),
                "bin",
                System.getProperty("os.name").startsWith("Windows")
                        ? "java.exe"
                        : "java"
        ).toString();

        String classPath = System.getProperty(
                "surefire.test.class.path",
                System.getProperty("java.class.path")
        );

        Process process = new ProcessBuilder(
                executable,
                "-cp",
                classPath,
                RestartWriter.class.getName(),
                database.toString(),
                mode
        ).redirectErrorStream(true).start();

        try {
            assertTrue(
                    process.waitFor(20, TimeUnit.SECONDS),
                    "Дочерний процесс не завершился"
            );

            String output = new String(
                    process.getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8
            );

            assertEquals(0, process.exitValue(), output);
        } finally {
            process.destroyForcibly();
        }

        UserState saved;

        try (TestChat test = new TestChat(database)) {
            ChatService chat = test.chat;

            assertEquals(
                    10L,
                    chat.getMetrics().getRequestsCount()
            );

            assertEquals(
                    2L,
                    chat.getMetrics().getNotificationsCount()
            );

            saved = chat.getUserState("Анна");

            assertTrue(saved.online());
            assertNotNull(saved.lastConnected());

            assertFalse(
                    chat.getUserState("Вера")
                            .notificationsEnabled()
            );

            assertFalse(
                    chat.getUserState("Борис").online()
            );

            chat.connect("Борис");

            List<Message> history =
                    chat.getHistory("Борис");

            assertEquals(1, history.size());

            assertEquals(
                    "До перезапуска.",
                    history.get(0).text()
            );

            assertTrue(
                    chat.readNotifications("Анна").isEmpty()
            );

            assertTrue(
                    chat.readNotifications("Вера").isEmpty()
            );

            assertEquals(
                    history,
                    chat.readNotifications("Борис")
            );

            chat.acknowledgeNotification(
                    "Борис",
                    history.get(0).id()
            );

            chat.acknowledgeNotification(
                    "Борис",
                    history.get(0).id()
            );
        }

        try (TestChat test = new TestChat(database)) {
            ChatService chat = test.chat;

            assertEquals(
                    saved,
                    chat.getUserState("Анна")
            );

            assertTrue(
                    chat.readNotifications("Борис").isEmpty()
            );

            Message next = chat.sendMessage(
                    "Борис",
                    "После перезапуска."
            );

            assertTrue(next.id() > 1);

            assertEquals(
                    2,
                    chat.getHistory("Борис").size()
            );
        }
    }
}