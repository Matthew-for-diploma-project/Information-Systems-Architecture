package distributed;

import org.h2.tools.Server;
import repository.H2ChatRepository;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/** Единственный процесс, открывающий файл H2; storage использует JDBC over TCP. */
public final class DatabaseMain {
    private DatabaseMain() { }
    public static Server start(Path database, int port) throws java.sql.SQLException {
        Path path = database.toAbsolutePath().normalize();
        try (var repository = new H2ChatRepository(path)) { repository.getMetrics(); }
        return Server.createTcpServer("-tcpPort", "" + port, "-tcpAllowOthers",
                "-baseDir", path.getParent().toString()).start();
    }
    public static void main(String[] args) throws Exception {
        Server database = start(Path.of(Wire.env("DB_FILE", "/data/chat")),
                Integer.parseInt(Wire.env("DB_PORT", "9092")));
        Runtime.getRuntime().addShutdownHook(new Thread(database::stop));
        new CountDownLatch(1).await();
    }
}
