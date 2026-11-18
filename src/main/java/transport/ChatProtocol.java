package transport;

import model.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public final class ChatProtocol {
    private ChatProtocol() {
    }

    public static List<String> read(Reader input) throws IOException {
        StringBuilder frame = new StringBuilder();
        int symbol;

        while ((symbol = input.read()) != -1) {
            if (symbol == 0) {
                List<String> fields = new ArrayList<>();

                for (String field : frame.toString().split("\t", -1)) {
                    fields.add(unescape(field));
                }

                return fields;
            }

            frame.append((char) symbol);
        }

        if (!frame.isEmpty()) {
            throw new EOFException(
                    "Соединение закрыто до конца кадра"
            );
        }

        return null;
    }

    public static void write(
            Writer output,
            List<String> fields
    ) throws IOException {
        StringJoiner frame = new StringJoiner("\t");

        for (String field : fields) {
            frame.add(escape(field));
        }

        output.write(frame.toString());
        output.write(0);
        output.flush();
    }

    private static String escape(String value) {
        if (value == null) {
            return "\\N";
        }

        return value.replace("\\", "\\\\")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\0", "\\0");
    }

    private static String unescape(String value) {
        if (value.equals("\\N")) {
            return null;
        }

        StringBuilder result = new StringBuilder();

        for (int i = 0; i < value.length(); i++) {
            char symbol = value.charAt(i);

            if (symbol == '\\') {
                if (++i == value.length()) {
                    throw new IllegalArgumentException(
                            "Незавершённая escape-последовательность"
                    );
                }

                symbol = switch (value.charAt(i)) {
                    case '\\' -> '\\';
                    case 't' -> '\t';
                    case 'n' -> '\n';
                    case 'r' -> '\r';
                    case '0' -> '\0';
                    default -> throw new IllegalArgumentException(
                            "Неизвестная escape-последовательность"
                    );
                };
            }

            result.append(symbol);
        }

        return result.toString();
    }

    public static boolean booleanValue(String value) {
        if (!"true".equals(value) && !"false".equals(value)) {
            throw new IllegalArgumentException(
                    "Ожидалось true или false"
            );
        }

        return Boolean.parseBoolean(value);
    }

    public static void addMessage(
            List<String> fields,
            Message message
    ) {
        fields.add(Long.toString(message.id()));
        fields.add(message.sender());
        fields.add(message.text());
        fields.add(message.createdAt().toString());
    }

    public static Message message(
            List<String> fields,
            int offset
    ) {
        return new Message(
                Long.parseLong(fields.get(offset)),
                fields.get(offset + 1),
                fields.get(offset + 2),
                Instant.parse(fields.get(offset + 3))
        );
    }

    public static void addUser(
            List<String> fields,
            UserState user
    ) {
        fields.add(user.name());
        fields.add(Boolean.toString(user.online()));
        fields.add(user.lastConnected().toString());
        fields.add(Boolean.toString(user.notificationsEnabled()));
    }

    public static UserState user(
            List<String> fields,
            int offset
    ) {
        return new UserState(
                fields.get(offset),
                booleanValue(fields.get(offset + 1)),
                Instant.parse(fields.get(offset + 2)),
                booleanValue(fields.get(offset + 3))
        );
    }
}