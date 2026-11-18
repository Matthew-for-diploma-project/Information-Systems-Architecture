package distributed;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/** Реальный protobuf: поле 1 — операция, поле 2 — повторяющиеся строки. */
public record Packet(String operation, List<String> values) {
    private static final int MAX_BYTES = 64 * 1024 * 1024;
    public Packet {
        java.util.Objects.requireNonNull(operation);
        values = List.copyOf(values);
    }
    public static Packet of(String operation, String... values) {
        return new Packet(operation, List.of(values));
    }
    public void writeTo(OutputStream stream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(buffer);
        output.writeString(1, operation);
        for (String value : values) output.writeString(2, value);
        output.flush();
        byte[] bytes = buffer.toByteArray();
        if (bytes.length > MAX_BYTES) throw new IOException("Слишком большой protobuf-пакет");
        CodedOutputStream frame = CodedOutputStream.newInstance(stream);
        frame.writeUInt32NoTag(bytes.length);
        frame.writeRawBytes(bytes);
        frame.flush();
    }
    public static Packet readFrom(InputStream stream) throws IOException {
        int first = stream.read();
        if (first == -1) return null;
        int length = CodedInputStream.readRawVarint32(first, stream);
        if (length < 0 || length > MAX_BYTES) throw new IOException("Неверная длина protobuf-пакета");
        byte[] bytes = stream.readNBytes(length);
        if (bytes.length != length) throw new EOFException("Оборван protobuf-пакет");
        CodedInputStream input = CodedInputStream.newInstance(bytes);
        String operation = "";
        List<String> values = new ArrayList<>();
        int tag;
        while ((tag = input.readTag()) != 0) {
            switch (tag) {
                case 10 -> operation = input.readStringRequireUtf8();
                case 18 -> values.add(input.readStringRequireUtf8());
                default -> {
                    if (!input.skipField(tag)) throw new IOException("Неверное поле protobuf");
                }
            }
        }
        return new Packet(operation, values);
    }
}
