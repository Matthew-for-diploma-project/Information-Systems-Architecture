package rpc;

import api.RpcEndpoint;
import rpc.generated.ChatWire.Reply;
import rpc.generated.ChatWire.Request;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/** Один последовательный канал. Границы protobuf: varint длины + бинарное сообщение. */
public final class ProtoConnection implements RpcEndpoint {
    private final Socket socket = new Socket();
    private final BufferedInputStream input;
    private final BufferedOutputStream output;

    public ProtoConnection(String host, int port) {
        try {
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setSoTimeout(10000);
            socket.setTcpNoDelay(true);
            input = new BufferedInputStream(socket.getInputStream());
            output = new BufferedOutputStream(socket.getOutputStream());
        } catch (IOException exception) {
            close();
            throw new IllegalStateException("Нет связи с внутренним слоем", exception);
        }
    }
    @Override
    public synchronized Reply handle(Request request) {
        try {
            request.writeDelimitedTo(output);
            output.flush();
            Reply reply = Reply.parseDelimitedFrom(input);
            if (reply == null) throw new IOException("Внутренний слой закрыл соединение");
            return ProtoMapper.checked(reply);
        } catch (IOException exception) {
            close();
            throw new IllegalStateException("Ошибка внутреннего протокола", exception);
        }
    }
    @Override
    public void close() {
        try { socket.close(); } catch (IOException ignored) { }
    }
}
