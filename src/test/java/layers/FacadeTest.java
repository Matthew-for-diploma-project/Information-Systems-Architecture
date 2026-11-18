package layers;

import api.RpcEndpoint;
import facade.ChatFacade;
import org.junit.jupiter.api.Test;
import rpc.generated.ChatWire;
import java.util.Arrays;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class FacadeTest {
    @Test
    void verifiesExternalInputWithoutCallingOtherLayers() {
        RpcEndpoint backend = mock(RpcEndpoint.class);
        ChatFacade facade = new ChatFacade(backend);
        for (List<String> input : List.of(
                List.of("UNKNOWN"), List.of("BEGIN"), List.of("CONNECT", " "),
                List.of("CONNECT", "Анна", "лишний"), List.of("SEND", "Анна", " "),
                List.of("NOTIFICATIONS", "Анна", "yes"), List.of("ACK", "Анна", "0"),
                Arrays.asList("CONNECT", null))) {
            assertThrows(IllegalArgumentException.class, () -> facade.handle(input));
        }
        verifyNoInteractions(backend);
    }
    @Test
    void convertsSendToTypedProtobufAndBackWithoutChangingText() {
        String text = "Первая\nВторая\t\\\0";
        RpcEndpoint backend = request -> {
            assertEquals(ChatWire.Request.Operation.SEND, request.getOperation());
            assertEquals("Анна", request.getName());
            assertEquals(text, request.getText());
            return ChatWire.Reply.newBuilder().addMessages(ChatWire.Message.newBuilder()
                    .setId(7).setSender("Анна").setText(text).setCreatedAt("2026-01-01T00:00:00Z")).build();
        };
        assertEquals(List.of("OK", "7", "Анна", text, "2026-01-01T00:00:00Z"),
                new ChatFacade(backend).handle(List.of("SEND", "Анна", text)));
    }
    @Test
    void parsesBooleanAndPositiveAcknowledgementId() {
        RpcEndpoint backend = mock(RpcEndpoint.class);
        when(backend.handle(any())).thenReturn(ChatWire.Reply.getDefaultInstance());
        ChatFacade facade = new ChatFacade(backend);
        assertEquals(List.of("OK"), facade.handle(List.of("NOTIFICATIONS", "Борис", "false")));
        verify(backend).handle(ChatWire.Request.newBuilder().setOperation(ChatWire.Request.Operation.NOTIFICATIONS)
                .setName("Борис").setEnabled(false).build());
        facade.handle(List.of("ACK", "Борис", "15"));
        verify(backend).handle(ChatWire.Request.newBuilder().setOperation(ChatWire.Request.Operation.ACK)
                .setName("Борис").setMessageId(15).build());
    }
    @Test
    void preservesBackendErrors() {
        ChatFacade facade = new ChatFacade(request -> ChatWire.Reply.newBuilder()
                .setStatus(ChatWire.Reply.Status.STATE).setError("Уже в сети").build());
        assertEquals("Уже в сети", assertThrows(IllegalStateException.class,
                () -> facade.handle(List.of("CONNECT", "Анна"))).getMessage());
    }
    @Test
    void formatsListsStateAndMetrics() {
        ChatWire.User user = ChatWire.User.newBuilder().setName("Анна").setOnline(true)
                .setLastConnected("2026-01-01T00:00:00Z").setNotificationsEnabled(false).build();
        ChatFacade facade = new ChatFacade(request -> ChatWire.Reply.newBuilder().addUsers(user)
                .setMetrics(ChatWire.Metrics.newBuilder().setRequests(10).setNotifications(2)).build());
        assertEquals(List.of("OK", "0"), facade.handle(List.of("HISTORY", "Анна")));
        assertEquals(List.of("OK", "Анна", "true", "2026-01-01T00:00:00Z", "false"),
                facade.handle(List.of("STATE", "Анна")));
        assertEquals(List.of("OK", "1", "Анна", "true", "2026-01-01T00:00:00Z", "false"),
                facade.handle(List.of("USERS")));
        assertEquals(List.of("OK", "10", "2"), facade.handle(List.of("METRICS")));
    }
}
