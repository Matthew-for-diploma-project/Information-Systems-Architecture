package layers;

import model.Message;
import model.UserState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import rpc.generated.ChatWire;
import service.ApplicationEndpoint;
import service.ChatServer;
import storage.ChatRepository;
import storage.ChatTransaction;
import java.time.*;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ApplicationTest {
    private final Instant time = Instant.parse("2026-01-01T00:00:00Z");
    private ChatRepository repository;
    private ChatTransaction transaction;
    private ChatServer chat;
    @BeforeEach
    void setUp() {
        repository = mock(ChatRepository.class);
        transaction = mock(ChatTransaction.class);
        when(repository.begin()).thenReturn(transaction);
        chat = new ChatServer(repository, Clock.fixed(time, ZoneOffset.UTC));
    }
    @Test
    void reconnectKeepsPreferencesAndUpdatesTime() {
        when(transaction.findUser("Анна")).thenReturn(new UserState("Анна", false, time.minusSeconds(10), false));
        chat.connect("Анна");
        verify(transaction).saveUser(new UserState("Анна", true, time, false));
        verify(transaction).commit();
        verify(transaction).close();
    }
    @Test
    void sendsToEnabledRecipientsIncludingOfflineUsers() {
        UserState anna = new UserState("Анна", true, time, true);
        when(transaction.findUser("Анна")).thenReturn(anna);
        when(transaction.users()).thenReturn(List.of(anna,
                new UserState("Вера", true, time, false), new UserState("Борис", false, time, true)));
        Message message = new Message(1, "Анна", "Привет", time);
        when(transaction.addMessage("Анна", "Привет", time)).thenReturn(message);
        assertEquals(message, chat.sendMessage("Анна", "Привет"));
        verify(transaction).enqueue(1, List.of("Анна", "Борис"));
        verify(transaction).commit();
    }
    @Test
    void rejectsInvalidInputBeforeBeginningTransaction() {
        assertThrows(IllegalArgumentException.class, () -> chat.sendMessage("Анна", " "));
        assertThrows(IllegalArgumentException.class, () -> chat.connect(null));
        assertThrows(IllegalArgumentException.class, () -> chat.acknowledgeNotification("Анна", 0));
        verifyNoInteractions(repository);
    }
    @Test
    void offlineUsersCannotSendAndFailedOperationDoesNotCommit() {
        when(transaction.findUser("Борис")).thenReturn(new UserState("Борис", false, time, true));
        assertThrows(IllegalStateException.class, () -> chat.sendMessage("Борис", "Привет"));
        verify(transaction, never()).addMessage(anyString(), anyString(), any());
        verify(transaction, never()).commit();
        verify(transaction).close();
    }
    @Test
    void readingDoesNotAcknowledgeAndStorageFailureClosesTransaction() {
        when(transaction.findUser("Анна")).thenReturn(new UserState("Анна", true, time, true));
        Message message = new Message(1, "Анна", "Привет", time);
        when(transaction.notifications("Анна")).thenReturn(List.of(message));
        assertEquals(List.of(message), chat.readNotifications("Анна"));
        verify(transaction, never()).acknowledge(anyString(), anyLong());
        reset(transaction);
        when(transaction.findUser("Анна")).thenReturn(new UserState("Анна", true, time, true));
        when(transaction.addMessage(anyString(), anyString(), any())).thenThrow(new IllegalStateException("Сбой"));
        assertThrows(IllegalStateException.class, () -> chat.sendMessage("Анна", "Привет"));
        verify(transaction, never()).commit();
        verify(transaction).close();
    }
    @Test
    void applicationEndpointRejectsStorageCommands() {
        ApplicationEndpoint endpoint = new ApplicationEndpoint(chat);
        assertThrows(IllegalArgumentException.class, () -> endpoint.handle(ChatWire.Request.newBuilder()
                .setOperation(ChatWire.Request.Operation.BEGIN).build()));
        assertThrows(IllegalArgumentException.class, () -> endpoint.handle(ChatWire.Request.newBuilder()
                .setOperation(ChatWire.Request.Operation.NOTIFICATIONS).setName("Анна").build()));
        verifyNoInteractions(repository);
    }
}
