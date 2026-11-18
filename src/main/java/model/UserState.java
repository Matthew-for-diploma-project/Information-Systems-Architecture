package model;

import java.time.Instant;

public record UserState(
        String name,
        boolean online,
        Instant lastConnected,
        boolean notificationsEnabled
) {
}