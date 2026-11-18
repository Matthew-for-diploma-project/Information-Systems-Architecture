package model;

import java.time.Instant;

public record Message(long id, String sender, String text, Instant createdAt) {
    @Override
    public String toString() {
        return id + ". " + sender + ": " + text;
    }
}