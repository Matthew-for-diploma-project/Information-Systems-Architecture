package service;

public final class Validation {
    private Validation() {
    }
    public static void name(String value) {
        if (value == null || value.isBlank() || value.length() > 255) {
            throw new IllegalArgumentException("Имя должно содержать от 1 до 255 символов");
        }
    }
    public static void text(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Сообщение не должно быть пустым");
        }
    }
    public static void id(long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("Номер сообщения должен быть положительным");
        }
    }
}
