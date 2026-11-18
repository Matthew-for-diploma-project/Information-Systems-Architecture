package api;

import java.util.List;

/** Входной интерфейс фасада: уже выделенные поля пользовательского кадра. */
@FunctionalInterface
public interface TextEndpoint {
    List<String> handle(List<String> fields);
}
