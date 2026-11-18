package api;

import java.util.List;

/** Внешние поля команды -> внешний ответ. Детали нижнего слоя скрыты. */
@FunctionalInterface
public interface Facade {
    List<String> handle(List<String> fields);
}
