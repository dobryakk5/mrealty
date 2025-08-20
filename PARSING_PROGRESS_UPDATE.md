# Обновление системы parsing_progress

## Что добавлено

В таблицу `system.parsing_progress` добавлено новое поле `source` типа `SMALLINT` для идентификации источника парсинга.

## Значения поля source

- **1** - AVITO
- **2** - DOMCLICK  
- **3** - YANDEX
- **4** - CIAN (значение по умолчанию)

## Изменения в коде

### 1. Структура таблицы
```sql
CREATE TABLE system.parsing_progress (
    id SERIAL PRIMARY KEY,
    property_type INTEGER NOT NULL,
    time_period INTEGER,
    current_metro_id INTEGER NOT NULL,
    source SMALLINT NOT NULL DEFAULT 4,  -- НОВОЕ ПОЛЕ
    time_upd TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'active',
    total_metros INTEGER,
    processed_metros INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2. Обновленные индексы
```sql
-- С учетом поля source
CREATE INDEX idx_parsing_progress_latest ON system.parsing_progress(property_type, time_period, source, time_upd DESC);
CREATE INDEX idx_parsing_progress_no_time ON system.parsing_progress(property_type, source, time_upd DESC) WHERE time_period IS NULL;
```

### 3. Функции работы с БД

#### `create_parsing_session()`
- Автоматически устанавливает `source = 4` для CIAN
- Обновлена сигнатура INSERT запроса

#### `get_last_parsing_progress()`
- Ищет только записи с `source = 4` (CIAN)
- Обеспечивает изоляцию между разными источниками

#### `update_parsing_progress()` и `complete_parsing_session()`
- Остались без изменений
- Работают с существующими записями

## Миграция существующих данных

Если таблица `parsing_progress` уже существует, выполните SQL-скрипт:

```bash
psql -d your_database -f migrate_parsing_progress.sql
```

Скрипт:
1. Добавит поле `source` если его нет
2. Установит `source = 4` для всех существующих записей
3. Пересоздаст индексы с учетом нового поля
4. Покажет статистику по записям

## Преимущества изменений

1. **Изоляция источников**: Прогресс парсинга разных источников не смешивается
2. **Масштабируемость**: Легко добавить новые источники (AVITO, DOMCLICK, YANDEX)
3. **Обратная совместимость**: Существующий код продолжает работать
4. **Производительность**: Индексы оптимизированы для поиска по source

## Использование

### Создание новой сессии
```python
# Автоматически создается с source = 4
session_id = await create_parsing_session(property_type, time_period, total_metros)
```

### Поиск прогресса
```python
# Автоматически ищет только по source = 4 (CIAN)
progress = await get_last_parsing_progress(property_type, time_period)
```

### Обновление прогресса
```python
# Обновляет существующую запись (source уже установлен)
await update_parsing_progress(session_id, current_metro_id, processed_count)
```

## Проверка работы

После запуска парсера проверьте таблицу:

```sql
SELECT 
    id, property_type, time_period, current_metro_id, 
    source, status, processed_metros, total_metros,
    time_upd
FROM system.parsing_progress 
WHERE source = 4 
ORDER BY time_upd DESC;
```

Все записи должны иметь `source = 4` и корректно отслеживать прогресс парсинга CIAN.
