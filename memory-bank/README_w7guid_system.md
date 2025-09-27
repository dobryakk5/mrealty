# Система работы с GUID и расширенными данными

## Обзор
Реализована двухуровневая система сбора данных:
1. **flexible_collector.py** - основной сбор с сохранением GUID в базу
2. **flexible_collector1.py** - получение расширенных данных по GUID

## Изменения в базе данных

### Добавлено поле `guid` в таблицу `ads_w7`
```sql
ALTER TABLE ads_w7 ADD COLUMN guid text;
```

### Обновлен process_record в save_to_ads_w7.py
- Добавлено извлечение `guid` из API ответа
- Обновлены SQL запросы для сохранения GUID

## Использование

### 1. Основной сбор (flexible_collector.py)
```bash
# Обычный сбор - теперь сохраняет GUID в БД
python3 flexible_collector.py --days 7 --rooms "1,2" --status active
```

**Что происходит:**
- Собирает объявления по заданным фильтрам
- Сохраняет в БД все основные поля + **GUID**
- GUID используется для дальнейшего получения расширенных данных

### 2. Расширенные данные (flexible_collector1.py)

#### Одиночный GUID:
```bash
python3 flexible_collector1.py --guid "09261C9A-8991-0000-0000-0063E9CF0000"
```

#### Множественные GUID:
```bash
python3 flexible_collector1.py --guids "guid1,guid2,guid3"
```

#### GUID из файла:
```bash
# Создаем файл с GUID (по одному на строку)
echo "09261C9A-8991-0000-0000-0063E9CF0000" > guids.txt
echo "09CD7DFC-5991-0000-0000-006332050000" >> guids.txt

python3 flexible_collector1.py --file guids.txt
```

## Различия между обычными и расширенными данными

### Обычный запрос (DSL v2):
- **85+ полей** с системными именами
- Поля: `price_rub`, `geo_cache_*`, `walls_material_type_id`
- Быстрый массовый сбор

### Расширенный запрос (DSL v3):
- **120+ полей** с человечными названиями
- Поля: `price`, `walls_material_type_name`, `apartment_condition_type_name`
- Дополнительные поля: `bedroom_count`, `cadastral_number`, `note`, `has_*`
- Детальный анализ конкретных объявлений

## Примеры полей в расширенных данных

### Дополнительные характеристики:
- `bedroom_count` - количество спален
- `cadastral_number` - кадастровый номер
- `note` - подробное описание
- `apartment_condition_type_name` - состояние квартиры
- `has_furniture`, `has_refrigerator` - наличие мебели/техники
- `is_pet_allowed`, `is_children_allowed` - разрешения

### Названия вместо ID:
- `walls_material_type_name` вместо `walls_material_type_id`
- `realty_type_name` вместо `realty_type_id`
- `balcony_type_name` вместо `balcony_type_id`

## Рабочий процесс

1. **Массовый сбор**: `flexible_collector.py` собирает тысячи объявлений с GUID
2. **Фильтрация**: Анализируем данные в БД, выбираем интересные объявления
3. **Детализация**: `flexible_collector1.py` получает расширенные данные по выбранным GUID
4. **Анализ**: Работаем с полными данными в JSON формате

## Форматы файлов

### Основной сбор
Данные сохраняются в PostgreSQL таблицу `ads_w7`

### Расширенные данные
Сохраняются в JSON файлы:
- `extended_{GUID}_{timestamp}.json` - одиночный GUID
- `extended_multiple_{timestamp}.json` - множественные GUID
- `extended_from_file_{timestamp}.json` - из файла

## Мониторинг и отладка

### Проверка сохранения GUID:
```sql
SELECT id, guid, price, address FROM ads_w7 WHERE guid IS NOT NULL LIMIT 5;
```

### Статистика по GUID:
```sql
SELECT
    COUNT(*) as total_records,
    COUNT(guid) as with_guid,
    COUNT(*) - COUNT(guid) as without_guid
FROM ads_w7;
```

## Производительность

- **Основной сбор**: ~400 объявлений за запрос, массовая обработка
- **Расширенные данные**: 1 GUID за запрос, детальная обработка
- **Рекомендация**: Сначала массовый сбор, затем выборочная детализация

## Ошибки и их решение

### "column guid does not exist"
```bash
# Добавить поле в существующую таблицу
python3 -c "
import asyncio, asyncpg, os
from dotenv import load_dotenv

async def fix():
    load_dotenv()
    conn = await asyncpg.connect(os.getenv('DATABASE_URL'))
    await conn.execute('ALTER TABLE ads_w7 ADD COLUMN guid text')
    await conn.close()
    print('✅ Поле guid добавлено')

asyncio.run(fix())
"
```

### "HTTP 401" при запросах
Проверить актуальность токенов в конфигурации.

### "GUID не найден"
GUID может быть устаревшим или объявление снято с публикации.