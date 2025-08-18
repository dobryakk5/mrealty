# Парсинг одного метро - Инструкция по использованию

## Обзор

Добавлен новый метод `parse_single_metro` в класс `EnhancedMetroParser`, который позволяет парсить одно метро с заданными параметрами и вызывать его из других файлов.

## Новый метод

### `parse_single_metro(metro_id, max_pages, max_cards=None)`

**Параметры:**
- `metro_id` (int) - ID метро из таблицы metro
- `max_pages` (int) - Количество страниц для парсинга
- `max_cards` (int, optional) - Количество карточек на странице (0 = все карточки)

**Возвращает:**
- `tuple: (success: bool, saved_count: int, total_cards: int)`
  - `success` - Успешно ли выполнен парсинг
  - `saved_count` - Количество сохраненных записей в БД
  - `total_cards` - Общее количество спарсенных карточек

## Способы использования

### 1. Импорт и вызов из Python кода

```python
import asyncio
from parse_avito_1metro import EnhancedMetroParser

async def parse_my_metro():
    # Создаем парсер
    parser = EnhancedMetroParser()
    parser.database_url = "your_database_url"
    
    # Парсим метро ID=1, 3 страницы, 15 карточек на странице
    success, saved_count, total_cards = await parser.parse_single_metro(
        metro_id=1,
        max_pages=3,
        max_cards=15
    )
    
    if success:
        print(f"Сохранено {saved_count} из {total_cards} карточек")
    else:
        print("Ошибка парсинга")

# Запуск
asyncio.run(parse_my_metro())
```

### 2. Использование готового скрипта

#### Быстрый парсинг (quick_metro_parse.py)

**Настройки по умолчанию:**
В файле `quick_metro_parse.py` в начале можно задать параметры по умолчанию:

```python
# Метро по умолчанию (ID из таблицы metro)
DEFAULT_METRO_ID = 2

# Количество страниц по умолчанию
DEFAULT_MAX_PAGES = 1

# Количество карточек на странице по умолчанию (0 = все карточки)
DEFAULT_MAX_CARDS = 5
```

**Использование:**

```bash
# Запуск с настройками по умолчанию (метро ID=2, 1 страница, 5 карточек)
python quick_metro_parse.py

# Парсим метро ID=1 с настройками по умолчанию
python quick_metro_parse.py 1

# Парсим метро ID=1, 3 страницы, карточки по умолчанию
python quick_metro_parse.py 1 3

# Парсим метро ID=2, 1 страница, 15 карточек
python quick_metro_parse.py 2 1 15

# Парсим метро ID=5, 2 страницы, все карточки
python quick_metro_parse.py 5 2 0

# Показать справку
python quick_metro_parse.py --help
```

**Преимущества:**
- Можно запустить без аргументов - будут использованы настройки по умолчанию
- Можно задать только ID метро - остальные параметры возьмутся по умолчанию
- Легко изменить настройки по умолчанию прямо в файле
- Гибкость: можно переопределить любой параметр через командную строку
- Встроенная справка с примерами использования

#### Примеры использования (example_metro_parser_usage.py)

```bash
# Запуск примеров
python example_metro_parser_usage.py
```

### 3. Парсинг нескольких метро

```python
import asyncio
from parse_avito_1metro import EnhancedMetroParser

async def parse_multiple_metros():
    metros_to_parse = [
        {"id": 1, "pages": 2, "cards": 10},
        {"id": 2, "pages": 1, "cards": 15},
        {"id": 3, "pages": 3, "cards": 20},
    ]
    
    total_saved = 0
    total_cards = 0
    
    for metro_config in metros_to_parse:
        # Создаем новый парсер для каждого метро
        parser = EnhancedMetroParser()
        parser.database_url = "your_database_url"
        
        success, saved_count, cards_count = await parser.parse_single_metro(
            metro_id=metro_config['id'],
            max_pages=metro_config['pages'],
            max_cards=metro_config['cards']
        )
        
        if success:
            total_saved += saved_count
            total_cards += cards_count
    
    print(f"Всего сохранено: {total_saved} из {total_cards} карточек")

# Запуск
asyncio.run(parse_multiple_metros())
```

## Требования

### Переменные окружения
Создайте файл `.env` в корне проекта:

```env
DATABASE_URL=postgresql://username:password@host:port/database
```

### Зависимости
Убедитесь, что установлены все необходимые зависимости:

```bash
pip install -r requirements_avito_scrapy.txt
```

## Особенности

### Автоматическое управление ресурсами
- Метод автоматически создает и закрывает Selenium WebDriver
- Автоматически загружает и применяет cookies
- Автоматически получает avito_id для метро из БД

### Обработка ошибок
- Детальное логирование всех этапов
- Graceful обработка ошибок с возвратом статуса
- Автоматическая очистка ресурсов при ошибках

### Гибкость настройки
- Можно задать количество страниц и карточек
- Поддержка парсинга всех карточек на странице (max_cards=0)
- Настройки берутся из config_parser.py или используются значения по умолчанию

## Логирование

Метод выводит подробную информацию о процессе:
- 🚀 Запуск парсинга
- 📍 Информация о метро
- 🌐 URL страниц
- 📊 Количество найденных карточек
- 💾 Процесс сохранения в БД
- 📊 Итоговая статистика

## Примеры вывода

```
🚀 Запуск парсинга метро ID=1, страниц=3, карточек на странице=15
📍 Метро: Авиамоторная (ID: 1, avito_id: 1)
🌐 URL: https://www.avito.ru/moskva/kvartiry/prodam?metro=1&s=104
📄 Страница 1: 15 карточек
📄 Страница 2: 15 карточек
📄 Страница 3: 15 карточек
💾 Сохраняем 45 карточек в БД...
✅ Сохранено в БД: 45 записей

📊 ИТОГОВАЯ СТАТИСТИКА:
   Метро ID: 1
   Метро avito_id: 1
   Страниц спарсено: 3
   Карточек спарсено: 45
   Сохранено в БД: 45

🎉 Парсинг завершен успешно!
```

## Устранение неполадок

### Ошибка подключения к БД
- Проверьте правильность DATABASE_URL в .env
- Убедитесь, что БД доступна

### Ошибка загрузки cookies
- Проверьте наличие файла avito_cookies.json
- Убедитесь, что cookies не устарели

### Ошибка Selenium
- Проверьте установку Chrome/ChromeDriver
- Убедитесь, что браузер может запуститься в headless режиме

### Метро не найдено
- Проверьте, что metro_id существует в таблице metro
- Убедитесь, что у метро есть avito_id

### Проблема с сохранением metro_id
**Исправлено!** Теперь `metro_id` корректно сохраняется в БД.

**Что было исправлено:**
1. Добавлено поле `metro_id` в таблицу `ads_avito`
2. Обновлен метод `prepare_data_for_db` для сохранения `metro_id`
3. Обновлен SQL запрос для вставки/обновления

**Проверка исправления:**
```bash
# Запустите тест для проверки сохранения metro_id
python test_metro_id_save.py
```

**Структура данных в БД:**
- `metro_id` - ID метро из таблицы metro (связь с таблицей metro)
- `metro` - название метро (текст)
- `min_metro` - время до метро в минутах

**Преимущества исправления:**
- Теперь можно точно знать, из какого метро было спарсено объявление
- Возможность делать выборки по metro_id
- Связь с таблицей metro для дополнительной информации

### Проблема с сохранением avitoid
**Исправлено!** Теперь `avitoid` корректно сохраняется в БД.

**Что было исправлено:**
1. Исправлено несоответствие названий полей между `parse_avito_1metro.py` и `parse_todb_avito.py`
2. В `parse_todb_avito.py` теперь ищется поле `avitoid` вместо `offer_id`
3. Добавлена отладочная информация для отслеживания парсинга `avitoid`

**Проверка исправления:**
```bash
# Запустите тест для проверки парсинга avitoid
python test_avitoid_parsing.py
```

**Как работает парсинг avitoid:**
1. Извлекается атрибут `data-item-id` из HTML карточки
2. Сохраняется в `card_data['item_id']`
3. В `prepare_data_for_db` копируется в `db_data['avitoid']`
4. В `save_avito_ad` обрабатывается как `ad_data['avitoid']`

**Отладочная информация:**
- Показывает процесс парсинга `item_id` из HTML
- Отображает подготовку `avitoid` для БД
- Логирует процесс сохранения в БД
- Показывает доступные поля при ошибках
