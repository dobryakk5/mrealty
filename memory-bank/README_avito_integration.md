# Интеграция Avito в listings_processor

## Обзор

`listings_processor.py` теперь поддерживает парсинг объявлений как с **Cian**, так и с **Avito**. Система автоматически определяет источник ссылки и использует соответствующий парсер.

## Основные возможности

### 🏠 Поддерживаемые источники

- **Avito** (source = 1) - использует Selenium WebDriver с cookies
- **Cian** (source = 4) - использует requests + BeautifulSoup
- **Неизвестные источники** (source = 0) - не обрабатываются

### 🔍 Автоматическое определение источника

```python
processor = ListingsProcessor()

# Определение типа ссылки
if processor.is_avito_url(url):
    print("Это ссылка на Avito")
elif processor.is_cian_url(url):
    print("Это ссылка на Cian")

# Получение числового идентификатора источника
source = processor.get_url_source(url)  # 1 для Avito, 4 для Cian
```

## Новые методы

### 1. Парсинг объявлений Avito

```python
async def parse_avito_listing(self, url: str) -> dict:
    """Парсит объявление с Avito и возвращает данные в формате для БД"""
```

**Возвращает:**
- `rooms` - количество комнат
- `price` - цена
- `floor` - этаж
- `area_m2` - площадь в м²
- `walk_minutes` - время до метро
- `address` - адрес
- `tags` - теги
- `person` - информация о продавце
- `person_type` - тип продавца
- `metro_id` - ID метро
- и другие поля

### 2. Универсальный парсинг

```python
async def parse_listing_universal(self, url: str) -> dict:
    """Универсальный метод для парсинга объявлений с Cian и Avito"""
```

### 3. Пакетный парсинг

```python
async def parse_listings_batch(self, listing_urls: list[str]) -> list[dict]:
    """Универсальный метод для парсинга списка объявлений с Cian и Avito"""
```

### 4. Извлечение фотографий Avito

```python
def extract_avito_photo_urls(self, soup: BeautifulSoup) -> list[str]:
    """Извлекает ссылки на фотографии с Avito"""
```

## Использование в боте

### Обработка ссылок

```python
from listings_processor import ListingsProcessor

processor = ListingsProcessor()

# Обработка ссылки (автоматически определяет источник)
listing_data = await processor.parse_listing_universal(url)

if listing_data:
    source = listing_data.get('source', 0)
    if source == 1:
        print("Объявление с Avito")
    elif source == 4:
        print("Объявление с Cian")
```

### Генерация HTML галереи

```python
# Автоматически обрабатывает как Cian, так и Avito
html_content = processor.generate_html_gallery(listing_urls, user_id, subtitle)
```

### Экспорт в Excel

```python
# Автоматически обрабатывает смешанные ссылки
excel_file, request_id = await export_listings_to_excel(listing_urls, user_id)
```

## Структура данных в БД

### Поле `source`

- **1** - Avito
- **4** - Cian

### Пример данных Avito

```python
{
    'URL': 'https://www.avito.ru/...',
    'rooms': 2,
    'price': 8500000,
    'floor': 5,
    'area_m2': 45.5,
    'walk_minutes': 15,
    'address': 'ул. Примерная, 123',
    'tags': 'Собственник, Ремонт',
    'person': 'Иван Иванов',
    'person_type': 'собственник',
    'metro_id': 1,
    'source': 1  # Avito
}
```

## Требования

### Для Avito парсинга

1. **Selenium WebDriver** - для работы с браузером
2. **Chrome/Chromium** - браузер для WebDriver
3. **Cookies файл** - `avito_cookies.json` с авторизацией
4. **Модуль parse_avito_1metro** - основной парсер Avito

### Для Cian парсинга

1. **requests** - для HTTP запросов
2. **BeautifulSoup** - для парсинга HTML

## Обработка ошибок

### Avito парсер недоступен

```python
if not AVITO_AVAILABLE:
    print("⚠️ Модуль parse_avito_1metro не найден, парсинг Avito недоступен")
```

### Ошибки парсинга

```python
try:
    listing_data = await processor.parse_avito_listing(url)
    if listing_data:
        # Обработка успешного результата
        pass
    else:
        print("❌ Не удалось спарсить объявление Avito")
except Exception as e:
    print(f"❌ Ошибка парсинга: {e}")
```

## Логирование

Система выводит подробную информацию о процессе парсинга:

```
🏠 Парсим объявление Avito: https://www.avito.ru/...
🔄 Парсим объявление Avito: https://www.avito.ru/...
✅ Объявление Avito успешно спарсено
📊 Всего успешно спарсено: 1 из 1
```

## Тестирование

Запустите тестовый скрипт для проверки интеграции:

```bash
python test_avito_integration.py
```

## Примечания

1. **Фотографии Avito** - требуют отдельной обработки (базовая поддержка добавлена)
2. **Cookies** - должны быть актуальными для работы с Avito
3. **Selenium** - может потребовать настройки для вашей системы
4. **Производительность** - Avito парсинг медленнее Cian из-за использования браузера

## Поддержка

При возникновении проблем:

1. Проверьте доступность модуля `parse_avito_1metro`
2. Убедитесь в актуальности cookies
3. Проверьте настройки Selenium WebDriver
4. Посмотрите логи для диагностики ошибок
