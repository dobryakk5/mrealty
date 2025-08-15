# 🕷️ Новый парсер AVITO на основе Scrapy

Этот парсер полностью переписан на основе готового Scrapy-паука **cyberj0g/avito-watcher** и использует современные технологии для обхода защиты AVITO.

## 🚀 Основные возможности

- **Scrapy Framework** - мощный и быстрый фреймворк для веб-скрапинга
- **Selenium WebDriver** - обход JavaScript-защиты и динамического контента
- **Автоматическое сохранение** в БД PostgreSQL
- **Защита от бана** - ротация User-Agent, случайные задержки
- **Обработка ошибок** и повторные попытки
- **Логирование** всех операций

## 📋 Что парсится

- ✅ **ID объявления** (offer_id)
- ✅ **URL объявления**
- ✅ **Заголовок**
- ✅ **Цена**
- ✅ **Адрес**
- ✅ **Метро и время до метро**
- ✅ **Количество комнат**
- ✅ **Площадь**
- ✅ **Этаж и этажность**
- ✅ **Тип продавца**
- ✅ **Время размещения**
- ✅ **Количество фото**

## 🛠️ Установка

### 1. Установка зависимостей

```bash
pip install -r requirements_avito_scrapy.txt
```

### 2. Установка Chrome WebDriver

```bash
# Автоматическая установка через webdriver-manager
pip install webdriver-manager

# Или ручная установка ChromeDriver
# Скачайте с https://chromedriver.chromium.org/
```

### 3. Настройка переменных окружения

Создайте файл `.env`:

```env
DATABASE_URL=postgresql://user:password@localhost:5432/database
MAX_CARDS_TO_PARSE=5
```

## 🚀 Использование

### Базовый запуск

```bash
python parse_avito_scrapy.py
```

### Настройка количества карточек

```bash
export MAX_CARDS_TO_PARSE=10
python parse_avito_scrapy.py
```

### Запуск с кастомными URL

```python
# В коде
start_urls = [
    'https://www.avito.ru/moskva/kvartiry/vtorichka',
    'https://www.avito.ru/moskva/kvartiry/novostroyka'
]
```

## 🔧 Архитектура

### 1. AvitoScrapySpider
Основной Scrapy-паук, который:
- Обрабатывает страницы списков объявлений
- Использует Selenium для динамического контента
- Парсит карточки объявлений
- Извлекает все необходимые данные

### 2. AvitoScrapyParser
Главный класс для:
- Настройки базы данных
- Запуска Scrapy-процесса
- Сохранения данных в БД

### 3. Selenium Integration
- Настройка Chrome WebDriver
- Обход детекции автоматизации
- Ожидание загрузки контента
- Получение HTML после JavaScript

## 📊 Структура данных

Данные сохраняются в таблицу `ads_avito`:

```sql
CREATE TABLE ads_avito (
    id SERIAL PRIMARY KEY,
    url TEXT,
    avitoid NUMERIC,
    price BIGINT,
    rooms SMALLINT,
    area NUMERIC,
    floor SMALLINT,
    total_floors SMALLINT,
    complex TEXT,
    metro TEXT,
    min_metro SMALLINT,
    address TEXT,
    tags TEXT,
    person_type TEXT,
    person TEXT,
    object_type_id SMALLINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(avitoid)
);
```

## 🛡️ Защита от бана

### 1. User-Agent ротация
```python
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit...",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36..."
]
```

### 2. Случайные задержки
```python
'DOWNLOAD_DELAY': random.uniform(2, 5),
'RANDOMIZE_DOWNLOAD_DELAY': True,
```

### 3. Selenium настройки
```python
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
```

## 🔍 Отладка

### Логирование
```python
'LOG_LEVEL': 'INFO'  # DEBUG, INFO, WARNING, ERROR
```

### Selenium отладка
```python
# Добавьте для отладки
chrome_options.add_argument('--remote-debugging-port=9222')
chrome_options.add_argument('--disable-extensions')
```

## 📝 Примеры использования

### Парсинг конкретного района
```python
start_urls = [
    'https://www.avito.ru/moskva/kvartiry/vtorichka?f=ASgBAgICAkSSA8gQ8AeQUgFAzAgUlmMFeyJmcm9tIjoiZ2VvIn0&s=104'
]
```

### Парсинг с фильтрами
```python
start_urls = [
    'https://www.avito.ru/moskva/kvartiry/vtorichka?f=ASgBAgICAkSSA8gQ8AeQUgFAzAgUlmMFeyJmcm9tIjoiZ2VvIn0&s=104&pmax=10000000&pmin=5000000'
]
```

## ⚠️ Важные замечания

1. **Соблюдайте robots.txt** - настройте задержки между запросами
2. **Используйте прокси** для больших объемов данных
3. **Мониторьте логи** на предмет ошибок и банов
4. **Тестируйте на малых объемах** перед масштабированием

## 🆘 Устранение неполадок

### Ошибка ChromeDriver
```bash
pip install webdriver-manager
# Или скачайте ChromeDriver вручную
```

### Ошибка Scrapy
```bash
pip install scrapy[selenium]
```

### Ошибка базы данных
```bash
# Проверьте DATABASE_URL в .env
# Убедитесь, что таблица ads_avito создана
```

## 📈 Производительность

- **Скорость**: ~2-5 секунд между запросами
- **Память**: ~100-200 МБ на процесс
- **Параллельность**: 1 запрос одновременно (настраивается)
- **Масштабируемость**: легко увеличить количество процессов

## 🔗 Ссылки

- [cyberj0g/avito-watcher](https://github.com/cyberj0g/avito-watcher) - оригинальный паук
- [Scrapy Documentation](https://docs.scrapy.org/) - документация Scrapy
- [Selenium Documentation](https://selenium-python.readthedocs.io/) - документация Selenium
- [PostgreSQL Documentation](https://www.postgresql.org/docs/) - документация PostgreSQL
