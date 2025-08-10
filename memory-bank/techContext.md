## Project map (single source of truth) — mrealty

Назначение: короткая «мапа» проекта. Читается первой при любом изменении кода, проверке багов и планировании. Если обнаружены расхождения с кодом — доверяем коду и оперативно обновляем этот файл.

### Точка входа
- `main.py`: инициализация aiogram `Bot`/`Dispatcher`, регистрация хэндлеров, ротационное логирование, запуск polling.

### Обработчики
- `start_handlers.py`: `on_start(message)` — приветствие и клавиатура с «📘 инструкция».
- `text_handlers.py`:
  - `handle_text_message(message)` — разбирает текст, показывает «инструкцию», извлекает URL.
  - `_handle_listings_export(urls, message)` — экспортирует 2 Excel-файла и отправляет пользователю.

### Основная бизнес-логика
- `listings_processor.py`:
  - `extract_urls(text)` — вытаскивает URL из текста.
  - `parse_listing(url, session)` — парсинг страницы CIAN (`requests` + `BeautifulSoup`).
  - `export_listings_to_excel(listing_urls, user_id, output_path?)` — парсит список, сохраняет в БД (`save_listings`) и формирует основной Excel.
  - `export_sim_ads(request_id, output_path?)` — формирует второй Excel по похожим объявлениям (агрегации из БД).

### Хранилище данных
- `db_handler.py` (PostgreSQL, `asyncpg`):
  - Пул соединений, `init_schema()` (схема `users`, таблицы `requests`, `listings`).
  - `save_listings(rows, user_id)` → создаёт `request_id`, сохраняет объявления.
  - `find_similar_ads_grouped(request_id)` → агрегации для похожих объявлений.
  - Нормализация числовых полей (`clean_numeric`), парсинг этажей/лифтов/счётчиков.

### Вспомогательные скрипты
- `standalone/*` — отдельные утилиты (selenium, тесты, домклик и т.п.), не используются рантаймом бота.

### Поток данных (сквозной путь)
Telegram → `main.py` → `text_handlers.py` → `listings_processor.py` → `db_handler.py` → Excel (в памяти через `BytesIO`) → Telegram

### Конфигурация окружения
- `.env`: `API_TOKEN` (Telegram Bot API), `DATABASE_URL` (PostgreSQL).
- Часовой пояс: операции таймстампов в БД — Europe/Moscow (см. `db_handler.py`).

### Логирование
- Ротация логов через `TimedRotatingFileHandler` (кежесуточно), вывод в `logs/bot.log` + консоль. Формат: `"%(asctime)s [%(levelname)s] %(message)s"`.

### База данных (кратко)
- `users.requests(id, userid, ts)` — запросы пользователя.
- `users.listings(...)` — нормализованные поля объявления + `other JSONB` для нераспознанных ключей.
- Внешние ключи: `listings.request_id` → `requests.id` (ON DELETE CASCADE).

### Внешние зависимости (ядро)
- aiogram, requests, beautifulsoup4, pandas, openpyxl, asyncpg, python-dotenv.
- Дополнительно в `standalone`: selenium / seleniumbase.

### Правило использования этого файла (для ассистента и разработчиков)
1) Этот файл читается ПЕРВЫМ при начале любой задачи или правки.
2) Если описание расходится с кодом — приоритет у кода; файл следует обновить сразу после выявления.
3) Все архитектурные решения/инварианты фиксировать здесь короткими пунктами.

### MCP контекст
- Всегда использовать MCP Context7 как активный контекст кодирования (см. `memory-bank/activeContext.md`).

### Инварианты/заметки
- Имена колонок в Excel на русском; цена сначала как `Цена_raw`, затем форматируется и выводится как `Цена`.
- `parse_lifts` возвращает раздельно пассажирские/грузовые лифты.
- Обработка статуса объявления (Активно/Снято) определяется по тексту страницы.
- Генерация Excel — через `pandas`/`openpyxl`, bold для заголовков, формат тысяч для цены.


