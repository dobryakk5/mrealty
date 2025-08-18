# 🏠 mrealty - Система анализа недвижимости

Система для парсинга, анализа и управления данными о недвижимости с различных площадок (ЦИАН, Авито).

## 🚀 Быстрый старт

### 1. Проверка данных между таблицами complexes и jk

Если вам нужно проверить, какие данные из таблицы `public.jk` не были скопированы в `public.complexes`:

```bash
# Создайте файл .env с подключением к БД
echo "DATABASE_URL=postgresql://username:password@localhost:5432/database_name" > .env

# Быстрая проверка
python quick_check.py

# Детальный анализ
python check_complexes_data.py

# Миграция данных (если нужно)
python migrate_jk_to_complexes.py --execute
```

📖 **Подробная инструкция**: [QUICK_START.md](QUICK_START.md)

## 📋 Основные компоненты

### Парсеры
- **ЦИАН**: `parse_cian_to_db.py` - парсинг объявлений с CIAN
- **Авито**: `parse_avito_to_db.py` - парсинг объявлений с Авито
- **Метро**: `enhanced_metro_parser.py` - обновление данных о метро

### Обработка данных
- `listings_processor.py` - основная логика обработки объявлений
- `db_handler.py` - работа с базой данных
- `text_handlers.py` - обработка текстовых сообщений

### Telegram Bot
- `main.py` - точка входа бота
- `start_handlers.py` - обработчики команд
- `text_handlers.py` - обработка текста

## 🗄️ База данных

### Основные таблицы
- `users.requests` - запросы пользователей
- `users.listings` - объявления недвижимости
- `ads_cian` - объявления с CIAN
- `ads_avito` - объявления с Авито
- `public.complexes` - жилые комплексы
- `public.jk` - данные ЖК с ЦИАН

### Схемы
- `users` - пользовательские данные
- `public` - общие данные
- `system` - системные таблицы

## 🛠️ Установка и настройка

### Зависимости
```bash
pip install -r requirements_avito_scrapy.txt
```

### Переменные окружения
Создайте файл `.env`:
```env
DATABASE_URL=postgresql://username:password@localhost:5432/database_name
API_TOKEN=your_telegram_bot_token
```

📖 **Подробная настройка**: [SETUP_ENV.md](SETUP_ENV.md)

## 📊 Анализ данных

### Проверка комплексов
```bash
# Быстрая проверка
python quick_check.py

# Детальный анализ
python check_complexes_data.py
```

### Миграция данных
```bash
# Просмотр (без изменений)
python migrate_jk_to_complexes.py

# Выполнение
python migrate_jk_to_complexes.py --execute
```

## 🔧 Разработка

### Структура проекта
```
mrealty/
├── memory-bank/          # Документация и контекст
├── standalone/           # Утилиты и тесты
├── *.py                 # Основные скрипты
├── README_*.md          # Документация по компонентам
└── requirements_*.txt   # Зависимости
```

### Основные правила
- Всегда читайте `memory-bank/techContext.md` первым
- Используйте MCP Context7 для кодирования
- Следуйте правилам из `memory-bank/activeContext.md`

## 📚 Документация

- [QUICK_START.md](QUICK_START.md) - Быстрый старт
- [SETUP_ENV.md](SETUP_ENV.md) - Настройка окружения
- [README_complexes_migration.md](README_complexes_migration.md) - Миграция данных
- [README_cian_parser.md](README_cian_parser.md) - Парсер ЦИАН
- [README_avito_parser.md](README_avito_parser.md) - Парсер Авито
- [README_metro_parser.md](README_metro_parser.md) - Парсер метро

## 🚨 Устранение проблем

### Частые ошибки
1. **DATABASE_URL не установлен** - создайте файл `.env`
2. **Ошибка подключения к БД** - проверьте PostgreSQL
3. **Таблицы не найдены** - проверьте схему и права доступа

### Логи
Логи сохраняются в директории `logs/` с ротацией по дням.

## 🤝 Вклад в проект

1. Следуйте архитектурным решениям из `memory-bank/techContext.md`
2. Обновляйте документацию при изменении кода
3. Используйте существующие паттерны и стили

## 📄 Лицензия

Проект для внутреннего использования.

---

**Нужна помощь?** Запустите `python quick_check.py` и покажите вывод для диагностики.
