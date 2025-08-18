# Настройка переменных окружения

## Создание файла .env

Для работы скриптов проверки и миграции данных необходимо создать файл `.env` в корне проекта.

### Шаг 1: Создайте файл .env

```bash
touch .env
```

### Шаг 2: Добавьте в файл .env следующие строки:

```env
# Подключение к PostgreSQL базе данных
DATABASE_URL=postgresql://username:password@localhost:5432/database_name

# Telegram Bot API токен (если используется)
API_TOKEN=your_telegram_bot_token_here
```

### Примеры DATABASE_URL:

```env
# Локальная база данных
DATABASE_URL=postgresql://postgres:mypassword@localhost:5432/realty_db

# Удаленная база данных
DATABASE_URL=postgresql://user:pass@192.168.1.100:5432/mrealty

# Продакшн база данных
DATABASE_URL=postgresql://username:password@db.example.com:5432/production_db
```

### Параметры подключения:

- **username**: имя пользователя PostgreSQL
- **password**: пароль пользователя  
- **localhost**: хост базы данных (или IP адрес)
- **5432**: порт PostgreSQL (по умолчанию 5432)
- **database_name**: название базы данных

## Проверка настройки

После создания файла `.env` запустите быструю проверку:

```bash
python quick_check.py
```

Если настройка корректна, вы увидите:
```
✅ Подключение к БД установлено
📋 Найденные таблицы: complexes, jk
```

## Безопасность

⚠️ **ВАЖНО**: Файл `.env` содержит конфиденциальные данные!

1. **НЕ коммитьте** файл `.env` в git
2. **Добавьте** `.env` в `.gitignore`
3. **Храните** файл в безопасном месте
4. **Используйте** разные учетные данные для разработки и продакшна

## Устранение проблем

### Ошибка: "DATABASE_URL is not set"

**Решение**: Проверьте, что файл `.env` создан и содержит корректный DATABASE_URL

### Ошибка подключения к базе данных

**Возможные причины**:
- Неправильный пароль
- Неверный хост или порт
- База данных не запущена
- Проблемы с сетевым доступом

**Проверка**:
```bash
# Тест подключения через psql
psql "postgresql://username:password@localhost:5432/database_name"
```

### Таблицы не найдены

**Возможные причины**:
- Таблицы находятся в другой схеме
- Неправильные права доступа
- Таблицы не созданы

**Проверка**:
```sql
-- Посмотреть все таблицы в схеме public
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';

-- Посмотреть все схемы
SELECT schema_name FROM information_schema.schemata;
```
