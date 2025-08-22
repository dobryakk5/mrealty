# 🚇 Парсер метро Avito - Документация

## 📋 Описание

`parse_avito.py` - это расширенный парсер объявлений Avito, который может работать как с одним метро, так и со всеми метро Москвы одновременно.

## 🎯 Новый метод: `parse_metro_by_id()`

### Описание
Метод для парсинга **одной конкретной станции метро** с заданными параметрами.

### Сигнатура
```python
async def parse_metro_by_id(self, metro_id, max_pages=None, max_cards=None):
```

### Параметры
- **`metro_id`** (int) - ID станции метро из таблицы `metro` (обязательный)
- **`max_pages`** (int, optional) - Максимальное количество страниц:
  - `None` - использовать настройки из конфига
  - `0` - неограниченное количество страниц
  - `> 0` - конкретное количество страниц
- **`max_cards`** (int, optional) - Максимальное количество карточек на страницу:
  - `None` - использовать настройки из конфига
  - `> 0` - конкретное количество карточек

### Возвращаемое значение
```python
{
    'success': bool,           # Успешность операции
    'metro_name': str,         # Название станции метро
    'total_cards': int,        # Общее количество найденных карточек
    'saved_cards': int,        # Количество сохраненных в БД карточек
    'error': str or None       # Описание ошибки (если есть)
}
```

## 🚀 Примеры использования

### 1. Простой парсинг одного метро

```python
import asyncio
from parse_avito import AvitoMetroParser

async def simple_example():
    # Создаем парсер
    parser = AvitoMetroParser(
        enable_db=True,      # Включаем сохранение в БД
        headless=True        # Запускаем в headless режиме
    )
    
    try:
        # Парсим метро ID = 1, максимум 2 страницы, 5 карточек на страницу
        result = await parser.parse_metro_by_id(
            metro_id=1,
            max_pages=2,
            max_cards=5
        )
        
        if result['success']:
            print(f"✅ Успешно! Сохранено {result['saved_cards']} карточек")
        else:
            print(f"❌ Ошибка: {result['error']}")
            
    finally:
        # Закрываем браузер
        if hasattr(parser, 'driver') and parser.driver:
            parser.driver.quit()

# Запуск
asyncio.run(simple_example())
```

### 2. Парсинг нескольких метро

```python
async def multiple_metros_example():
    parser = AvitoMetroParser(enable_db=True, headless=True)
    
    # Список метро для парсинга
    metros = [
        {'id': 1, 'max_pages': 2, 'max_cards': 3},    # Чкаловская
        {'id': 2, 'max_pages': 1, 'max_cards': 5},    # Маяковская
        {'id': 3, 'max_pages': 3, 'max_cards': 4},    # Третьяковская
    ]
    
    total_saved = 0
    
    try:
        for metro_config in metros:
            result = await parser.parse_metro_by_id(
                metro_id=metro_config['id'],
                max_pages=metro_config['max_pages'],
                max_cards=metro_config['max_cards']
            )
            
            if result['success']:
                print(f"✅ {result['metro_name']}: {result['saved_cards']} карточек")
                total_saved += result['saved_cards']
            else:
                print(f"❌ {metro_config['id']}: {result['error']}")
        
        print(f"🎉 ИТОГО: {total_saved} карточек")
        
    finally:
        if hasattr(parser, 'driver') and parser.driver:
            parser.driver.quit()
```

### 3. Неограниченный парсинг

```python
async def unlimited_example():
    parser = AvitoMetroParser(enable_db=True, headless=True)
    
    try:
        # Парсим все страницы (max_pages=0)
        result = await parser.parse_metro_by_id(
            metro_id=1,
            max_pages=0,      # Неограниченно страниц
            max_cards=10      # 10 карточек на страницу
        )
        
        print(f"Найдено: {result['total_cards']}, Сохранено: {result['saved_cards']}")
        
    finally:
        if hasattr(parser, 'driver') and parser.driver:
            parser.driver.quit()
```

## 🔧 Особенности работы

### Автоматическая инициализация
- Если браузер не запущен, метод автоматически:
  - Загружает список станций метро
  - Настраивает Selenium WebDriver
  - Загружает и применяет cookies

### Восстановление настроек
- После выполнения метода восстанавливаются оригинальные настройки `max_pages` и `max_cards`
- Это позволяет использовать один экземпляр парсера для разных задач

### Обработка ошибок
- Метод возвращает структурированный результат с информацией об ошибках
- Браузер автоматически закрывается при ошибках

## 📊 Сохраняемые поля

Метод извлекает и сохраняет **все поля** из объявлений:

- ✅ `title` - заголовок
- ✅ `price` - цена
- ✅ `rooms` - количество комнат (из заголовка)
- ✅ `area` - площадь в м² (из заголовка)
- ✅ `floor` - этаж (из заголовка)
- ✅ `total_floors` - всего этажей (из заголовка)
- ✅ `metro` - название метро
- ✅ `min_metro` - время до метро (из адреса)
- ✅ `address` - адрес (улица, дом)
- ✅ `person` - информация о продавце
- ✅ `person_type` - тип продавца
- ✅ `tags` - теги карточки
- ✅ `source_created` - дата публикации

## 🎯 Преимущества

1. **Гибкость** - можно задавать любые параметры для каждого метро
2. **Переиспользование** - один экземпляр парсера для разных задач
3. **Автоматизация** - не нужно вручную управлять браузером
4. **Мониторинг** - детальная информация о результатах
5. **Безопасность** - автоматическое восстановление настроек

## 📝 Примечания

- Метод **асинхронный** - используйте `await` при вызове
- Всегда **закрывайте браузер** в блоке `finally`
- Для **массового парсинга** используйте `parse_all_metro_stations()`
- **Cookies** загружаются автоматически из `avito_cookies.json`
