# Интеграция модуля удаления водяных знаков ЦИАН

## Быстрая интеграция в существующий код

### 1. В listings_processor.py

```python
# Находим функцию, которая обрабатывает фотографии
def process_listing_photos(self, photo_urls):
    # Заменяем старый вызов:
    # processed_photos = photo_processor.process_photos_for_embedded_html(photo_urls, remove_watermarks=True)
    
    # На новый с автоматическим удалением водяных знаков:
    processed_photos = photo_processor.process_photos_for_embedded_html(
        photo_urls,
        remove_watermarks=False,      # Отключаем старый метод
        auto_remove_cian=True,       # Включаем автоматическое удаление
        watermark_method='auto'       # Метод удаления
    )
    
    return processed_photos
```

### 2. В avito_parser_integration.py

```python
# В функции extract_photos или аналогичной
def extract_photos(self):
    # ... код извлечения URL фотографий ...
    
    # Обрабатываем фотографии с удалением водяных знаков
    processed_photos = photo_processor.process_photos_for_embedded_html(
        photos_urls,
        auto_remove_cian=True,
        watermark_method='inpainting'  # Рекомендуемый метод
    )
    
    return processed_photos
```

### 3. В text_handlers.py

```python
# В функции _handle_listings_export
async def _handle_listings_export(urls, message):
    # ... существующий код ...
    
    # При обработке фотографий для Excel
    if listing.get('photos'):
        processed_photos = photo_processor.process_photos_for_embedded_html(
            listing['photos'],
            auto_remove_cian=True,
            watermark_method='auto'
        )
        # ... дальнейшая обработка ...
```

## Параметры настройки

### watermark_method:
- `'auto'` - автоматический выбор лучшего метода
- `'inpainting'` - полное удаление с восстановлением фона (рекомендуется)
- `'blur'` - размытие + замена на "МИЭЛЬ"

### auto_remove_cian:
- `True` - включить автоматическое удаление водяных знаков
- `False` - отключить (использовать обычную обработку)

## Обратная совместимость

Модуль полностью совместим с существующим кодом:
- Старые параметры `remove_watermarks=True` продолжают работать
- Новые параметры опциональны
- Fallback на обычную обработку при ошибках

## Пример полной интеграции

```python
from photo_processor import photo_processor

def process_real_estate_photos(photo_urls):
    """
    Обрабатывает фотографии недвижимости с автоматическим удалением водяных знаков
    """
    try:
        # Обрабатываем фотографии
        processed_photos = photo_processor.process_photos_for_embedded_html(
            photo_urls,
            remove_watermarks=False,      # Отключаем старый метод
            auto_remove_cian=True,       # Включаем автоматическое удаление
            watermark_method='inpainting' # Используем inpainting
        )
        
        print(f"✅ Обработано {len(processed_photos)} фотографий")
        
        # Фильтруем успешно обработанные
        successful_photos = [p for p in processed_photos if p.get('watermark_removed')]
        print(f"🎯 Водяные знаки удалены с {len(successful_photos)} фотографий")
        
        return processed_photos
        
    except Exception as e:
        print(f"❌ Ошибка при обработке фотографий: {e}")
        # Fallback на обычную обработку
        return photo_processor.process_photos_for_embedded_html(photo_urls)
```

## Проверка работы

После интеграции проверьте:

1. **Логи**: Должны появиться сообщения о удалении водяных знаков
2. **Результаты**: В `processed_photos` должно быть поле `watermark_removed: True`
3. **Качество**: Водяные знаки должны быть незаметны или заменены на "МИЭЛЬ"

## Устранение проблем

### Модуль не импортируется:
```bash
# Проверьте наличие файла
ls -la watermark_remover.py

# Проверьте зависимости
pip install opencv-python pillow numpy
```

### Ошибки OpenCV:
```bash
# Переустановите OpenCV
pip uninstall opencv-python
pip install opencv-python
```

### Медленная работа:
- Используйте `watermark_method='blur'` для ускорения
- Обрабатывайте изображения пакетно
- Уменьшите качество JPEG (в watermark_remover.py)
