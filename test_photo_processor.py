#!/usr/bin/env python3
"""
Тест для упрощенного модуля photo_processor
"""

from photo_processor import photo_processor

def test_photo_processor():
    """Тестирует упрощенные функции photo_processor"""
    
    print("🔍 Тестирую упрощенный модуль photo_processor...")
    
    # Тестовые ссылки на фотографии
    test_photo_urls = [
        "https://images.cdn-cian.ru/images/kvartira-sosenskoe-kuprina-prospekt-2571476328-1.jpg",
        "https://images.cdn-cian.ru/images/kvartira-buninskie-kvartaly-2571494327-1.jpg",
        "https://images.cdn-cian.ru/images/kvartira-sosenskoe-kuprina-prospekt-2571512755-1.jpg"
    ]
    
    print(f"📸 Тестирую с {len(test_photo_urls)} тестовыми ссылками")
    
    try:
        # Тест 1: Обработка для встроенного HTML (base64)
        print("\n🔗 Тест 1: Обработка для встроенного HTML")
        print("⚠️ Это может занять время...")
        embedded_photos = photo_processor.process_photos_for_embedded_html(test_photo_urls)
        print(f"Обработано для встраивания: {len(embedded_photos)}")
        
        for photo in embedded_photos:
            print(f"  Фото {photo['index']}: {photo['type']}")
            if photo['type'] == 'base64':
                print(f"    MIME: {photo['mime_type']}, Размер: {photo['size']} байт")
        
        # Тест 2: Генерация HTML сетки для встроенных фото
        print("\n🔧 Тест 2: Генерация HTML сетки для встроенных фото")
        photo_grid_html = photo_processor.generate_photo_grid_html(embedded_photos, 'embedded')
        print(f"HTML сетки сгенерирован, размер: {len(photo_grid_html)} символов")
        
        # Тест 3: Генерация HTML сетки для обычных ссылок
        print("\n🖼️ Тест 3: Генерация HTML сетки для обычных ссылок")
        url_photos = [{'url': url, 'index': i+1, 'type': 'url'} for i, url in enumerate(test_photo_urls)]
        url_grid_html = photo_processor.generate_photo_grid_html(url_photos, 'url')
        print(f"HTML сетки для ссылок сгенерирован, размер: {len(url_grid_html)} символов")
        
        # Тест 4: Проверка содержимого HTML
        print("\n📄 Тест 4: Проверка содержимого HTML")
        if 'photo-grid' in photo_grid_html:
            print("✅ HTML содержит класс 'photo-grid'")
        if 'photo-item' in photo_grid_html:
            print("✅ HTML содержит класс 'photo-item'")
        if 'img' in photo_grid_html:
            print("✅ HTML содержит теги 'img'")
        
        print("\n✅ Все тесты завершены успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    test_photo_processor()
