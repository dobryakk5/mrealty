#!/usr/bin/env python3
"""
Тестовый скрипт для проверки генерации HTML с фотографиями
"""

from listings_processor import generate_html_gallery

def test_html_generation():
    """Тестирует генерацию HTML с фотографиями"""
    
    # Тестовые ссылки
    test_urls = [
        "https://www.cian.ru/sale/flat/320036040/"
        ,"https://www.cian.ru/sale/flat/320872910/"
        ,"https://www.cian.ru/rent/flat/320775304/"
    ]
    
    # Тестовый подзаголовок
    test_subtitle = "2-комнатные квартиры в новостройках Москвы"
    
    print(f"🔍 Тестирую генерацию HTML для {len(test_urls)} ссылок")
    print(f"📝 Подзаголовок: '{test_subtitle}'")
    
    try:
        # Генерируем HTML с подзаголовком
        html_content = generate_html_gallery(test_urls, user_id=12345, subtitle=test_subtitle)
        
        print(f"✅ HTML успешно сгенерирован")
        print(f"📏 Размер HTML: {len(html_content)} символов")
        
        # Сохраняем в файл для проверки
        output_file = "test_gallery_with_subtitle.html"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"💾 HTML сохранен в файл: {output_file}")
        print(f"🌐 Откройте файл в браузере для просмотра")
        
        # Показываем начало HTML
        print("\n📄 Начало HTML:")
        print(html_content[:800] + "...")
        
        # Проверяем наличие подзаголовка
        if test_subtitle in html_content:
            print(f"✅ Подзаголовок '{test_subtitle}' найден в HTML")
        else:
            print(f"❌ Подзаголовок '{test_subtitle}' НЕ найден в HTML")
            
        # Проверяем наличие "Вариант #"
        if "Вариант #" in html_content:
            print("✅ Заголовки 'Вариант #' найдены в HTML")
        else:
            print("❌ Заголовки 'Вариант #' НЕ найдены в HTML")
        
    except Exception as e:
        print(f"❌ Ошибка генерации HTML: {e}")

if __name__ == "__main__":
    test_html_generation()
