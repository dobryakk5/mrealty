#!/usr/bin/env python3
"""
Тестовый скрипт для проверки функции извлечения фотографий
"""

import requests
from bs4 import BeautifulSoup
from listings_processor import extract_photo_urls

def test_photo_extraction():
    """Тестирует извлечение фотографий с реальной страницы CIAN"""
    
    # Тестовая ссылка (замените на реальную)
    test_url = "https://www.cian.ru/rent/flat/2571476517/"
    
    print(f"🔍 Тестирую извлечение фотографий с: {test_url}")
    
    try:
        # Получаем страницу
        headers = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/115.0.0.0 Safari/537.36'
            ),
            'Accept-Language': 'ru-RU,ru;q=0.9',
        }
        
        response = requests.get(test_url, headers=headers)
        response.encoding = 'utf-8'
        
        if response.status_code == 200:
            print("✅ Страница успешно загружена")
            
            # Парсим HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Извлекаем фотографии
            photo_urls = extract_photo_urls(soup)
            
            print(f"📸 Найдено фотографий: {len(photo_urls)}")
            
            if photo_urls:
                print("\n🔗 Ссылки на фотографии:")
                for i, url in enumerate(photo_urls, 1):
                    print(f"{i:2d}. {url}")
            else:
                print("❌ Фотографии не найдены")
                
                # Попробуем найти галерею
                gallery = soup.find('div', {'data-name': 'GalleryInnerComponent'})
                if gallery:
                    print("✅ Галерея найдена, но фотографии не извлечены")
                    print(f"HTML галереи: {gallery.prettify()[:500]}...")
                else:
                    print("❌ Галерея не найдена")
                    
        else:
            print(f"❌ Ошибка загрузки страницы: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    test_photo_extraction()
