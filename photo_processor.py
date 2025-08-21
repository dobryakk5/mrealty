#!/usr/bin/env python3
"""
Модуль для обработки фотографий из объявлений CIAN
Работает только с готовыми ссылками, вставляет все фото без ограничений
"""

import base64
import requests
from typing import List, Dict, Any, Optional

# Заголовки для HTTP-запросов
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/115.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'ru-RU,ru;q=0.9',
}

class PhotoProcessor:
    """Класс для обработки фотографий из объявлений CIAN"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def download_and_convert_to_base64(self, photo_url: str, timeout: int = 10) -> Optional[Dict[str, str]]:
        """
        Загружает изображение и конвертирует в base64
        
        Args:
            photo_url: URL изображения
            timeout: Таймаут загрузки в секундах
            
        Returns:
            Словарь с MIME-типом и base64 данными или None при ошибке
        """
        try:
            response = self.session.get(photo_url, timeout=timeout)
            if response.status_code == 200:
                img_base64 = base64.b64encode(response.content).decode('utf-8')
                img_mime = response.headers.get('content-type', 'image/jpeg')
                
                return {
                    'mime_type': img_mime,
                    'base64_data': img_base64,
                    'size': len(response.content)
                }
            else:
                print(f"Ошибка загрузки изображения {photo_url}: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Ошибка при загрузке изображения {photo_url}: {e}")
            return None
    
    def process_photos_for_embedded_html(self, photo_urls: List[str]) -> List[Dict[str, Any]]:
        """
        Обрабатывает фотографии для встроенного HTML (base64)
        Вставляет ВСЕ фотографии без ограничений
        
        Args:
            photo_urls: Список URL фотографий
            
        Returns:
            Список обработанных фотографий с base64 данными
        """
        processed_photos = []
        
        for i, url in enumerate(photo_urls):
            # Загружаем и конвертируем в base64
            base64_data = self.download_and_convert_to_base64(url)
            
            if base64_data:
                processed_photos.append({
                    'url': url,
                    'index': i + 1,
                    'type': 'base64',
                    'mime_type': base64_data['mime_type'],
                    'base64_data': base64_data['base64_data'],
                    'size': base64_data['size']
                })
            else:
                # Fallback для неудачной загрузки
                processed_photos.append({
                    'url': url,
                    'index': i + 1,
                    'type': 'fallback'
                })
        
        return processed_photos
    
    def generate_photo_grid_html(self, processed_photos: List[Dict[str, Any]], grid_type: str = 'url') -> str:
        """
        Генерирует HTML для сетки фотографий
        
        Args:
            processed_photos: Список обработанных фотографий
            grid_type: Тип сетки ('url' или 'embedded')
            
        Returns:
            HTML строка для сетки фотографий
        """
        if not processed_photos:
            return '<p class="no-photos">📷 Фотографии не найдены</p>'
        
        html_parts = [f'<div class="photo-grid">']
        
        for photo in processed_photos:
            if grid_type == 'embedded' and photo['type'] == 'base64':
                # Встроенное изображение в base64
                html_parts.append(f"""
                <div class="photo-item">
                    <img src="data:{photo['mime_type']};base64,{photo['base64_data']}" alt="Фото {photo['index']}">
                </div>
                """)
            elif grid_type == 'embedded' and photo['type'] == 'fallback':
                # Fallback для встроенных изображений
                html_parts.append(f"""
                <div class="photo-item">
                    <div class="photo-fallback">
                        <div>📷 Фото {photo['index']}</div>
                        <div class="photo-link">
                            <a href="{photo['url']}" target="_blank">Открыть фото</a>
                        </div>
                    </div>
                </div>
                """)
            else:
                # Обычное изображение по ссылке
                html_parts.append(f"""
                <div class="photo-item">
                    <img src="{photo['url']}" alt="Фото {photo['index']}" 
                         onerror="this.style.display='none'; this.nextElementSibling.style.display='block';"
                         loading="lazy">
                    <div class="photo-fallback" style="display: none;">
                        <div>📷 Фото {photo['index']}</div>
                        <div class="photo-link">
                            <a href="{photo['url']}" target="_blank">Открыть фото</a>
                        </div>
                    </div>
                </div>
                """)
        
        html_parts.append('</div>')
        return ''.join(html_parts)

# Создаем глобальный экземпляр для использования в других модулях
photo_processor = PhotoProcessor()
