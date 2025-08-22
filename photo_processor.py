#!/usr/bin/env python3
"""
Модуль для обработки фотографий из объявлений CIAN
Работает только с готовыми ссылками, вставляет все фото без ограничений
"""

import requests
import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont
from typing import List, Dict, Any, Optional
import io
import base64

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
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def download_and_convert_to_base64(self, photo_url: str, timeout: int = 10) -> Optional[Dict[str, str]]:
        """
        Скачивает фото и конвертирует в base64
        """
        try:
            response = self.session.get(photo_url, timeout=timeout)
            response.raise_for_status()
            
            # Конвертируем в base64
            image_data = response.content
            base64_data = base64.b64encode(image_data).decode('utf-8')
            
            # Определяем тип изображения
            image = Image.open(io.BytesIO(image_data))
            format_type = image.format.lower()
            
            return {
                'base64': base64_data,
                'format': format_type,
                'size': image.size
            }
        except Exception as e:
            print(f"❌ Ошибка при обработке {photo_url}: {e}")
            return None

    def process_photos_for_embedded_html(self, photo_urls: List[str], remove_watermarks: bool = False) -> List[Dict[str, Any]]:
        """
        Обрабатывает фотографии для встроенного HTML
        """
        processed_photos = []
        
        for i, url in enumerate(photo_urls):
            try:
                if remove_watermarks:
                    # Применяем метод 6: МИЭЛЬ поверх водяного знака
                    result = self.method6_miel_overlay(url)
                    if result:
                        processed_photos.append({
                            'url': url,
                            'base64': result['base64'],
                            'format': result['format'],
                            'size': result['size'],
                            'processed': True
                        })
                else:
                    # Обычная конвертация в base64
                    result = self.download_and_convert_to_base64(url)
                    if result:
                        processed_photos.append({
                            'url': url,
                            'base64': result['base64'],
                            'format': result['format'],
                            'size': result['size'],
                            'processed': False
                        })
                        
            except Exception as e:
                print(f"❌ Ошибка при обработке фото {i+1}: {e}")
                continue
        
        return processed_photos

    def method6_miel_overlay(self, photo_url: str) -> Optional[Dict[str, Any]]:
        """
        Метод 6: Замена водяного знака CIAN на "МИЭЛЬ" с увеличенной областью на 50%
        """
        try:
            # Скачиваем фото
            response = self.session.get(photo_url, timeout=10)
            response.raise_for_status()
            image_data = response.content
            
            # Конвертируем в numpy array
            nparr = np.frombuffer(image_data, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if image is None:
                print(f"❌ Не удалось декодировать изображение: {photo_url}")
                return None
            
            height, width = image.shape[:2]
            
            # Определяем область водяного знака (правая нижняя часть)
            # Увеличиваем область на 50%
            watermark_width = int(width * 0.25 * 1.5)  # 25% ширины + 50%
            watermark_height = int(height * 0.15 * 1.5)  # 15% высоты + 50%
            
            x1 = width - watermark_width
            y1 = height - watermark_height
            x2 = width
            y2 = height
            
            # Создаем маску для области водяного знака
            mask = np.zeros((height, width), dtype=np.uint8)
            mask[y1:y2, x1:x2] = 255
            
            # Применяем легкое размытие к области водяного знака для лучшего наложения текста
            blurred_region = cv2.GaussianBlur(image[y1:y2, x1:x2], (15, 15), 0)
            image[y1:y2, x1:x2] = blurred_region
            
            # Конвертируем в PIL для добавления текста
            pil_image = Image.fromarray(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
            
            # Создаем новый слой для текста
            draw = ImageDraw.Draw(pil_image)
            
            # Пытаемся использовать системный шрифт, если не получится - используем дефолтный
            try:
                # Попробуем найти подходящий шрифт
                font_size = min((x2-x1) // 8, (y2-y1) // 3)  # Базовый размер шрифта
                # Увеличиваем размер шрифта на 50%, затем еще в 2 раза, затем еще на 20% (итого в 3.6 раза)
                font_size = int(font_size * 1.5 * 2 * 1.2)
                font = ImageFont.truetype("/System/Library/Fonts/Arial.ttf", font_size)
            except:
                try:
                    font_size = min((x2-x1) // 10, (y2-y1) // 4)  # Базовый размер шрифта
                    # Увеличиваем размер шрифта на 50%, затем еще в 2 раза, затем еще на 20% (итого в 3.6 раза)
                    font_size = int(font_size * 1.5 * 2 * 1.2)
                    font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
                except:
                    # Используем дефолтный шрифт
                    font = ImageFont.load_default()
                    font_size = min((x2-x1) // 10, (y2-y1) // 4)  # Базовый размер шрифта
                    # Увеличиваем размер шрифта на 50%, затем еще в 2 раза, затем еще на 20% (итого в 3.6 раза)
                    font_size = int(font_size * 1.5 * 2 * 1.2)
            
            # Определяем размеры текста
            bbox = draw.textbbox((0, 0), "МИЭЛЬ", font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
            
            # Позиционируем текст: поднимаем на высоту одной буквы М и сдвигаем влево на 2 буквы М
            # Высота одной буквы М примерно равна text_height
            # Ширина одной буквы М примерно равна text_width / 5 (так как в слове "МИЭЛЬ" 5 букв)
            single_letter_height = text_height
            single_letter_width = text_width / 5
            
            # Сдвигаем влево на 2 буквы М
            left_shift = int(single_letter_width * 2)
            # Поднимаем на высоту одной буквы М
            up_shift = int(single_letter_height)
            # Опускаем на 3% ниже на экране
            down_shift = int(height * 0.03)
            
            # Позиционируем текст с учетом сдвигов (относительно полного изображения)
            # Центрируем в области водяного знака, а не в центре всего изображения
            text_x = x1 + (x2 - x1 - text_width) // 2 - left_shift
            text_y = y1 + (y2 - y1 - text_height) // 2 - up_shift + down_shift
            
            # Создаем белый цвет с 20% непрозрачности (80% прозрачности) - еще на 10% прозрачнее
            # RGBA: (255, 255, 255, 51) где 51 = 255 * 0.2 = 20% непрозрачности
            transparent_white = (255, 255, 255, 51)
            
            # Рисуем текст с прозрачностью и увеличенной толщиной линий на 15%
            # Для создания толщины рисуем текст несколько раз со смещением
            stroke_width = max(1, int(font_size * 0.02))  # Базовая толщина обводки
            stroke_width = int(stroke_width * 1.15)  # Увеличиваем на 15%
            
            # Рисуем основной текст с толщиной
            for dx in range(-stroke_width, stroke_width + 1):
                for dy in range(-stroke_width, stroke_width + 1):
                    if dx == 0 and dy == 0:
                        continue  # Основной текст рисуем отдельно
                    # Рисуем обводку с той же прозрачностью
                    draw.text((text_x + dx, text_y + dy), "МИЭЛЬ", font=font, fill=transparent_white)
            
            # Рисуем основной текст поверх
            draw.text((text_x, text_y), "МИЭЛЬ", font=font, fill=transparent_white)
            
            # Конвертируем обратно в base64
            img_buffer = io.BytesIO()
            pil_image.save(img_buffer, format='JPEG', quality=95)
            img_buffer.seek(0)
            
            base64_data = base64.b64encode(img_buffer.getvalue()).decode('utf-8')
            
            return {
                'base64': base64_data,
                'format': 'jpeg',
                'size': pil_image.size
            }
            
        except Exception as e:
            print(f"❌ Ошибка метода 6 (МИЭЛЬ): {e}")
            return None

    def generate_photo_grid_html(self, processed_photos: List[Dict[str, Any]], grid_type: str = 'url') -> str:
        """
        Генерирует HTML для сетки фотографий
        """
        if not processed_photos:
            return "<p>❌ Фотографии не найдены</p>"
        
        html_parts = ['<div class="photo-grid">']
        
        for i, photo in enumerate(processed_photos):
            if photo.get('processed') and photo.get('base64'):
                # Для обработанных фото используем base64
                img_src = f"data:image/{photo['format']};base64,{photo['base64']}"
                alt_text = f"Фото {i+1}"
            elif photo.get('base64'):
                # Для обычных фото также используем base64
                img_src = f"data:image/{photo['format']};base64,{photo['base64']}"
                alt_text = f"Фото {i+1}"
            else:
                # Fallback на URL
                img_src = photo['url']
                alt_text = f"Фото {i+1}"
            
            html_parts.append(f'''
                <div class="photo-item">
                    <img src="{img_src}" alt="{alt_text}" loading="lazy">
                </div>
            ''')
        
        html_parts.append('</div>')
        return ''.join(html_parts)

# Создаем глобальный экземпляр для использования в других модулях
photo_processor = PhotoProcessor()
