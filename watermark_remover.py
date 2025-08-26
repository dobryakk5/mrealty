#!/usr/bin/env python3
"""
Модуль для автоматического удаления водяных знаков ЦИАН с фотографий
Использует улучшенные алгоритмы компьютерного зрения для точного обнаружения
"""

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFont
import requests
import io
import base64
from typing import Optional, Dict, Any, Tuple
import re

class WatermarkRemover:
    """Класс для удаления водяных знаков ЦИАН с фотографий"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Улучшенные цветовые диапазоны для водяных знаков ЦИАН
        self.watermark_color_ranges = [
            # Белый цвет (более точный диапазон)
            ((245, 245, 245), (255, 255, 255)),
            # Светло-серый (более точный диапазон)
            ((220, 220, 220), (245, 245, 245)),
            # Очень светлый серый
            ((200, 200, 200), (220, 220, 220))
        ]
        
        # Размеры области поиска (более точные для ЦИАН)
        self.search_region_width = 0.35  # 35% ширины (вместо 50%)
        self.search_region_height = 0.25  # 25% высоты (вместо 50%)
    
    def detect_cian_watermark_region(self, image: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
        """
        Точный алгоритм обнаружения водяного знака ЦИАН
        
        Args:
            image: Изображение в формате numpy array (BGR)
            
        Returns:
            Tuple (x1, y1, x2, y2) с координатами области водяного знака или None
        """
        try:
            height, width = image.shape[:2]
            
            # Конвертируем в RGB для лучшего анализа
            rgb_image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            
            # Очень точная область поиска для надписи ЦИАН
            # Ищем в самом правом нижнем углу
            search_width = int(width * 0.12)   # 12% ширины - только самый правый край
            search_height = int(height * 0.08)  # 8% высоты - только самый нижний край
            
            x_start = width - search_width
            y_start = height - search_height
            
            print(f"🔍 Область поиска: ({x_start}, {y_start}) - ({width}, {height})")
            print(f"   Размер области поиска: {search_width} x {search_height} пикселей")
            
            # Вырезаем область поиска
            search_region = rgb_image[y_start:height, x_start:width]
            
            # Создаем маску для области поиска
            search_mask = np.zeros((search_height, search_width), dtype=np.uint8)
            
            # Ищем более широкий диапазон светлых цветов для надписи ЦИАН
            # Водяные знаки могут быть не чисто белыми
            color_ranges = [
                # Почти белый
                (np.array([240, 240, 240]), np.array([255, 255, 255])),
                # Светло-серый
                (np.array([200, 200, 200]), np.array([240, 240, 240])),
                # Очень светлый серый
                (np.array([180, 180, 180]), np.array([200, 200, 200]))
            ]
            
            for lower, upper in color_ranges:
                color_mask = cv2.inRange(search_region, lower, upper)
                search_mask = cv2.bitwise_or(search_mask, color_mask)
            
            # Применяем морфологические операции для очистки маски
            kernel = np.ones((2, 2), np.uint8)
            search_mask = cv2.morphologyEx(search_mask, cv2.MORPH_CLOSE, kernel)
            search_mask = cv2.morphologyEx(search_mask, cv2.MORPH_OPEN, kernel)
            
            # Находим контуры в маске
            contours, _ = cv2.findContours(search_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            if contours:
                print(f"🔍 Найдено контуров: {len(contours)}")
                
                # Фильтруем контуры по размеру и форме
                valid_contours = []
                for i, c in enumerate(contours):
                    area = cv2.contourArea(c)
                    x, y, w, h = cv2.boundingRect(c)
                    aspect_ratio = w / h if h > 0 else 0
                    
                    print(f"   Контур {i+1}: площадь={area:.0f}, размер={w}x{h}, соотношение={aspect_ratio:.2f}")
                    
                    # Ищем контуры с характеристиками текста
                    min_area = 30  # Минимум 30 пикселей (уменьшил)
                    max_area = search_width * search_height * 0.5  # Максимум 50% от области поиска (увеличил)
                    
                    if min_area < area < max_area:
                        # Текст обычно шире, чем высота, но не слишком широкий
                        if 0.5 < aspect_ratio < 5.0:  # Расширил диапазон
                            valid_contours.append(c)
                            print(f"     ✅ Валидный контур")
                        else:
                            print(f"     ❌ Неподходящее соотношение сторон")
                    else:
                        print(f"     ❌ Неподходящая площадь")
                
                if valid_contours:
                    # Выбираем контур с наибольшей площадью среди валидных
                    best_contour = max(valid_contours, key=cv2.contourArea)
                    
                    # Получаем bounding box
                    x, y, w, h = cv2.boundingRect(best_contour)
                    
                    # Переводим координаты в систему координат всего изображения
                    x1 = x_start + x
                    y1 = y_start + y
                    x2 = x1 + w
                    y2 = y1 + h
                    
                    # Расширяем область на 25% для полного покрытия водяного знака
                    expand_x = int(w * 0.25)
                    expand_y = int(h * 0.25)
                    
                    x1 = max(0, x1 - expand_x)
                    y1 = max(0, y1 - expand_y)
                    x2 = min(width, x2 + expand_x)
                    y2 = min(height, y2 + expand_y)
                    
                    print(f"🔍 Обнаружена область водяного знака: ({x1}, {y1}) - ({x2}, {y2})")
                    print(f"   Размер области: {x2-x1} x {y2-y1} пикселей")
                    print(f"   Метод: анализ контуров")
                    
                    return (x1, y1, x2, y2)
                else:
                    print("⚠️ Валидных контуров не найдено")
            else:
                print("⚠️ Контуры не найдены")
            
            # Если не нашли контуры, используем очень точный эвристический подход
            print("⚠️ Использую точный эвристический подход")
            return self._precise_heuristic_detection(image)
            
        except Exception as e:
            print(f"❌ Ошибка при определении области водяного знака: {e}")
            return None
    
    def _precise_heuristic_detection(self, image: np.ndarray) -> Tuple[int, int, int, int]:
        """
        Очень точный эвристический подход для обнаружения водяного знака ЦИАН
        """
        height, width = image.shape[:2]
        
        # Очень точные размеры для надписи ЦИАН
        watermark_width = int(width * 0.10)   # 10% ширины - только надпись
        watermark_height = int(height * 0.06)  # 6% высоты - только надпись
        
        # Позиционируем точно в правом нижнем углу
        x1 = width - watermark_width
        y1 = height - watermark_height
        x2 = width
        y2 = height
        
        print(f"🔍 Точный эвристический подход: ({x1}, {y1}) - ({x2}, {y2})")
        print(f"   Размер области: {watermark_width} x {watermark_height} пикселей")
        print(f"   Позиция: правый нижний угол")
        
        return (x1, y1, x2, y2)
    
    def detect_watermark_region(self, image: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
        """
        Основной метод обнаружения области водяного знака (для обратной совместимости)
        """
        return self.detect_cian_watermark_region(image)
    
    def remove_watermark_inpainting(self, image: np.ndarray, watermark_region: Tuple[int, int, int, int]) -> np.ndarray:
        """
        Удаляет водяной знак с помощью алгоритма inpainting
        """
        try:
            x1, y1, x2, y2 = watermark_region
            height, width = image.shape[:2]
            
            # Создаем маску для области водяного знака
            mask = np.zeros((height, width), dtype=np.uint8)
            mask[y1:y2, x1:x2] = 255
            
            # Применяем алгоритм inpainting
            result = cv2.inpaint(image, mask, 3, cv2.INPAINT_TELEA)
            
            return result
            
        except Exception as e:
            print(f"❌ Ошибка при удалении водяного знака inpainting: {e}")
            return image
    
    def remove_watermark_blur(self, image: np.ndarray, watermark_region: Tuple[int, int, int, int]) -> np.ndarray:
        """
        Удаляет водяной знак с помощью размытия и наложения текста
        """
        try:
            x1, y1, x2, y2 = watermark_region
            height, width = image.shape[:2]
            
            # Создаем копию изображения
            result = image.copy()
            
            # Применяем размытие к области водяного знака
            blurred_region = cv2.GaussianBlur(result[y1:y2, x1:x2], (15, 15), 0)
            result[y1:y2, x1:x2] = blurred_region
            
            # Конвертируем в PIL для добавления текста
            pil_image = Image.fromarray(cv2.cvtColor(result, cv2.COLOR_BGR2RGB))
            
            # Создаем новый слой для текста
            draw = ImageDraw.Draw(pil_image)
            
            # Определяем размер шрифта
            font_size = min((x2-x1) // 8, (y2-y1) // 3)
            
            # Пытаемся использовать стандартные шрифты
            font_paths = [
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
                "/System/Library/Fonts/Arial.ttf",
                "/System/Library/Fonts/Helvetica.ttc",
                "C:/Windows/Fonts/arial.ttf"
            ]
            
            font = None
            for font_path in font_paths:
                try:
                    font = ImageFont.truetype(font_path, font_size)
                    break
                except:
                    continue
            
            if font is None:
                font = ImageFont.load_default()
                font_size = min((x2-x1) // 12, (y2-y1) // 5)
            
            # Определяем размеры текста
            bbox = draw.textbbox((0, 0), "МИЭЛЬ", font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
            
            # Позиционируем текст в центре области
            text_x = x1 + (x2 - x1 - text_width) // 2
            text_y = y1 + (y2 - y1 - text_height) // 2
            
            # Создаем прозрачный слой для текста
            text_layer = Image.new('RGBA', pil_image.size, (0, 0, 0, 0))
            text_draw = ImageDraw.Draw(text_layer)
            
            # Белый цвет с прозрачностью
            transparent_white = (255, 255, 255, 179)
            
            # Рисуем текст с обводкой
            stroke_width = max(1, int(font_size * 0.02))
            
            for dx in range(-stroke_width, stroke_width + 1):
                for dy in range(-stroke_width, stroke_width + 1):
                    if dx == 0 and dy == 0:
                        continue
                    text_draw.text((text_x + dx, text_y + dy), "МИЭЛЬ", font=font, fill=transparent_white)
            
            # Рисуем основной текст
            text_draw.text((text_x, text_y), "МИЭЛЬ", font=font, fill=transparent_white)
            
            # Объединяем слои
            pil_image = pil_image.convert('RGBA')
            pil_image = Image.alpha_composite(pil_image, text_layer)
            
            # Конвертируем обратно в RGB
            pil_image = pil_image.convert('RGB')
            
            # Конвертируем обратно в numpy array
            result = cv2.cvtColor(np.array(pil_image), cv2.COLOR_RGB2BGR)
            
            return result
            
        except Exception as e:
            print(f"❌ Ошибка при удалении водяного знака blur: {e}")
            return image
    
    def process_image_from_url(self, photo_url: str, method: str = 'auto') -> Optional[Dict[str, Any]]:
        """
        Обрабатывает изображение по URL, удаляя водяной знак
        """
        try:
            # Скачиваем изображение
            response = self.session.get(photo_url, timeout=10)
            response.raise_for_status()
            image_data = response.content
            
            # Конвертируем в numpy array
            nparr = np.frombuffer(image_data, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if image is None:
                print(f"❌ Не удалось декодировать изображение: {photo_url}")
                return None
            
            # Определяем область водяного знака
            watermark_region = self.detect_cian_watermark_region(image)
            
            if watermark_region is None:
                print(f"⚠️ Не удалось определить область водяного знака для: {photo_url}")
                return None
            
            # Удаляем водяной знак выбранным методом
            if method == 'inpainting':
                processed_image = self.remove_watermark_inpainting(image, watermark_region)
            elif method == 'blur':
                processed_image = self.remove_watermark_blur(image, watermark_region)
            else:  # auto
                # Пробуем inpainting, если не получилось - blur
                try:
                    processed_image = self.remove_watermark_inpainting(image, watermark_region)
                except:
                    processed_image = self.remove_watermark_blur(image, watermark_region)
            
            # Конвертируем обратно в base64
            success, buffer = cv2.imencode('.jpg', processed_image, [cv2.IMWRITE_JPEG_QUALITY, 95])
            
            if not success:
                print(f"❌ Не удалось закодировать обработанное изображение: {photo_url}")
                return None
            
            # Конвертируем в base64
            base64_data = base64.b64encode(buffer).decode('utf-8')
            
            return {
                'base64': base64_data,
                'format': 'jpeg',
                'size': (processed_image.shape[1], processed_image.shape[0]),
                'watermark_removed': True,
                'method': method
            }
            
        except Exception as e:
            print(f"❌ Ошибка при обработке изображения {photo_url}: {e}")
            return None
    
    def batch_process_images(self, photo_urls: list, method: str = 'auto') -> list:
        """
        Обрабатывает несколько изображений пакетно
        """
        processed_images = []
        
        for i, url in enumerate(photo_urls):
            print(f"🔄 Обрабатываю изображение {i+1}/{len(photo_urls)}: {url}")
            
            result = self.process_image_from_url(url, method)
            if result:
                processed_images.append({
                    'url': url,
                    **result
                })
                print(f"✅ Водяной знак удален: {url}")
            else:
                print(f"❌ Не удалось обработать: {url}")
        
        print(f"📊 Обработано изображений: {len(processed_images)}/{len(photo_urls)}")
        return processed_images

# Создаем глобальный экземпляр для использования в других модулях
watermark_remover = WatermarkRemover()
