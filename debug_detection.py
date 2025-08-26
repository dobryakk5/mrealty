#!/usr/bin/env python3
"""
Диагностика проблемы с детектом водяных знаков
"""

import cv2
import numpy as np
import requests
from watermark_remover import watermark_remover

def debug_detection():
    """Диагностирует проблему с детектом"""
    
    print("🔍 Диагностика детекта водяных знаков ЦИАН")
    print("=" * 60)
    
    url = "https://images.cdn-cian.ru/images/2504097329-1.jpg"
    
    try:
        # Скачиваем изображение
        print(f"📥 Скачиваю изображение: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        image_data = response.content
        
        # Конвертируем в numpy array
        nparr = np.frombuffer(image_data, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if image is None:
            print("❌ Не удалось декодировать изображение")
            return
        
        height, width = image.shape[:2]
        print(f"📏 Размер изображения: {width} x {height}")
        
        # Анализируем правую нижнюю часть изображения
        print(f"\n🔍 Анализирую правую нижнюю часть...")
        
        # Вырезаем правую нижнюю часть для анализа
        right_width = int(width * 0.15)  # 15% справа
        bottom_height = int(height * 0.10)  # 10% снизу
        
        x_start = width - right_width
        y_start = height - bottom_height
        
        print(f"   Область анализа: ({x_start}, {y_start}) - ({width}, {height})")
        print(f"   Размер области: {right_width} x {bottom_height}")
        
        # Вырезаем область
        roi = image[y_start:height, x_start:width]
        
        # Конвертируем в RGB для анализа цветов
        roi_rgb = cv2.cvtColor(roi, cv2.COLOR_BGR2RGB)
        
        # Анализируем цвета в области
        print(f"\n🎨 Анализ цветов в области...")
        
        # Ищем белые пиксели
        white_threshold = 250
        white_pixels = np.sum(np.all(roi_rgb >= white_threshold, axis=2))
        total_pixels = roi_rgb.shape[0] * roi_rgb.shape[1]
        white_percentage = (white_pixels / total_pixels) * 100
        
        print(f"   Белых пикселей (>=250): {white_pixels} из {total_pixels} ({white_percentage:.1f}%)")
        
        # Создаем маску для белых пикселей
        white_mask = np.all(roi_rgb >= white_threshold, axis=2).astype(np.uint8) * 255
        
        # Находим контуры в маске
        contours, _ = cv2.findContours(white_mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        print(f"   Найдено контуров: {len(contours)}")
        
        if contours:
            print(f"\n📐 Анализ контуров:")
            for i, c in enumerate(contours):
                area = cv2.contourArea(c)
                x, y, w, h = cv2.boundingRect(c)
                aspect_ratio = w / h if h > 0 else 0
                
                print(f"   Контур {i+1}: площадь={area:.0f}, размер={w}x{h}, соотношение={aspect_ratio:.2f}")
        
        # Сохраняем маску для визуального анализа
        mask_filename = "debug_white_mask.jpg"
        cv2.imwrite(mask_filename, white_mask)
        print(f"\n💾 Маска белых пикселей сохранена: {mask_filename}")
        
        # Сохраняем область анализа
        roi_filename = "debug_roi.jpg"
        cv2.imwrite(roi_filename, roi)
        print(f"💾 Область анализа сохранена: {roi_filename}")
        
        # Тестируем текущий алгоритм
        print(f"\n🧪 Тестирую текущий алгоритм детекта...")
        watermark_region = watermark_remover.detect_cian_watermark_region(image)
        
        if watermark_region:
            x1, y1, x2, y2 = watermark_region
            print(f"✅ Алгоритм вернул область: ({x1}, {y1}) - ({x2}, {y2})")
            
            # Создаем визуализацию
            vis_image = image.copy()
            cv2.rectangle(vis_image, (x1, y1), (x2, y2), (0, 255, 0), 3)
            cv2.putText(vis_image, f"Detected: ({x1},{y1})-({x2},{y2})", 
                       (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
            
            # Рисуем область анализа
            cv2.rectangle(vis_image, (x_start, y_start), (width, height), (255, 0, 0), 2)
            cv2.putText(vis_image, f"ROI: ({x_start},{y_start})-({width},{height})", 
                       (x_start, y_start-10), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 0, 0), 2)
            
            vis_filename = "debug_visualization.jpg"
            cv2.imwrite(vis_filename, vis_image)
            print(f"💾 Визуализация сохранена: {vis_filename}")
            
        else:
            print("❌ Алгоритм не вернул область")
            
    except Exception as e:
        print(f"❌ Ошибка при диагностике: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_detection()
    print("\n✅ Диагностика завершена!")
    print("🔍 Проверьте созданные файлы для анализа")
