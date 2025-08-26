#!/usr/bin/env python3
"""
Тест улучшенного алгоритма обнаружения водяных знаков ЦИАН
"""

from watermark_remover import watermark_remover
import cv2
import numpy as np
import requests

def test_improved_detection():
    """Тестирует улучшенный алгоритм обнаружения"""
    
    print("🧪 Тест улучшенного алгоритма обнаружения водяных знаков ЦИАН")
    print("=" * 70)
    
    # Тестовые URL изображений
    test_urls = [
        "https://images.cdn-cian.ru/images/2504097329-1.jpg",
        "https://images.cdn-cian.ru/images/2504097383-1.jpg"
    ]
    
    for i, url in enumerate(test_urls):
        print(f"\n📸 Тестирую изображение {i+1}: {url}")
        print("-" * 50)
        
        try:
            # Скачиваем изображение
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            image_data = response.content
            
            # Конвертируем в numpy array
            nparr = np.frombuffer(image_data, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if image is None:
                print("❌ Не удалось декодировать изображение")
                continue
            
            height, width = image.shape[:2]
            print(f"📏 Размер изображения: {width} x {height}")
            
            # Тестируем улучшенный алгоритм
            print("\n🔍 Тестирую улучшенный алгоритм обнаружения...")
            watermark_region = watermark_remover.detect_cian_watermark_region(image)
            
            if watermark_region:
                x1, y1, x2, y2 = watermark_region
                region_width = x2 - x1
                region_height = y2 - y1
                
                print(f"✅ Область водяного знака определена:")
                print(f"   Координаты: ({x1}, {y1}) - ({x2}, {y2})")
                print(f"   Размер области: {region_width} x {region_height} пикселей")
                print(f"   Позиция: правая нижняя часть")
                
                # Проверяем, что область действительно в правой нижней части
                right_quarter = width * 0.75
                bottom_quarter = height * 0.75
                
                if x1 > right_quarter and y1 > bottom_quarter:
                    print("✅ Область корректно расположена в правой нижней части")
                else:
                    print("⚠️ Область может быть расположена не в том месте")
                
                # Проверяем размеры области
                expected_width = width * 0.25
                expected_height = height * 0.18
                
                width_ratio = region_width / expected_width
                height_ratio = region_height / expected_height
                
                print(f"📊 Соотношение размеров:")
                print(f"   Ширина: {width_ratio:.2f}x от ожидаемой")
                print(f"   Высота: {height_ratio:.2f}x от ожидаемой")
                
                if 0.5 < width_ratio < 2.0 and 0.5 < height_ratio < 2.0:
                    print("✅ Размеры области в разумных пределах")
                else:
                    print("⚠️ Размеры области могут быть некорректными")
                    
            else:
                print("❌ Не удалось определить область водяного знака")
                
        except Exception as e:
            print(f"❌ Ошибка при тестировании: {e}")
    
    print("\n" + "=" * 70)
    print("✅ Тестирование завершено!")

def test_visualization():
    """Тестирует визуализацию обнаруженных областей"""
    
    print("\n\n🎨 Тест визуализации обнаруженных областей")
    print("=" * 70)
    
    url = "https://images.cdn-cian.ru/images/2504097329-1.jpg"
    
    try:
        # Скачиваем изображение
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        image_data = response.content
        
        # Конвертируем в numpy array
        nparr = np.frombuffer(image_data, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if image is None:
            print("❌ Не удалось декодировать изображение")
            return
        
        # Определяем область водяного знака
        watermark_region = watermark_remover.detect_cian_watermark_region(image)
        
        if watermark_region:
            x1, y1, x2, y2 = watermark_region
            
            # Создаем копию изображения для рисования
            vis_image = image.copy()
            
            # Рисуем прямоугольник вокруг обнаруженной области
            cv2.rectangle(vis_image, (x1, y1), (x2, y2), (0, 255, 0), 3)
            
            # Добавляем текст с координатами
            cv2.putText(vis_image, f"Watermark: ({x1},{y1})-({x2},{y2})", 
                       (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
            
            # Сохраняем результат
            output_filename = "watermark_detection_visualization.jpg"
            cv2.imwrite(output_filename, vis_image)
            
            print(f"✅ Визуализация сохранена: {output_filename}")
            print(f"🔍 Откройте файл для проверки точности обнаружения")
            
        else:
            print("❌ Не удалось определить область для визуализации")
            
    except Exception as e:
        print(f"❌ Ошибка при визуализации: {e}")

if __name__ == "__main__":
    test_improved_detection()
    test_visualization()
