#!/usr/bin/env python3
"""
Тест исправленного алгоритма обнаружения водяных знаков ЦИАН
Только для одного изображения
"""

from watermark_remover import watermark_remover
import cv2
import numpy as np
import requests
import os

def test_single_image():
    """Тестирует исправленный алгоритм на одном изображении"""
    
    print("🧪 Тест исправленного алгоритма обнаружения водяных знаков ЦИАН")
    print("=" * 70)
    
    # Только одно изображение
    url = "https://images.cdn-cian.ru/images/2504097329-1.jpg"
    
    print(f"📸 Тестирую изображение: {url}")
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
            return
        
        height, width = image.shape[:2]
        print(f"📏 Размер изображения: {width} x {height}")
        
        # Тестируем исправленный алгоритм
        print("\n🔍 Тестирую исправленный алгоритм обнаружения...")
        watermark_region = watermark_remover.detect_cian_watermark_region(image)
        
        if watermark_region:
            x1, y1, x2, y2 = watermark_region
            region_width = x2 - x1
            region_height = y2 - y1
            
            print(f"\n✅ Область водяного знака определена:")
            print(f"   Координаты: ({x1}, {y1}) - ({x2}, {y2})")
            print(f"   Размер области: {region_width} x {region_height} пикселей")
            
            # Проверяем позиционирование
            right_edge = width * 0.9  # 90% от ширины
            bottom_edge = height * 0.9  # 90% от высоты
            
            if x1 > right_edge and y1 > bottom_edge:
                print("✅ Область корректно расположена в правом нижнем углу")
            else:
                print("⚠️ Область может быть расположена не в том месте")
            
            # Создаем визуализацию
            vis_image = image.copy()
            cv2.rectangle(vis_image, (x1, y1), (x2, y2), (0, 255, 0), 3)
            cv2.putText(vis_image, f"Watermark: ({x1},{y1})-({x2},{y2})", 
                       (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
            
            # Сохраняем визуализацию
            vis_filename = "corrected_watermark_detection.jpg"
            cv2.imwrite(vis_filename, vis_image)
            print(f"💾 Визуализация области сохранена: {vis_filename}")
            
            # Тестируем удаление водяного знака
            print(f"\n🔧 Тестирую удаление водяного знака...")
            
            # Метод blur
            result_blur = watermark_remover.remove_watermark_blur(image, watermark_region)
            if result_blur is not None:
                print("✅ Blur метод успешен")
                
                # Сохраняем результат
                output_filename = "corrected_result_blur.jpg"
                cv2.imwrite(output_filename, result_blur)
                print(f"💾 Результат blur сохранен: {output_filename}")
            
            # Метод inpainting
            result_inpainting = watermark_remover.remove_watermark_inpainting(image, watermark_region)
            if result_inpainting is not None:
                print("✅ Inpainting метод успешен")
                
                # Сохраняем результат
                output_filename = "corrected_result_inpainting.jpg"
                cv2.imwrite(output_filename, result_inpainting)
                print(f"💾 Результат inpainting сохранен: {output_filename}")
            
        else:
            print("❌ Не удалось определить область водяного знака")
            
    except Exception as e:
        print(f"❌ Ошибка при тестировании: {e}")

if __name__ == "__main__":
    test_single_image()
    print("\n✅ Тестирование завершено!")
    print("🔍 Проверьте созданные файлы для сравнения")
