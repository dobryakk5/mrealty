#!/usr/bin/env python3
"""
Тестовый скрипт для демонстрации работы модуля удаления водяных знаков ЦИАН
"""

import sys
import os

# Добавляем текущую директорию в путь для импорта
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from watermark_remover import watermark_remover
from photo_processor import photo_processor

def test_watermark_removal():
    """Тестирует удаление водяных знаков с тестовыми изображениями"""
    
    # Тестовые URL изображений с водяными знаками ЦИАН
    test_urls = [
        # Замените на реальные URL изображений с водяными знаками
        "https://images.cdn-cian.ru/images/2504097329-1.jpg",
        "https://images.cdn-cian.ru/images/2504097383-1.jpg"
    ]
    
    print("🧪 Тестирование модуля удаления водяных знаков ЦИАН")
    print("=" * 60)
    
    # Тест 1: Автоматическое определение области водяного знака
    print("\n1️⃣ Тест автоматического определения области водяного знака")
    print("-" * 50)
    
    for i, url in enumerate(test_urls):
        print(f"\n📸 Тестирую изображение {i+1}: {url}")
        
        try:
            # Скачиваем изображение
            import requests
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            image_data = response.content
            
            # Конвертируем в numpy array
            import cv2
            import numpy as np
            nparr = np.frombuffer(image_data, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if image is not None:
                # Определяем область водяного знака
                watermark_region = watermark_remover.detect_watermark_region(image)
                
                if watermark_region:
                    x1, y1, x2, y2 = watermark_region
                    print(f"✅ Область водяного знака определена: ({x1}, {y1}) - ({x2}, {y2})")
                    print(f"   Размер области: {x2-x1} x {y2-y1} пикселей")
                else:
                    print("❌ Не удалось определить область водяного знака")
            else:
                print("❌ Не удалось декодировать изображение")
                
        except Exception as e:
            print(f"❌ Ошибка при тестировании: {e}")
    
    # Тест 2: Удаление водяного знака разными методами
    print("\n\n2️⃣ Тест удаления водяного знака разными методами")
    print("-" * 50)
    
    methods = ['auto', 'inpainting', 'blur']
    
    for method in methods:
        print(f"\n🔧 Тестирую метод: {method}")
        print("-" * 30)
        
        for i, url in enumerate(test_urls):
            print(f"📸 Обрабатываю изображение {i+1} методом {method}")
            
            try:
                result = watermark_remover.process_image_from_url(url, method)
                
                if result:
                    print(f"✅ Успешно обработано методом {method}")
                    print(f"   Размер: {result['size']}")
                    print(f"   Формат: {result['format']}")
                    print(f"   Водяной знак удален: {result.get('watermark_removed', False)}")
                else:
                    print(f"❌ Не удалось обработать методом {method}")
                    
            except Exception as e:
                print(f"❌ Ошибка при обработке методом {method}: {e}")
    
    # Тест 3: Интеграция с photo_processor
    print("\n\n3️⃣ Тест интеграции с photo_processor")
    print("-" * 50)
    
    try:
        # Тестируем автоматическое удаление водяных знаков
        processed_photos = photo_processor.process_photos_for_embedded_html(
            test_urls, 
            remove_watermarks=False, 
            auto_remove_cian=True, 
            watermark_method='auto'
        )
        
        print(f"📊 Обработано фотографий: {len(processed_photos)}")
        
        for i, photo in enumerate(processed_photos):
            print(f"\n📸 Фото {i+1}:")
            print(f"   URL: {photo['url']}")
            print(f"   Обработано: {photo.get('processed', False)}")
            print(f"   Водяной знак удален: {photo.get('watermark_removed', False)}")
            print(f"   Метод: {photo.get('method', 'unknown')}")
            
    except Exception as e:
        print(f"❌ Ошибка при интеграции с photo_processor: {e}")

def test_with_local_image(image_path: str):
    """Тестирует удаление водяного знака с локального изображения"""
    
    if not os.path.exists(image_path):
        print(f"❌ Файл не найден: {image_path}")
        return
    
    print(f"\n🧪 Тестирование с локальным изображением: {image_path}")
    print("=" * 60)
    
    try:
        # Читаем локальное изображение
        import cv2
        image = cv2.imread(image_path)
        
        if image is None:
            print("❌ Не удалось прочитать изображение")
            return
        
        print(f"📸 Размер изображения: {image.shape[1]} x {image.shape[0]}")
        
        # Определяем область водяного знака
        watermark_region = watermark_remover.detect_watermark_region(image)
        
        if watermark_region:
            x1, y1, x2, y2 = watermark_region
            print(f"✅ Область водяного знака определена: ({x1}, {y1}) - ({x2}, {y2})")
            
            # Тестируем удаление водяного знака
            print("\n🔧 Тестирую удаление водяного знака...")
            
            # Метод inpainting
            result_inpainting = watermark_remover.remove_watermark_inpainting(image, watermark_region)
            if result_inpainting is not None:
                print("✅ Inpainting метод успешен")
                
                # Сохраняем результат
                output_path = image_path.replace('.', '_no_watermark_inpainting.')
                cv2.imwrite(output_path, result_inpainting)
                print(f"💾 Результат сохранен: {output_path}")
            
            # Метод blur + текст
            result_blur = watermark_remover.remove_watermark_blur(image, watermark_region)
            if result_blur is not None:
                print("✅ Blur + текст метод успешен")
                
                # Сохраняем результат
                output_path = image_path.replace('.', '_no_watermark_blur.')
                cv2.imwrite(output_path, result_blur)
                print(f"💾 Результат сохранен: {output_path}")
        else:
            print("❌ Не удалось определить область водяного знака")
            
    except Exception as e:
        print(f"❌ Ошибка при тестировании локального изображения: {e}")

if __name__ == "__main__":
    print("🚀 Запуск тестирования модуля удаления водяных знаков ЦИАН")
    
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1:
        # Если передан путь к изображению, тестируем с ним
        image_path = sys.argv[1]
        test_with_local_image(image_path)
    else:
        # Иначе запускаем общие тесты
        test_watermark_removal()
    
    print("\n✅ Тестирование завершено!")
