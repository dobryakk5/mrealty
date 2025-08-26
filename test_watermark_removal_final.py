#!/usr/bin/env python3
"""
Финальный тест модуля удаления водяных знаков ЦИАН с сохранением результатов
"""

import os
from watermark_remover import watermark_remover

def test_watermark_removal_with_save():
    """Тестирует удаление водяных знаков и сохраняет результаты"""
    
    print("🧪 Финальный тест удаления водяных знаков ЦИАН")
    print("=" * 60)
    
    # Тестовые URL изображений
    test_urls = [
        "https://images.cdn-cian.ru/images/2504097329-1.jpg",
        "https://images.cdn-cian.ru/images/2504097383-1.jpg"
    ]
    
    # Создаем папку для результатов
    output_dir = "final_watermark_removal_results"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"📁 Создана папка: {output_dir}")
    
    print(f"📁 Результаты будут сохранены в папку: {output_dir}")
    
    for i, url in enumerate(test_urls):
        print(f"\n📸 Обрабатываю изображение {i+1}: {url}")
        print("-" * 50)
        
        try:
            # Скачиваем исходное изображение
            import requests
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            image_data = response.content
            
            # Сохраняем исходное изображение
            original_filename = f"{output_dir}/original_{i+1}.jpg"
            with open(original_filename, 'wb') as f:
                f.write(image_data)
            print(f"💾 Исходное изображение сохранено: {original_filename}")
            
            # Тестируем оба метода
            methods = ['inpainting', 'blur']
            
            for method in methods:
                print(f"🔧 Тестирую метод: {method}")
                
                try:
                    result = watermark_remover.process_image_from_url(url, method)
                    
                    if result:
                        # Сохраняем результат
                        output_filename = f"{output_dir}/result_{i+1}_{method}.jpg"
                        
                        # Декодируем base64 и сохраняем
                        import base64
                        img_data = base64.b64decode(result['base64'])
                        
                        with open(output_filename, 'wb') as f:
                            f.write(img_data)
                        
                        print(f"✅ Результат сохранен: {output_filename}")
                        print(f"   Размер: {result['size']}")
                        print(f"   Метод: {result.get('method', 'unknown')}")
                        print(f"   Водяной знак удален: {result.get('watermark_removed', False)}")
                    else:
                        print(f"❌ Не удалось обработать методом {method}")
                        
                except Exception as e:
                    print(f"❌ Ошибка при обработке методом {method}: {e}")
                    
        except Exception as e:
            print(f"❌ Ошибка при обработке изображения {i+1}: {e}")
    
    print(f"\n📊 Все результаты сохранены в папке: {output_dir}")
    print("🔍 Откройте папку и сравните исходные и обработанные изображения")
    print("\n📋 Файлы для сравнения:")
    
    # Показываем список созданных файлов
    for filename in sorted(os.listdir(output_dir)):
        file_path = os.path.join(output_dir, filename)
        file_size = os.path.getsize(file_path)
        print(f"   📄 {filename} ({file_size:,} байт)")

def test_single_image(url: str):
    """Тестирует одно изображение с подробным анализом"""
    
    print(f"🔍 Детальный анализ изображения: {url}")
    print("=" * 60)
    
    try:
        # Скачиваем изображение
        import requests
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        image_data = response.content
        
        # Конвертируем в numpy array для анализа
        import cv2
        import numpy as np
        nparr = np.frombuffer(image_data, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if image is None:
            print("❌ Не удалось декодировать изображение")
            return
        
        height, width = image.shape[:2]
        print(f"📏 Размер изображения: {width} x {height}")
        
        # Определяем область водяного знака
        watermark_region = watermark_remover.detect_cian_watermark_region(image)
        
        if watermark_region:
            x1, y1, x2, y2 = watermark_region
            region_width = x2 - x1
            region_height = y2 - y1
            
            print(f"\n🎯 Область водяного знака:")
            print(f"   Координаты: ({x1}, {y1}) - ({x2}, {y2})")
            print(f"   Размер: {region_width} x {region_height} пикселей")
            print(f"   Позиция: правая нижняя часть")
            
            # Проверяем позиционирование
            right_quarter = width * 0.75
            bottom_quarter = height * 0.75
            
            if x1 > right_quarter and y1 > bottom_quarter:
                print("✅ Область корректно расположена в правой нижней части")
            else:
                print("⚠️ Область может быть расположена не в том месте")
            
            # Создаем визуализацию
            vis_image = image.copy()
            cv2.rectangle(vis_image, (x1, y1), (x2, y2), (0, 255, 0), 3)
            cv2.putText(vis_image, f"Watermark: ({x1},{y1})-({x2},{y2})", 
                       (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)
            
            # Сохраняем визуализацию
            vis_filename = "watermark_region_visualization.jpg"
            cv2.imwrite(vis_filename, vis_image)
            print(f"💾 Визуализация области сохранена: {vis_filename}")
            
        else:
            print("❌ Не удалось определить область водяного знака")
            
    except Exception as e:
        print(f"❌ Ошибка при анализе: {e}")

if __name__ == "__main__":
    print("🚀 Финальный тест модуля удаления водяных знаков ЦИАН")
    
    # Запускаем основной тест
    test_watermark_removal_with_save()
    
    # Дополнительно тестируем первое изображение детально
    print("\n" + "=" * 60)
    test_single_image("https://images.cdn-cian.ru/images/2504097329-1.jpg")
    
    print("\n✅ Все тесты завершены!")
    print("🔍 Проверьте папку 'final_watermark_removal_results' для сравнения результатов")
