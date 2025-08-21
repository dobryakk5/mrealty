#!/usr/bin/env python3
"""
Тест для проверки исправления ошибки с BufferedInputFile
"""

import io
from aiogram.types import BufferedInputFile

def test_buffered_input_file():
    """Тестирует создание BufferedInputFile"""
    
    # Создаем тестовый контент
    test_content = "Тестовый HTML контент"
    test_file = io.BytesIO(test_content.encode('utf-8'))
    test_file.name = "test.html"
    
    try:
        # Создаем BufferedInputFile
        input_file = BufferedInputFile(test_file.getvalue(), filename="test.html")
        print("✅ BufferedInputFile успешно создан")
        print(f"📁 Имя файла: {input_file.filename}")
        print(f"📏 Размер: {len(test_content)} символов")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка создания BufferedInputFile: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Тестирую исправление ошибки с BufferedInputFile...")
    test_buffered_input_file()
