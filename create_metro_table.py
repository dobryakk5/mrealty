#!/usr/bin/env python3
"""
Скрипт для создания таблицы metro с необходимыми полями для парсинга Авито
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

async def create_metro_table():
    """Создает таблицу metro с необходимыми полями"""
    
    # Получаем DATABASE_URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        print("💡 Создайте файл .env с содержимым:")
        print("   DATABASE_URL=postgresql://username:password@host:port/database")
        return False
    
    try:
        # Подключаемся к БД
        conn = await asyncpg.connect(database_url)
        print("✅ Подключение к БД установлено")
        
        # Создаем таблицу metro
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS metro (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            line_id SMALLINT NOT NULL,
            lat DOUBLE PRECISION NOT NULL,
            lon DOUBLE PRECISION NOT NULL,
            exits SMALLINT NOT NULL,
            cian_id SMALLINT,
            avito_id INTEGER,  -- ID метро для Авито
            is_msk BOOLEAN DEFAULT TRUE,  -- Флаг московского метро
            UNIQUE(name, line_id)
        );
        """)
        
        # Создаем индексы
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_metro_name ON metro(name)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_metro_cian_id ON metro(cian_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_metro_line_id ON metro(line_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_metro_avito_id ON metro(avito_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_metro_is_msk ON metro(is_msk)")
        
        print("✅ Таблица metro создана/обновлена успешно")
        
        # Проверяем, есть ли данные в таблице
        count = await conn.fetchval("SELECT COUNT(*) FROM metro")
        print(f"📊 В таблице metro: {count} записей")
        
        if count == 0:
            print("⚠️ Таблица пустая. Нужно заполнить данными о станциях метро.")
            print("💡 Запустите скрипт metro_mapping.py для заполнения данными")
        
        await conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка создания таблицы metro: {e}")
        return False

if __name__ == "__main__":
    try:
        success = asyncio.run(create_metro_table())
        if success:
            print("✅ Таблица metro готова к использованию")
        else:
            print("❌ Не удалось создать таблицу metro")
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
