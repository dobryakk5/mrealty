#!/usr/bin/env python3
"""
Скрипт для проверки структуры таблицы metro
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

async def check_metro_structure():
    """Проверяет структуру таблицы metro"""
    if not DATABASE_URL:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        return
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Подключение к БД установлено")
        
        # Проверяем существование таблицы
        table_exists = await conn.fetchval("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'metro'
        );
        """)
        
        if not table_exists:
            print("❌ Таблица metro не найдена")
            return
        
        print("✅ Таблица metro найдена")
        
        # Получаем структуру таблицы
        columns = await conn.fetch("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = 'metro'
        ORDER BY ordinal_position
        """)
        
        print("\n📋 Структура таблицы metro:")
        print("-" * 60)
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
            print(f"{col['column_name']:<20} {col['data_type']:<15} {nullable:<10}{default}")
        
        # Получаем количество записей
        count = await conn.fetchval("SELECT COUNT(*) FROM metro")
        print(f"\n📊 Количество записей: {count}")
        
        # Показываем несколько примеров записей
        if count > 0:
            print("\n📝 Примеры записей:")
            print("-" * 60)
            rows = await conn.fetch("SELECT * FROM metro LIMIT 3")
            for i, row in enumerate(rows, 1):
                print(f"Запись {i}: {dict(row)}")
        
        await conn.close()
        print("\n🔌 Соединение с БД закрыто")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == '__main__':
    asyncio.run(check_metro_structure())
