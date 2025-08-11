#!/usr/bin/env python3
"""
Скрипт для добавления колонки metro_id в существующую таблицу ads_cian
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

async def add_metro_id_column():
    """Добавляет колонку metro_id в таблицу ads_cian"""
    if not DATABASE_URL:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        return
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Подключение к БД установлено")
        
        # Проверяем, существует ли колонка metro_id
        column_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'ads_cian' 
                AND column_name = 'metro_id'
            );
        """)
        
        if column_exists:
            print("✅ Колонка metro_id уже существует")
        else:
            # Добавляем колонку metro_id
            await conn.execute("""
                ALTER TABLE ads_cian 
                ADD COLUMN metro_id INTEGER;
            """)
            print("✅ Колонка metro_id добавлена")
            
            # Создаем индекс для metro_id
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ads_cian_metro_id ON ads_cian(metro_id);
            """)
            print("✅ Индекс для metro_id создан")
        
        # Показываем текущую структуру таблицы
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'ads_cian'
            ORDER BY ordinal_position
        """)
        
        print("\n📋 Текущая структура таблицы ads_cian:")
        print("-" * 60)
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"{col['column_name']:<20} {col['data_type']:<15} {nullable}")
        
        # Показываем статистику по metro_id
        if column_exists:
            metro_stats = await conn.fetch("""
                SELECT 
                    CASE 
                        WHEN metro_id IS NULL THEN 'Без metro_id'
                        ELSE 'С metro_id'
                    END as status,
                    COUNT(*) as count
                FROM ads_cian
                GROUP BY status
                ORDER BY status
            """)
            
            print(f"\n📊 Статистика по metro_id:")
            print("-" * 40)
            for row in metro_stats:
                print(f"{row['status']}: {row['count']} объявлений")
        
        await conn.close()
        print("\n🔌 Соединение с БД закрыто")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == '__main__':
    asyncio.run(add_metro_id_column())
