#!/usr/bin/env python3
"""
Скрипт для проверки структуры таблицы metro в БД
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

async def check_metro_table():
    """Проверяет структуру таблицы metro в БД"""
    
    # Получаем DATABASE_URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        return False
    
    try:
        # Подключаемся к БД
        conn = await asyncpg.connect(database_url)
        print("✅ Подключение к БД установлено")
        
        # Проверяем текущий search_path
        search_path = await conn.fetchval("SHOW search_path")
        print(f"🔍 Текущий search_path: {search_path}")
        
        # Устанавливаем правильный search_path
        await conn.execute("SET search_path TO public, information_schema")
        print("✅ Установлен search_path: public, information_schema")
        
        # Проверяем существование таблицы
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'metro'
            );
        """)
        
        if not table_exists:
            print("❌ Таблица metro не существует")
            await conn.close()
            return False
        
        print("✅ Таблица metro существует")
        
        # Получаем структуру таблицы
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'metro'
            ORDER BY ordinal_position;
        """)
        
        print("\n📋 Структура таблицы metro:")
        print("=" * 60)
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            default = f" DEFAULT {col['column_default']}" if col['column_default'] else ""
            print(f"• {col['column_name']:<15} {col['data_type']:<20} {nullable}{default}")
        
        # Проверяем количество записей
        try:
            count = await conn.fetchval("SELECT COUNT(*) FROM metro")
            print(f"\n📊 Количество записей в таблице: {count}")
            
            if count > 0:
                # Показываем несколько примеров записей
                print("\n📝 Примеры записей:")
                print("=" * 60)
                rows = await conn.fetch("SELECT * FROM metro LIMIT 5")
                for row in rows:
                    print(f"• {dict(row)}")
        except Exception as e:
            print(f"⚠️ Не удалось получить количество записей: {e}")
            print("🔍 Проблема с доступом к таблице metro")
            
            # Пробуем определить схему таблицы
            try:
                schema = await conn.fetchval("""
                    SELECT table_schema 
                    FROM information_schema.tables 
                    WHERE table_name = 'metro'
                """)
                print(f"🔍 Таблица metro находится в схеме: {schema}")
                
                if schema != 'public':
                    print(f"💡 Попробуйте использовать полное имя: {schema}.metro")
            except Exception as schema_e:
                print(f"⚠️ Не удалось определить схему: {schema_e}")
        
        # Проверяем наличие необходимых полей для парсинга Авито
        required_fields = ['id', 'name', 'avito_id', 'is_msk']
        existing_fields = [col['column_name'] for col in columns]
        
        print(f"\n🔍 Проверка необходимых полей для парсинга Авито:")
        print("=" * 60)
        for field in required_fields:
            if field in existing_fields:
                print(f"✅ {field}")
            else:
                print(f"❌ {field} - ОТСУТСТВУЕТ")
        
        await conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Ошибка проверки таблицы metro: {e}")
        return False

if __name__ == "__main__":
    try:
        success = asyncio.run(check_metro_table())
        if not success:
            print("❌ Не удалось проверить таблицу metro")
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
