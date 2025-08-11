#!/usr/bin/env python3
"""
Скрипт для заполнения metro_id в существующих записях таблицы ads_cian
на основе названий станций метро
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv
from typing import Optional

# Загружаем переменные окружения
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

async def fill_metro_ids():
    """Заполняет metro_id для существующих записей в ads_cian"""
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
        
        if not column_exists:
            print("❌ Колонка metro_id не существует. Сначала запустите add_metro_id_column.py")
            return
        
        # Получаем все записи с метро, но без metro_id
        records = await conn.fetch("""
            SELECT id, metro, metro_id
            FROM ads_cian
            WHERE metro IS NOT NULL AND metro != ''
            ORDER BY id
            LIMIT 100
        """)
        
        print(f"📊 Найдено {len(records)} записей с метро для обработки")
        
        if not records:
            print("✅ Все записи уже имеют metro_id")
            return
        
        # Обрабатываем каждую запись
        updated_count = 0
        not_found_count = 0
        
        for record in records:
            metro_name = record['metro']
            record_id = record['id']
            
            # Ищем metro_id по названию станции
            metro_id = await find_metro_id_by_name(conn, metro_name)
            
            if metro_id:
                # Обновляем запись
                await conn.execute("""
                    UPDATE ads_cian 
                    SET metro_id = $1 
                    WHERE id = $2
                """, metro_id, record_id)
                updated_count += 1
                print(f"  ✅ ID {record_id}: '{metro_name}' → metro_id = {metro_id}")
            else:
                not_found_count += 1
                print(f"  ❌ ID {record_id}: '{metro_name}' → metro_id не найден")
        
        print(f"\n📊 ИТОГИ ОБНОВЛЕНИЯ:")
        print(f"Обновлено: {updated_count}")
        print(f"Не найдено: {not_found_count}")
        
        # Показываем статистику
        stats = await conn.fetch("""
            SELECT 
                CASE 
                    WHEN metro_id IS NULL THEN 'Без metro_id'
                    ELSE 'С metro_id'
                END as status,
                COUNT(*) as count
            FROM ads_cian
            WHERE metro IS NOT NULL AND metro != ''
            GROUP BY status
            ORDER BY status
        """)
        
        print(f"\n📊 Статистика по metro_id:")
        print("-" * 40)
        for row in stats:
            print(f"{row['status']}: {row['count']} объявлений")
        
        await conn.close()
        print("\n🔌 Соединение с БД закрыто")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

async def find_metro_id_by_name(conn: asyncpg.Connection, metro_name: str) -> Optional[int]:
    """Находит ID станции метро по названию из таблицы metro"""
    if not metro_name:
        return None
    
    try:
        # Нормализуем название станции для поиска
        normalized_name = metro_name.lower().strip()
        
        # Убираем "м." в начале
        if normalized_name.startswith('м.'):
            normalized_name = normalized_name[2:].strip()
        
        # Ищем точное совпадение
        metro_id = await conn.fetchval("""
            SELECT id FROM metro 
            WHERE LOWER(name) = $1
            LIMIT 1
        """, normalized_name)
        
        if metro_id:
            return metro_id
        
        # Если точное совпадение не найдено, ищем частичное
        metro_id = await conn.fetchval("""
            SELECT id FROM metro 
            WHERE LOWER(name) LIKE $1
            LIMIT 1
        """, f"%{normalized_name}%")
        
        return metro_id
        
    except Exception as e:
        print(f"[DB] Ошибка поиска metro_id для '{metro_name}': {e}")
        return None

if __name__ == '__main__':
    asyncio.run(fill_metro_ids())
