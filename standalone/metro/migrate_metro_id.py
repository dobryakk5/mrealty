#!/usr/bin/env python3
"""
Основной скрипт миграции для добавления и заполнения metro_id в таблице ads_cian
Использует существующую таблицу metro для сопоставления
"""

import asyncio
import asyncpg
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

async def migrate_metro_id():
    """Выполняет полную миграцию для добавления metro_id"""
    if not DATABASE_URL:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        return
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Подключение к БД установлено")
        
        # Шаг 1: Проверяем существование таблицы metro
        metro_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'metro'
            );
        """)
        
        if not metro_exists:
            print("❌ Таблица metro не найдена. Сначала создайте её через metro_mapping.py")
            return
        
        metro_count = await conn.fetchval("SELECT COUNT(*) FROM metro")
        print(f"✅ Таблица metro найдена, содержит {metro_count} станций")
        
        # Шаг 2: Проверяем существование колонки metro_id
        column_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'ads_cian' 
                AND column_name = 'metro_id'
            );
        """)
        
        if not column_exists:
            print("📝 Добавляем колонку metro_id...")
            await conn.execute("""
                ALTER TABLE ads_cian 
                ADD COLUMN metro_id INTEGER;
            """)
            print("✅ Колонка metro_id добавлена")
            
            # Создаем индекс
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ads_cian_metro_id ON ads_cian(metro_id);
            """)
            print("✅ Индекс для metro_id создан")
        else:
            print("✅ Колонка metro_id уже существует")
        
        # Шаг 3: Заполняем metro_id для существующих записей
        print("\n📝 Заполняем metro_id для существующих записей...")
        
        # Получаем все записи с метро, но без metro_id
        records = await conn.fetch("""
            SELECT id, metro, metro_id
            FROM ads_cian
            WHERE metro IS NOT NULL AND metro != '' AND metro_id IS NULL
            ORDER BY id
        """)
        
        print(f"📊 Найдено {len(records)} записей для обновления")
        
        if records:
            updated_count = 0
            not_found_count = 0
            
            for record in records:
                metro_name = record['metro']
                record_id = record['id']
                
                # Ищем metro_id по названию станции в таблице metro
                metro_id = await conn.fetchval("""
                    SELECT id FROM metro 
                    WHERE LOWER(name) = LOWER($1)
                    LIMIT 1
                """, metro_name)
                
                if metro_id:
                    # Обновляем запись
                    await conn.execute("""
                        UPDATE ads_cian 
                        SET metro_id = $1 
                        WHERE id = $2
                    """, metro_id, record_id)
                    updated_count += 1
                    
                    if updated_count % 10 == 0:  # Показываем прогресс каждые 10 записей
                        print(f"  📊 Обработано: {updated_count}/{len(records)}")
                else:
                    not_found_count += 1
                    print(f"  ❌ ID {record_id}: '{metro_name}' → metro_id не найден")
            
            print(f"\n📊 ИТОГИ ОБНОВЛЕНИЯ:")
            print(f"Обновлено: {updated_count}")
            print(f"Не найдено: {not_found_count}")
        else:
            print("✅ Все записи уже имеют metro_id")
        
        # Шаг 4: Показываем финальную статистику
        print("\n📊 ФИНАЛЬНАЯ СТАТИСТИКА:")
        print("-" * 50)
        
        # Общая статистика
        total_count = await conn.fetchval("SELECT COUNT(*) FROM ads_cian")
        metro_records = await conn.fetchval("SELECT COUNT(*) FROM ads_cian WHERE metro IS NOT NULL AND metro != ''")
        with_metro_id = await conn.fetchval("SELECT COUNT(*) FROM ads_cian WHERE metro_id IS NOT NULL")
        
        print(f"Всего объявлений: {total_count}")
        print(f"С указанием метро: {metro_records}")
        print(f"С metro_id: {with_metro_id}")
        print(f"Процент заполнения metro_id: {with_metro_id/metro_records*100:.1f}%" if metro_records > 0 else "0%")
        
        # Статистика по конкретным станциям
        top_stations = await conn.fetch("""
            SELECT m.name, COUNT(*) as count
            FROM ads_cian a
            JOIN metro m ON a.metro_id = m.id
            GROUP BY m.id, m.name
            ORDER BY count DESC
            LIMIT 10
        """)
        
        if top_stations:
            print(f"\n🏆 Топ-10 станций по количеству объявлений:")
            print("-" * 50)
            for i, station in enumerate(top_stations, 1):
                print(f"{i:2}. {station['name']:<25} - {station['count']:>4} объявлений")
        
        await conn.close()
        print("\n🔌 Соединение с БД закрыто")
        print("\n✅ Миграция завершена успешно!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == '__main__':
    asyncio.run(migrate_metro_id())
