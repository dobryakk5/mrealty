#!/usr/bin/env python3
"""
Скрипт для сопоставления станций метро из ЦИАН с таблицей metro
и проставления cian_id для каждой найденной станции
"""

import asyncio
import csv
import re
import os
from typing import List, Dict, Optional, Tuple
import asyncpg
from dotenv import load_dotenv

# Load environment
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

# ========== НАСТРОЙКИ ==========
STATIONS_CSV_PATH = "stations.csv"  # Путь к файлу со станциями метро

# ========== УТИЛИТЫ ==========

def normalize_station_name(name: str) -> str:
    """Нормализует название станции для сравнения"""
    if not name:
        return ""
    
    # Приводим к нижнему регистру
    name = name.lower().strip()
    
    # Убираем лишние пробелы
    name = re.sub(r'\s+', ' ', name)
    
    # Убираем скобки и их содержимое (например, "Москва (город)")
    name = re.sub(r'\s*\([^)]*\)', '', name)
    
    # Убираем "метро" в конце
    name = re.sub(r'\s+метро$', '', name)
    
    # Убираем "станция" в конце
    name = re.sub(r'\s+станция$', '', name)
    
    return name.strip()



# ========== РАБОТА С БД ==========

async def create_metro_table(conn: asyncpg.Connection) -> None:
    """Создает таблицу metro если её нет"""
    try:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS metro (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            line_id SMALLINT NOT NULL,
            lat DOUBLE PRECISION NOT NULL,
            lon DOUBLE PRECISION NOT NULL,
            exits SMALLINT NOT NULL,
            cian_id SMALLINT,
            UNIQUE(name, line_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_metro_name ON metro(name);
        CREATE INDEX IF NOT EXISTS idx_metro_cian_id ON metro(cian_id);
        CREATE INDEX IF NOT EXISTS idx_metro_line_id ON metro(line_id);
        """)
        print("[DB] Таблица metro создана/проверена успешно")
    except Exception as e:
        print(f"[DB] Ошибка создания таблицы metro: {e}")
        raise

def load_stations_from_csv(csv_path: str) -> List[Dict]:
    """Загружает станции из CSV файла в память"""
    stations = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    line_id = int(row['line_id']) if row['line_id'].isdigit() else None
                    station_id = int(row['station_id']) if row['station_id'].isdigit() else None
                    
                    if line_id is not None and station_id is not None:
                        stations.append({
                            'line_id': line_id,
                            'line_name': row['line_name'],
                            'station_id': station_id,
                            'station_name': row['station_name']
                        })
                except Exception as e:
                    print(f"[WARN] Ошибка загрузки строки {row}: {e}")
                    continue
        
        print(f"[CSV] Загружено {len(stations)} станций из CSV")
        return stations
        
    except Exception as e:
        print(f"[CSV] Ошибка загрузки станций: {e}")
        raise

async def get_metro_stations(conn: asyncpg.Connection) -> List[Dict]:
    """Получает все станции метро из БД"""
    try:
        rows = await conn.fetch("""
        SELECT id, line_id, name, cian_id
        FROM metro
        ORDER BY line_id, id
        """)
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"[DB] Ошибка получения станций: {e}")
        raise

async def clear_existing_cian_ids(conn: asyncpg.Connection) -> int:
    """Очищает существующие cian_id для повторного сопоставления"""
    try:
        result = await conn.execute("UPDATE metro SET cian_id = NULL")
        cleared_count = await conn.fetchval("SELECT COUNT(*) FROM metro WHERE cian_id IS NULL")
        print(f"✅ Очищено {cleared_count} значений cian_id")
        return cleared_count
    except Exception as e:
        print(f"[DB] Ошибка очистки cian_id: {e}")
        return 0

async def update_metro_cian_id(conn: asyncpg.Connection, station_id: int, cian_id: int) -> bool:
    """Обновляет cian_id для станции метро"""
    try:
        result = await conn.execute("""
        UPDATE metro 
        SET cian_id = $1
        WHERE id = $2
        """, cian_id, station_id)
        return True
    except Exception as e:
        print(f"[DB] Ошибка обновления cian_id для станции {station_id}: {e}")
        return False

async def map_cian_to_metro(conn: asyncpg.Connection) -> Dict:
    """Основная функция сопоставления станций ЦИАН с таблицей metro"""
    print("\n=== СОПОСТАВЛЕНИЕ СТАНЦИЙ МЕТРО ===")
    
    # Загружаем станции из CSV
    csv_stations = load_stations_from_csv(STATIONS_CSV_PATH)
    print(f"Найдено {len(csv_stations)} станций в CSV файле")
    
    # Получаем станции из БД
    db_stations = await get_metro_stations(conn)
    print(f"Найдено {len(db_stations)} станций в таблице metro")
    
    # Статистика сопоставления
    exact_matches = 0
    partial_matches = 0
    no_matches = 0
    updated_count = 0
    
    print("\nСопоставление станций:")
    print("-" * 80)
    
    # Проходим по всем станциям из БД и ищем совпадения в CSV
    for db_station in db_stations:
        db_name = db_station['name']
        db_id = db_station['id']
        db_line_id = db_station['line_id']
        
        print(f"БД: '{db_name}' (ID: {db_id}, Линия: {db_line_id})")
        
        # Ищем совпадения в CSV
        potential_matches = []
        
        for csv_station in csv_stations:
            csv_name = csv_station['station_name']
            csv_station_id = csv_station['station_id']
            csv_line_id = csv_station['line_id']
            
            # Нормализуем названия для сравнения
            normalized_db = normalize_station_name(db_name)
            normalized_csv = normalize_station_name(csv_name)
            
            # Проверяем точное совпадение по названию
            if normalized_db == normalized_csv:
                potential_matches.append({
                    'csv_station': csv_station,
                    'confidence': 1.0,
                    'line_diff': abs(db_line_id - csv_line_id)
                })
        
        # Если найдены совпадения, выбираем лучшее
        if potential_matches:
            # Сортируем по уверенности и разнице в line_id
            potential_matches.sort(key=lambda x: (x['confidence'], x['line_diff']))
            
            best_match = potential_matches[0]
            csv_station = best_match['csv_station']
            csv_station_id = csv_station['station_id']
            csv_line_id = csv_station['line_id']
            line_diff = best_match['line_diff']
            
            # Проверяем, есть ли другие совпадения с близкими line_id
            close_matches = [m for m in potential_matches if m['line_diff'] <= 5]
            
            if len(close_matches) > 1 and line_diff <= 5:
                print(f"  ⚠️  ВНИМАНИЕ: Найдено {len(close_matches)} совпадений с близкими line_id:")
                for i, match in enumerate(close_matches[:3]):  # Показываем первые 3
                    m = match['csv_station']
                    print(f"    {i+1}. Линия {m['line_id']} ({m['line_name']}) - ID: {m['station_id']} (разница: {match['line_diff']})")
                
                # Если разница в line_id <= 5, спрашиваем пользователя
                if line_diff <= 5:
                    print(f"  ❓ Разница в line_id: {line_diff} (≤ 5). Требуется уточнение.")
                    print(f"  💡 Рекомендуется: Линия {csv_line_id} (разница: {line_diff})")
                    
                    # В автоматическом режиме выбираем с наименьшей разницей в line_id
                    print(f"  🤖 Автоматически выбрано: Линия {csv_line_id} (разница: {line_diff})")
                else:
                    print(f"  ✅ Автоматически выбрано: Линия {csv_line_id} (разница: {line_diff})")
            else:
                print(f"  ✅ Найдено точное совпадение: '{csv_station['station_name']}' (Линия: {csv_line_id}, CIAN ID: {csv_station_id})")
            
            exact_matches += 1
            
            # Обновляем cian_id в таблице metro
            if await update_metro_cian_id(conn, db_id, csv_station_id):
                updated_count += 1
                print(f"  ✓ Обновлен cian_id = {csv_station_id}")
            else:
                print(f"  ✗ Ошибка обновления cian_id")
        
        else:
            # Если точных совпадений не найдено, пробуем частичные
            partial_match = None
            best_confidence = 0.0
            
            for csv_station in csv_stations:
                csv_name = csv_station['station_name']
                csv_station_id = csv_station['station_id']
                csv_line_id = csv_station['line_id']
                
                normalized_db = normalize_station_name(db_name)
                normalized_csv = normalize_station_name(csv_name)
                
                # Проверяем частичное совпадение
                if normalized_csv in normalized_db or normalized_db in normalized_csv:
                    confidence = 0.8
                    if confidence > best_confidence:
                        partial_match = csv_station
                        best_confidence = confidence
                
                # Проверяем совпадение по словам
                db_words = set(normalized_db.split())
                csv_words = set(normalized_csv.split())
                
                if db_words and csv_words:
                    intersection = db_words.intersection(csv_words)
                    union = db_words.union(csv_words)
                    if union:
                        jaccard = len(intersection) / len(union)
                        if jaccard > 0.5 and jaccard > best_confidence:
                            partial_match = csv_station
                            best_confidence = jaccard
            
            if partial_match:
                csv_station_id = partial_match['station_id']
                csv_line_id = partial_match['line_id']
                line_diff = abs(db_line_id - csv_line_id)
                
                print(f"  ~ ЧАСТИЧНОЕ совпадение: '{partial_match['station_name']}' (Линия: {csv_line_id}, CIAN ID: {csv_station_id}, уверенность: {best_confidence:.2f})")
                print(f"    Разница в line_id: {line_diff}")
                
                partial_matches += 1
                
                # Обновляем cian_id в таблице metro
                if await update_metro_cian_id(conn, db_id, csv_station_id):
                    updated_count += 1
                    print(f"  ✓ Обновлен cian_id = {csv_station_id}")
                else:
                    print(f"  ✗ Ошибка обновления cian_id")
            else:
                print(f"  ✗ Совпадение НЕ НАЙДЕНО")
                no_matches += 1
        
        print()
    
    # Итоговая статистика
    print("=" * 80)
    print("ИТОГИ СОПОСТАВЛЕНИЯ:")
    print(f"Точных совпадений: {exact_matches}")
    print(f"Частичных совпадений: {partial_matches}")
    print(f"Без совпадений: {no_matches}")
    print(f"Обновлено записей в БД: {updated_count}")
    
    return {
        'exact_matches': exact_matches,
        'partial_matches': partial_matches,
        'no_matches': no_matches,
        'updated_count': updated_count
    }

async def show_mapping_results(conn: asyncpg.Connection) -> None:
    """Показывает результаты сопоставления"""
    print("\n=== РЕЗУЛЬТАТЫ СОПОСТАВЛЕНИЯ ===")
    
    try:
        # Станции с cian_id
        mapped_stations = await conn.fetch("""
        SELECT line_id, name, cian_id
        FROM metro
        WHERE cian_id IS NOT NULL
        ORDER BY line_id, name
        """)
        
        if mapped_stations:
            print(f"\nНайдено {len(mapped_stations)} сопоставленных станций:")
            print("-" * 60)
            for row in mapped_stations:
                print(f"Линия {row['line_id']} | {row['name']} | CIAN: {row['cian_id']}")
        else:
            print("Сопоставленных станций не найдено")
        
        # Станции без cian_id
        unmapped_stations = await conn.fetch("""
        SELECT line_id, name
        FROM metro
        WHERE cian_id IS NULL
        ORDER BY line_id, name
        LIMIT 20
        """)
        
        if unmapped_stations:
            print(f"\nПервые 20 несопоставленных станций:")
            print("-" * 60)
            for row in unmapped_stations:
                print(f"Линия {row['line_id']} | {row['name']}")
            
            if len(unmapped_stations) == 20:
                print("... (показаны только первые 20)")
        
        # Показываем статистику по уверенности сопоставлений
        confidence_stats = await conn.fetch("""
        SELECT 
            CASE 
                WHEN cian_id IS NOT NULL THEN 'Сопоставлено'
                ELSE 'Не сопоставлено'
            END as status,
            COUNT(*) as count
        FROM metro
        GROUP BY status
        ORDER BY status
        """)
        
        if confidence_stats:
            print(f"\nСтатистика сопоставления:")
            print("-" * 40)
            for row in confidence_stats:
                print(f"{row['status']}: {row['count']} станций")
        
    except Exception as e:
        print(f"[DB] Ошибка показа результатов: {e}")



async def export_mapping_results(conn: asyncpg.Connection, output_file: str = "metro_mapping_results.csv") -> None:
    """Экспортирует результаты сопоставления в CSV файл"""
    print(f"\n=== ЭКСПОРТ РЕЗУЛЬТАТОВ В {output_file} ===")
    
    try:
        # Получаем все станции с результатами сопоставления
        rows = await conn.fetch("""
        SELECT 
            m.line_id,
            m.name,
            m.cian_id,
            CASE 
                WHEN m.cian_id IS NOT NULL THEN 'Сопоставлено'
                ELSE 'Не сопоставлено'
            END as mapping_status
        FROM metro m
        ORDER BY m.line_id, m.id
        """)
        
        # Записываем в CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'line_id', 'name', 'cian_id', 'mapping_status'
            ])
            writer.writeheader()
            
            for row in rows:
                writer.writerow(dict(row))
        
        print(f"✅ Результаты экспортированы в {output_file}")
        print(f"Записано {len(rows)} записей")
        
    except Exception as e:
        print(f"❌ Ошибка экспорта: {e}")

# ========== ОСНОВНАЯ ФУНКЦИЯ ==========

async def main():
    """Основная функция"""
    print("СОПОСТАВЛЕНИЕ СТАНЦИЙ МЕТРО ЦИАН ↔ БД")
    print("=" * 50)
    
    if not DATABASE_URL:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        return
    
    if not os.path.exists(STATIONS_CSV_PATH):
        print(f"⚠️  Файл {STATIONS_CSV_PATH} не найден")
        print("Создаю тестовый файл stations.csv...")
        
        # Создаем тестовый файл с несколькими станциями
        test_stations = [
            "line_id,line_name,station_id,station_name",
            "4,Калининско-Солнцевская,1,Авиамоторная",
            "4,Калининско-Солнцевская,76,Новогиреево",
            "4,Калининско-Солнцевская,536,Пыхтино",
            "3,Замоскворецкая,2,Автозаводская",
            "3,Замоскворецкая,9,Аэропорт"
        ]
        
        with open(STATIONS_CSV_PATH, 'w', encoding='utf-8') as f:
            f.write('\n'.join(test_stations))
        
        print(f"✅ Создан тестовый файл {STATIONS_CSV_PATH} с {len(test_stations)-1} станциями")
        print("Для реальных данных запустите cian_metro.py")
    
    # Подключаемся к БД
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Подключение к БД установлено")
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return
    
    try:
        # Создаем таблицу metro если её нет
        await create_metro_table(conn)
        
        # Проверяем, есть ли уже данные в таблице
        count = await conn.fetchval("SELECT COUNT(*) FROM metro")
        if count == 0:
            print("❌ Таблица metro пуста. Сначала нужно загрузить данные.")
            return
        else:
            print(f"✅ В таблице metro уже есть {count} станций")
        
        # Очищаем существующие cian_id перед сопоставлением
        await clear_existing_cian_ids(conn)

        # Сопоставляем станции
        mapping_stats = await map_cian_to_metro(conn)
        
        # Показываем результаты
        await show_mapping_results(conn)
        
        # Экспортируем результаты в CSV
        await export_mapping_results(conn)
        
        print(f"\n✅ Сопоставление завершено!")
        print(f"Обработано станций: {mapping_stats['exact_matches'] + mapping_stats['partial_matches'] + mapping_stats['no_matches']}")
        
    except Exception as e:
        print(f"❌ Ошибка выполнения: {e}")
    finally:
        await conn.close()
        print("🔌 Соединение с БД закрыто")

if __name__ == '__main__':
    asyncio.run(main())
