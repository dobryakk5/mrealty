#!/usr/bin/env python3
"""
Обработка всех групп метро с обновленными списками
Использует паттерн "жилье у метро [Название]" для поиска
"""

import json
import os
import time
import asyncio
import asyncpg
import re
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

class CompleteGroupsUpdater:
    def __init__(self):
        self.cookies_file = "avito_cookies_fixed.json"
        self.driver = None
        self.database_url = None
        self.results = []
        
        # Обновленные группы с актуальными списками из URL-ов AVITO
        self.target_groups = {
            "Группа 1": [1, 2, 3, 4, 5, 54, 55, 56, 57, 58, 59, 93, 94, 95, 96, 97, 98, 99, 148, 151, 2135, 2136, 2142, 2174, 2187, 2213, 2227, 2238, 2250, 2257, 2271, 2288, 2315, 2319, 2330, 2336],
            "Группа 2": [6, 7, 8, 9, 10, 11, 12, 60, 100, 101, 102, 103, 104, 2144, 2149, 2150, 2158, 2159, 2165, 2177, 2179, 2180, 2186, 2204, 2209, 2222, 2235, 2239, 2240, 2254, 2299, 2311, 2318],
            "Группа 3": [13, 14, 15, 16, 18, 61, 62, 63, 64, 65, 105, 106, 107, 108, 215, 2002, 2145, 2150, 2191, 2207, 2246, 2247, 2251, 2268, 2275, 2279, 2299, 2303, 2316, 2321, 2323, 2333],
            "Группа 4": [19, 20, 66, 109, 110, 111, 127, 149, 1001, 1004, 1005, 1010, 1012, 2147, 2155, 2181, 2185, 2201, 2202, 2205, 2224, 2253, 2276, 2286, 2292, 2293, 2305, 2325, 2326, 2328],
            "Группа 5": [21, 22, 23, 24, 25, 67, 68, 69, 112, 113, 114, 115, 120, 214, 1002, 1007, 2160, 2163, 2168, 2190, 2221, 2223, 2234, 2243, 2266, 2272, 2283, 2290, 2296, 2329],
            "Группа 6": [26, 27, 28, 29, 70, 71, 72, 116, 117, 118, 119, 121, 122, 1003, 2133, 2152, 2173, 2200, 2208, 2225, 2226, 2228, 2229, 2245, 2249, 2252, 2256, 2264, 2289, 2301, 2317, 2331, 2339],
            "Группа 7": [17, 30, 31, 32, 33, 34, 73, 74, 123, 124, 125, 126, 128, 152, 1006, 2001, 2148, 2157, 2171, 2172, 2199, 2203, 2215, 2244, 2248, 2255, 2262, 2269, 2285, 2304, 2309, 2337, 2340],
            "Группа 8": [35, 75, 76, 77, 129, 130, 217, 1008, 1009, 1011, 2143, 2169, 2176, 2184, 2210, 2214, 2220, 2230, 2237, 2258, 2278, 2281, 2291, 2298, 2306, 2307, 2310, 2313, 2314, 2338],
            "Группа 9": [36, 37, 38, 39, 40, 41, 42, 43, 78, 79, 80, 81, 82, 131, 132, 133, 2154, 2161, 2183, 2188, 2193, 2212, 2233, 2242, 2260, 2261, 2267, 2282, 2287, 2300, 2312, 2324, 2332],
            "Группа 10": [44, 45, 46, 83, 84, 85, 86, 87, 88, 133, 134, 135, 136, 137, 138, 139, 140, 2151, 2162, 2166, 2182, 2194, 2195, 2241, 2261, 2270, 2297, 2302, 2320, 2327, 2334, 2343, 2344],
            "Группа 11": [47, 48, 49, 50, 51, 52, 53, 89, 90, 91, 92, 141, 142, 143, 144, 145, 146, 147, 216, 2219, 2236, 2259, 2263, 2265, 2273, 2274, 2277, 2280, 2284, 2295, 2308, 2322, 2335, 2167, 2342]
        }
        
        # Собираем все целевые avito_id
        self.all_target_ids = []
        for group_name, ids in self.target_groups.items():
            self.all_target_ids.extend(ids)
        
        print(f"🎯 Обработка всех групп метро с обновленными списками")
        print(f"📊 Всего целевых avito_id: {len(self.all_target_ids)}")
        print(f"📋 Группы: {', '.join(self.target_groups.keys())}")
    
    def load_cookies(self):
        """Загружает зафиксированные cookies"""
        try:
            if not os.path.exists(self.cookies_file):
                print(f"❌ Файл cookies не найден: {self.cookies_file}")
                return False
            
            with open(self.cookies_file, 'r', encoding='utf-8') as f:
                cookies_data = json.load(f)
            
            print(f"✅ Загружены cookies от {cookies_data['timestamp']}")
            print(f"📊 Количество cookies: {len(cookies_data['cookies'])}")
            return cookies_data
            
        except Exception as e:
            print(f"❌ Ошибка загрузки cookies: {e}")
            return False
    
    def setup_selenium(self):
        """Настраивает Selenium WebDriver"""
        try:
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
                self.driver = None
            
            options = Options()
            
            # Базовые настройки
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-plugins")
            options.add_argument("--headless")  # Headless для массовой обработки
            
            # User-Agent
            options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            
            # Размер окна
            options.add_argument("--window-size=1920,1080")
            
            # Дополнительные настройки для обхода блокировок
            options.add_argument("--disable-web-security")
            options.add_argument("--allow-running-insecure-content")
            options.add_argument("--disable-features=VizDisplayCompositor")
            
            # Убираем признаки автоматизации
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            self.driver = webdriver.Chrome(options=options)
            
            # Убираем признаки автоматизации
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            print("✅ Selenium WebDriver настроен")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка настройки Selenium: {e}")
            return False
    
    def apply_cookies(self, cookies_data):
        """Применяет cookies к WebDriver"""
        try:
            # Сначала переходим на домен
            self.driver.get("https://www.avito.ru")
            time.sleep(2)
            
            # Применяем cookies
            for cookie in cookies_data['cookies']:
                try:
                    # Убираем проблемные поля
                    if 'expiry' in cookie:
                        del cookie['expiry']
                    if 'sameSite' in cookie:
                        del cookie['sameSite']
                    
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    print(f"   ⚠️ Не удалось применить cookie: {e}")
            
            print("✅ Cookies применены")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка применения cookies: {e}")
            return False
    
    def detect_metro_station(self, avito_id):
        """Определяет название метро по avito_id"""
        try:
            # Формируем URL для поиска
            base_url = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
            target_url = f"{base_url}?metro={avito_id}"
            
            print(f"   📱 Переходим на: {target_url}")
            
            # Переходим на страницу
            self.driver.get(target_url)
            time.sleep(5)
            
            # Получаем заголовок и HTML
            page_title = self.driver.title
            page_source = self.driver.page_source
            
            print(f"   📄 Заголовок: {page_title}")
            
            # Извлекаем название метро
            metro_name = self.extract_metro_from_title(page_title, page_source)
            
            if metro_name:
                print(f"   ✅ Найдено метро: {metro_name}")
                return metro_name
            else:
                print(f"   ❌ Метро не найдено в заголовке")
                return None
                
        except Exception as e:
            print(f"   ❌ Ошибка определения метро: {e}")
            return None
    
    def extract_metro_from_title(self, page_title, page_source):
        """Извлекает название метро из заголовка страницы по паттерну 'жилье у метро [Название]'"""
        try:
            # Паттерн 1: "жилье у метро [Название станции]"
            metro_pattern1 = r'жилье\s+у\s+метро\s+([А-Яа-яЁё\s\-]+?)(?:\s*[:\|]|\s*-\s*Авито|\s*$)'
            match1 = re.search(metro_pattern1, page_title, re.IGNORECASE)
            
            # Паттерн 2: "у метро [Название станции]" - более гибкий
            metro_pattern2 = r'у\s+метро\s+([А-Яа-яЁё\s\-]+?)(?:\s*[:\|]|\s*-\s*Авито|\s*$)'
            match2 = re.search(metro_pattern2, page_title, re.IGNORECASE)
            
            # Паттерн 3: "метро [Название станции]"
            metro_pattern3 = r'метро\s+([А-Яа-яЁё\s\-]+?)(?:\s*[:\|]|\s*-\s*Авито|\s*$)'
            match3 = re.search(metro_pattern3, page_title, re.IGNORECASE)
            
            # Паттерн 4: "Купить квартиру у метро [Название]"
            metro_pattern4 = r'Купить\s+квартиру\s+у\s+метро\s+([А-Яа-яЁё\s\-]+?)(?:\s*[:\|]|\s*-\s*Авито|\s*$)'
            match4 = re.search(metro_pattern4, page_title, re.IGNORECASE)
            
            # Паттерн 5: поиск в HTML-коде страницы
            metro_pattern5 = r'у\s+метро\s+([А-Яа-яЁё\s\-]+?)(?:\s*[:\|]|\s*-\s*Авито|\s*$)'
            match5 = re.search(metro_pattern5, page_source, re.IGNORECASE)
            
            metro_name = None
            if match1:
                metro_name = match1.group(1).strip()
                print(f"   ✅ Найдено паттерном 1: {metro_name}")
            elif match2:
                metro_name = match2.group(1).strip()
                print(f"   ✅ Найдено паттерном 2: {metro_name}")
            elif match3:
                metro_name = match3.group(1).strip()
                print(f"   ✅ Найдено паттерном 3: {metro_name}")
            elif match4:
                metro_name = match4.group(1).strip()
                print(f"   ✅ Найдено паттерном 4: {metro_name}")
            elif match5:
                metro_name = match5.group(1).strip()
                print(f"   ✅ Найдено паттерном 5: {metro_name}")
            
            if not metro_name:
                return None
            
            # Дополнительная очистка названия
            # Убираем лишние пробелы
            metro_name = re.sub(r'\s+', ' ', metro_name).strip()
            
            # Исключаем случаи типа "улица 1905 года"
            if re.search(r'улица.*\d+$', metro_name, re.IGNORECASE):
                return None
            
            # Убираем цифры в конце названия (если они есть)
            metro_name = re.sub(r'\d+$', '', metro_name).strip()
            
            # Убираем символы, которые могут быть в конце
            metro_name = re.sub(r'[,\-\.\s]+$', '', metro_name)
            
            # Проверяем, что название не пустое и не слишком короткое
            if len(metro_name) < 2:
                return None
            
            return metro_name
            
        except Exception as e:
            print(f"   ❌ Ошибка извлечения метро из заголовка: {e}")
            return None
    
    async def find_matching_station(self, metro_name):
        """Ищет соответствующую станцию в БД"""
        try:
            conn = await asyncpg.connect(self.database_url)
            
            # Ищем станцию по названию (точное совпадение)
            result = await conn.fetchrow("""
                SELECT id, name, line_id 
                FROM metro 
                WHERE name ILIKE $1 AND is_msk IS NOT FALSE
                ORDER BY id
                LIMIT 1
            """, metro_name)
            
            if result:
                print(f"   🎯 Найдена станция в БД: {result['name']} (ID: {result['id']})")
                await conn.close()
                return result
            
            # Если точное совпадение не найдено, ищем частичное
            result = await conn.fetchrow("""
                SELECT id, name, line_id 
                FROM metro 
                WHERE name ILIKE $1 AND is_msk IS NOT FALSE
                ORDER BY id
                LIMIT 1
            """, f"%{metro_name}%")
            
            if result:
                print(f"   🎯 Найдена станция в БД (частичное): {result['name']} (ID: {result['id']})")
                await conn.close()
                return result
            
            await conn.close()
            print(f"   ❌ Станция '{metro_name}' не найдена в БД")
            return None
            
        except Exception as e:
            print(f"   ❌ Ошибка поиска станции {metro_name}: {e}")
            return None
    
    async def get_existing_avito_ids(self):
        """Получает существующие avito_id из БД"""
        try:
            conn = await asyncpg.connect(self.database_url)
            
            # Получаем все существующие avito_id
            result = await conn.fetch("""
                SELECT avito_id 
                FROM metro 
                WHERE avito_id IS NOT NULL
                ORDER BY avito_id
            """)
            
            await conn.close()
            
            existing_ids = [row['avito_id'] for row in result]
            return set(existing_ids)
            
        except Exception as e:
            print(f"❌ Ошибка получения существующих avito_id: {e}")
            return set()
    
    async def update_metro_avito_id(self, metro_id, avito_id):
        """Обновляет avito_id для станции метро"""
        try:
            conn = await asyncpg.connect(self.database_url)
            
            # Получаем текущий avito_id перед обновлением
            current = await conn.fetchrow("""
                SELECT avito_id FROM metro WHERE id = $1
            """, metro_id)
            
            old_avito_id = current['avito_id'] if current else None
            
            # Обновляем avito_id
            result = await conn.execute("""
                UPDATE metro 
                SET avito_id = $1 
                WHERE id = $2
            """, avito_id, metro_id)
            
            await conn.close()
            
            print(f"   🔄 Обновлено: {old_avito_id} → {avito_id}")
            return True, old_avito_id
            
        except Exception as e:
            print(f"   ❌ Ошибка обновления avito_id для станции {metro_id}: {e}")
            return False, None
    
    async def process_target_groups(self):
        """Обрабатывает все целевые группы"""
        try:
            print(f"🎯 Обработка всех групп метро с обновленными списками")
            print("=" * 80)
            
            # Загружаем cookies
            cookies_data = self.load_cookies()
            if not cookies_data:
                return False
            
            # Настраиваем Selenium
            if not self.setup_selenium():
                return False
            
            # Применяем cookies
            if not self.apply_cookies(cookies_data):
                return False
            
            # Получаем существующие avito_id из БД для пропуска
            existing_avito_ids = await self.get_existing_avito_ids()
            print(f"📊 Найдено существующих avito_id в БД: {len(existing_avito_ids)}")
            
            # Обрабатываем каждую группу
            for group_name, avito_ids in self.target_groups.items():
                print(f"\n📊 Обрабатываем {group_name} ({len(avito_ids)} avito_id)...")
                
                group_results = []
                
                for avito_id in avito_ids:
                    print(f"\n🔍 Обрабатываем avito_id={avito_id}...")
                    
                    # Пропускаем уже существующие avito_id в БД
                    if avito_id in existing_avito_ids:
                        print(f"   ⏭️ Пропускаем (уже существует в БД)")
                        result = {
                            'avito_id': avito_id,
                            'metro_name': None,
                            'db_id': None,
                            'db_name': None,
                            'line_id': None,
                            'old_avito_id': avito_id,
                            'new_avito_id': avito_id,
                            'status': 'already_exists'
                        }
                        group_results.append(result)
                        self.results.append(result)
                        continue
                    
                    # Определяем станцию по avito_id
                    metro_info = self.detect_metro_station(avito_id)
                    
                    if not metro_info:
                        print(f"   ❌ Станция с avito_id={avito_id} не найдена в заголовке")
                        result = {
                            'avito_id': avito_id,
                            'metro_name': None,
                            'db_id': None,
                            'db_name': None,
                            'line_id': None,
                            'old_avito_id': None,
                            'new_avito_id': avito_id,
                            'status': 'not_found_on_avito'
                        }
                        group_results.append(result)
                        self.results.append(result)
                        continue
                    
                    # Ищем соответствующую станцию в БД
                    db_station = await self.find_matching_station(metro_info)
                    
                    if not db_station:
                        print(f"   ❌ Станция '{metro_info}' не найдена в БД")
                        result = {
                            'avito_id': avito_id,
                            'metro_name': metro_info,
                            'db_id': None,
                            'db_name': None,
                            'line_id': None,
                            'old_avito_id': None,
                            'new_avito_id': avito_id,
                            'status': 'not_found_in_db'
                        }
                        group_results.append(result)
                        self.results.append(result)
                        continue
                    
                    # Обновляем avito_id в БД
                    success, old_avito_id = await self.update_metro_avito_id(db_station['id'], avito_id)
                    
                    if success:
                        result = {
                            'avito_id': avito_id,
                            'metro_name': metro_info,
                            'db_id': db_station['id'],
                            'db_name': db_station['name'],
                            'line_id': db_station['line_id'],
                            'old_avito_id': old_avito_id,
                            'new_avito_id': avito_id,
                            'status': 'updated'
                        }
                        print(f"   ✅ Успешно обновлено avito_id={avito_id} для станции '{metro_info}'")
                    else:
                        result = {
                            'avito_id': avito_id,
                            'metro_name': metro_info,
                            'db_id': db_station['id'],
                            'db_name': db_station['name'],
                            'line_id': db_station['line_id'],
                            'old_avito_id': old_avito_id,
                            'new_avito_id': avito_id,
                            'status': 'update_failed'
                        }
                        print(f"   ❌ Ошибка обновления avito_id={avito_id}")
                    
                    group_results.append(result)
                    self.results.append(result)
                    
                    # Небольшая пауза между запросами
                    time.sleep(2)
                
                # Статистика по группе
                updated = len([r for r in group_results if r['status'] == 'updated'])
                skipped = len([r for r in group_results if r['status'] == 'already_exists'])
                print(f"\n📊 {group_name} - Результат: {updated} обновлено, {skipped} пропущено")
            
            # Сохраняем результаты
            self.save_results()
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка обработки: {e}")
            return False
        finally:
            if self.driver:
                try:
                    self.driver.quit()
                    print("🔒 Браузер закрыт")
                except:
                    pass
    
    def save_results(self):
        """Сохраняет результаты в файл"""
        try:
            result_data = {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_target_ids': len(self.all_target_ids),
                'total_processed': len(self.results),
                'target_groups': self.target_groups,
                'results': self.results
            }
            
            filename = f"complete_groups_results_{time.strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2)
            
            print(f"\n💾 Результаты сохранены в {filename}")
            
            # Выводим статистику
            updated = len([r for r in self.results if r['status'] == 'updated'])
            skipped = len([r for r in self.results if r['status'] == 'already_exists'])
            not_found_db = len([r for r in self.results if r['status'] == 'not_found_in_db'])
            not_found_avito = len([r for r in self.results if r['status'] == 'not_found_on_avito'])
            update_failed = len([r for r in self.results if r['status'] == 'update_failed'])
            
            print(f"\n📊 СТАТИСТИКА:")
            print(f"   Обработано avito_id: {len(self.all_target_ids)}")
            print(f"   Успешно обновлено в БД: {updated}")
            print(f"   Пропущено (уже существует): {skipped}")
            print(f"   Не найдено в БД: {not_found_db}")
            print(f"   Не найдено на AVITO: {not_found_avito}")
            print(f"   Ошибки обновления: {update_failed}")
            print(f"   Всего обработано: {len(self.results)}")
            
            # Выводим список обновленных станций
            if updated > 0:
                print(f"\n✅ ОБНОВЛЕННЫЕ СТАНЦИИ:")
                for result in self.results:
                    if result['status'] == 'updated':
                        print(f"   {result['avito_id']:4d} - {result['metro_name']} (БД: {result['db_name']})")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения результатов: {e}")

async def main():
    # Загружаем переменные окружения
    load_dotenv()
    
    # Получаем DATABASE_URL
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("❌ Ошибка: DATABASE_URL не установлен в .env")
        print("💡 Создайте файл .env с содержимым:")
        print("   DATABASE_URL=postgresql://username:password@host:port/database")
        return
    
    updater = CompleteGroupsUpdater()
    updater.database_url = database_url
    
    # Запускаем обработку всех групп
    await updater.process_target_groups()

if __name__ == "__main__":
    asyncio.run(main())
