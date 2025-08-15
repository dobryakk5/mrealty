#!/usr/bin/env python3
"""
Целевое обновление avito_id только для станций из ссылок AVITO
"""

import json
import os
import time
import asyncio
import asyncpg
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import re

class TargetedMetroUpdater:
    def __init__(self):
        self.cookies_file = "avito_cookies_fixed.json"
        self.driver = None
        self.database_url = None
        self.results = []
        
        # Целевые avito_id из ссылок AVITO
        self.target_avito_ids = {
            # Ссылка 1: metro=1-2-3-4-5-54-55-56-57-58-59-93-94-95-96-97-98-99-148-151-2135-2136-2142-2174-2213-2227-2238-2250-2257-2271-2288-2315-2319
            "group1": [1, 2, 3, 4, 5, 54, 55, 56, 57, 58, 59, 93, 94, 95, 96, 97, 98, 99, 148, 151, 2135, 2136, 2142, 2174, 2213, 2227, 2238, 2250, 2257, 2271, 2288, 2315, 2319],
            
            # Ссылка 2: metro=6-7-8-9-10-11-12-60-100-101-102-103-104-2144-2149-2158-2159-2165-2177-2180-2186-2187-2204-2209-2222-2235-2239-2240-2254-2311-2318-2330-2336
            "group2": [6, 7, 8, 9, 10, 11, 12, 60, 100, 101, 102, 103, 104, 2144, 2149, 2158, 2159, 2165, 2177, 2180, 2186, 2187, 2204, 2209, 2222, 2235, 2239, 2240, 2254, 2311, 2318, 2330, 2336],
            
            # Ссылка 3: metro=13-14-15-16-18-61-62-63-64-65-105-106-107-108-215-2002-2145-2150-2179-2191-2207-2247-2251-2268-2275-2279-2299-2303-2316-2321-2323-2333
            "group3": [13, 14, 15, 16, 18, 61, 62, 63, 64, 65, 105, 106, 107, 108, 215, 2002, 2145, 2150, 2179, 2191, 2207, 2247, 2251, 2268, 2275, 2279, 2299, 2303, 2316, 2321, 2323, 2333],
            
            # Ссылка 4: metro=19-20-22-66-67-109-110-111-112-127-149-1001-1004-1005-1010-1012-2147-2155-2181-2185-2201-2202-2205-2224-2253-2276-2286-2292-2293-2305-2325-2326-2328
            "group4": [19, 20, 22, 66, 67, 109, 110, 111, 112, 127, 149, 1001, 1004, 1005, 1010, 1012, 2147, 2155, 2181, 2185, 2201, 2202, 2205, 2224, 2253, 2276, 2286, 2292, 2293, 2305, 2325, 2326, 2328],
            
            # Ссылка 5: metro=21-23-24-25-26-27-68-69-70-113-114-115-116-117-120-214-1002-1007-2160-2163-2168-2190-2221-2223-2234-2243-2256-2266-2272-2283-2290-2296-2329
            "group5": [21, 23, 24, 25, 26, 27, 68, 69, 70, 113, 114, 115, 116, 117, 120, 214, 1002, 1007, 2160, 2163, 2168, 2190, 2221, 2223, 2234, 2243, 2256, 2266, 2272, 2283, 2290, 2296, 2329],
            
            # Ссылка 6: metro=28-29-71-72-118-119-121-122-152-1003-2133-2148-2152-2173-2200-2208-2215-2225-2226-2228-2229-2245-2248-2249-2252-2262-2264-2289-2301-2317-2331-2337-2339
            "group6": [28, 29, 71, 72, 118, 119, 121, 122, 152, 1003, 2133, 2148, 2152, 2173, 2200, 2208, 2215, 2225, 2226, 2228, 2229, 2245, 2248, 2249, 2252, 2262, 2264, 2289, 2301, 2317, 2331, 2337, 2339],
            
            # Ссылка 7: metro=17-30-31-32-33-34-73-74-75-123-124-125-126-128-1006-1011-2001-2143-2157-2171-2172-2199-2210-2220-2244-2255-2269-2285-2304-2309-2313-2337-2340
            "group7": [17, 30, 31, 32, 33, 34, 73, 74, 75, 123, 124, 125, 126, 128, 1006, 1011, 2001, 2143, 2157, 2171, 2172, 2199, 2210, 2220, 2244, 2255, 2269, 2285, 2304, 2309, 2313, 2337, 2340],
            
            # Ссылка 8: metro=35-37-76-77-78-79-129-130-131-217-1008-1009-2169-2176-2184-2212-2214-2230-2237-2267-2278-2282-2291-2298-2306-2310-2312-2314-2324-2338
            "group8": [35, 37, 76, 77, 78, 79, 129, 130, 131, 217, 1008, 1009, 2169, 2176, 2184, 2212, 2214, 2230, 2237, 2267, 2278, 2282, 2291, 2298, 2306, 2310, 2312, 2314, 2324, 2338],
            
            # Ссылка 9: metro=36-38-39-40-41-42-43-44-45-80-81-82-83-132-133-134-135-2154-2161-2183-2188-2193-2194-2233-2242-2260-2261-2270-2287-2300-2327-2332-2344
            "group9": [36, 38, 39, 40, 41, 42, 43, 44, 45, 80, 81, 82, 83, 132, 133, 134, 135, 2154, 2161, 2183, 2188, 2193, 2194, 2233, 2242, 2260, 2261, 2270, 2287, 2300, 2327, 2332, 2344],
            
            # Ссылка 10: metro=46-47-48-85-86-87-88-89-90-136-137-138-139-140-141-142-216-2151-2162-2166-2182-2195-2241-2259-2265-2294-2297-2302-2320-2334-2343
            "group10": [46, 47, 48, 85, 86, 87, 88, 89, 90, 136, 137, 138, 139, 140, 141, 142, 216, 2151, 2162, 2166, 2182, 2195, 2241, 2259, 2265, 2294, 2297, 2302, 2320, 2334, 2343],
            
            # Ссылка 11: metro=49-50-51-52-53-91-92-143-144-145-146-147-2167-2219-2236-2263-2273-2274-2277-2284-2295-2322-2335-2342
            "group11": [49, 50, 51, 52, 53, 91, 92, 143, 144, 145, 146, 147, 2167, 2219, 2236, 2263, 2273, 2274, 2277, 2284, 2295, 2322, 2335, 2342]
        }
        
        # Объединяем все avito_id в один список
        self.all_target_ids = []
        for group_name, ids in self.target_avito_ids.items():
            self.all_target_ids.extend(ids)
        
        # Убираем дубликаты и сортируем
        self.all_target_ids = sorted(list(set(self.all_target_ids)))
        
        print(f"🎯 Целевые avito_id: {len(self.all_target_ids)} уникальных значений")
        print(f"📊 Диапазон: от {min(self.all_target_ids)} до {max(self.all_target_ids)}")
        
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
        
        print("🔧 Создаем браузер...")
        self.driver = webdriver.Chrome(options=options)
        
        # Убираем webdriver
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return True
    
    def apply_cookies(self, cookies_data):
        """Применяет cookies к драйверу"""
        try:
            if not cookies_data or 'cookies' not in cookies_data:
                return False
            
            # Сначала переходим на домен
            print("🌐 Переходим на AVITO...")
            self.driver.get("https://avito.ru")
            time.sleep(3)
            
            # Применяем cookies
            applied_count = 0
            for cookie in cookies_data['cookies']:
                try:
                    cookie_dict = {
                        'name': cookie['name'],
                        'value': cookie['value'],
                        'domain': cookie.get('domain', ''),
                        'path': cookie.get('path', '/')
                    }
                    
                    if cookie.get('expiry'):
                        cookie_dict['expiry'] = cookie['expiry']
                    if cookie.get('secure'):
                        cookie_dict['secure'] = cookie['secure']
                    if cookie.get('httpOnly'):
                        cookie_dict['httpOnly'] = cookie['httpOnly']
                    
                    self.driver.add_cookie(cookie_dict)
                    applied_count += 1
                    
                except Exception as e:
                    continue
            
            print(f"✅ Применено cookies: {applied_count}")
            
            # Обновляем страницу с примененными cookies
            print("🔄 Обновляем страницу с cookies...")
            self.driver.refresh()
            time.sleep(5)
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка применения cookies: {e}")
            return False
    
    def detect_metro_station(self, avito_id):
        """Определяет станцию метро по avito_id"""
        try:
            # URL с указанным metro ID
            base_url = "https://www.avito.ru/moskva/kvartiry/prodam/vtorichka-ASgBAgICAkSSA8YQ5geMUg"
            target_url = f"{base_url}?metro={avito_id}"
            
            print(f"🔍 Проверяем avito_id={avito_id}...")
            
            # Переходим на страницу
            self.driver.get(target_url)
            time.sleep(3)
            
            current_url = self.driver.current_url
            page_title = self.driver.title
            
            # Проверяем, что страница загрузилась
            if "data:" in current_url or "about:blank" in current_url:
                print(f"❌ Страница не загрузилась для avito_id={avito_id}")
                return None
            
            # Ищем информацию о метро на странице
            metro_info = self.extract_metro_info(page_title, self.driver.page_source)
            
            if metro_info:
                metro_info['avito_id'] = avito_id
                metro_info['url'] = current_url
                print(f"✅ Найдена станция: {metro_info['name']}")
                return metro_info
            else:
                print(f"❌ Станция с avito_id={avito_id} не найдена")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка определения станции avito_id={avito_id}: {e}")
            return None
    
    def extract_metro_info(self, page_title, page_source):
        """Извлекает информацию о метро со страницы"""
        try:
            # Паттерн 1: "у метро [Название станции]" - улучшенный для полных названий
            metro_pattern1 = r'у метро\s+([А-Яа-яЁё\s\-]+?)(?:\s*[,\.]|\s*-\s*Авито|\s*\||\s*$)'
            match1 = re.search(metro_pattern1, page_source)
            
            # Паттерн 2: "метро [Название станции]" - улучшенный для полных названий
            metro_pattern2 = r'метро\s+([А-Яа-яЁё\s\-]+?)(?:\s*[,\.]|\s*-\s*Авито|\s*\||\s*$)'
            match2 = re.search(metro_pattern2, page_source)
            
            # Паттерн 3: в заголовке страницы - улучшенный
            title_pattern = r'([А-Яа-яЁё\s\-]+?)\s*-\s*Авито'
            title_match = re.search(title_pattern, page_title)
            
            # Паттерн 4: "у метро [Название станции]: [описание]" - для случаев с двоеточием
            metro_pattern4 = r'у метро\s+([А-Яа-яЁё\s\-]+?):'
            match4 = re.search(metro_pattern4, page_source)
            
            metro_name = None
            if match1:
                metro_name = match1.group(1).strip()
            elif match2:
                metro_name = match2.group(1).strip()
            elif match4:
                metro_name = match4.group(1).strip()
            elif title_match:
                metro_name = title_match.group(1).strip()
            
            if not metro_name:
                return None
            
            # Дополнительная очистка названия
            # Убираем лишние пробелы и символы
            metro_name = re.sub(r'\s+', ' ', metro_name).strip()
            # Убираем символы, которые могут быть в конце
            metro_name = re.sub(r'[,\-\.\s]+$', '', metro_name)
            
            # Ищем информацию о линии метро
            line_pattern = r'линия\s+(\d+)'
            line_match = re.search(line_pattern, page_source, re.IGNORECASE)
            line_number = line_match.group(1) if line_match else "неизвестно"
            
            # Ищем время до метро
            time_pattern = r'(\d+[–-]\d+)\s*мин'
            time_match = re.search(time_pattern, page_source)
            time_to_metro = time_match.group(1) if time_match else "неизвестно"
            
            return {
                'name': metro_name,
                'line': line_number,
                'time': time_to_metro
            }
            
        except Exception as e:
            print(f"❌ Ошибка извлечения информации о метро: {e}")
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
            print(f"📊 Найдено существующих avito_id: {len(existing_ids)}")
            return set(existing_ids)
            
        except Exception as e:
            print(f"❌ Ошибка получения существующих avito_id: {e}")
            return set()
    
    async def find_matching_station(self, metro_name, line_id):
        """Ищет станцию в БД по названию и линии"""
        try:
            conn = await asyncpg.connect(self.database_url)
            
            # Ищем по названию (точное совпадение)
            station = await conn.fetchrow("""
                SELECT id, name, line_id, cian_id, avito_id
                FROM metro 
                WHERE LOWER(name) = LOWER($1)
                ORDER BY ABS(line_id - $2)  -- Ближайшая линия
                LIMIT 1
            """, metro_name, line_id if line_id != "неизвестно" else 0)
            
            await conn.close()
            return station
            
        except Exception as e:
            print(f"❌ Ошибка поиска станции {metro_name}: {e}")
            return None
    
    async def update_metro_avito_id(self, metro_id, avito_id):
        """Обновляет avito_id для станции метро"""
        try:
            conn = await asyncpg.connect(self.database_url)
            
            result = await conn.execute("""
                UPDATE metro 
                SET avito_id = $1 
                WHERE id = $2
            """, avito_id, metro_id)
            
            await conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Ошибка обновления avito_id для станции {metro_id}: {e}")
            return False
    
    async def process_targeted_metros(self):
        """Обрабатывает только целевые avito_id"""
        try:
            print(f"🎯 Целевое обновление avito_id для станций метро из ссылок AVITO")
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
            
            # Получаем существующие avito_id
            existing_avito_ids = await self.get_existing_avito_ids()
            
            # Обрабатываем только целевые avito_id
            for avito_id in self.all_target_ids:
                print(f"\n🔍 Обрабатываем avito_id={avito_id}...")
                
                # Пропускаем уже существующие
                if avito_id in existing_avito_ids:
                    print(f"   ⏭️ Пропускаем (уже существует в БД)")
                    continue
                
                # Определяем станцию по avito_id
                metro_info = self.detect_metro_station(avito_id)
                
                if metro_info:
                    # Ищем соответствующую станцию в БД
                    line_id = int(metro_info['line']) if metro_info['line'].isdigit() else 0
                    db_station = await self.find_matching_station(metro_info['name'], line_id)
                    
                    if db_station:
                        print(f"   📍 Найдена в БД: {db_station['name']} (ID: {db_station['id']}, Линия: {db_station['line_id']})")
                        
                        # Обновляем avito_id
                        print(f"   🔄 Обновляем avito_id: {db_station['avito_id']} → {avito_id}")
                        
                        if await self.update_metro_avito_id(db_station['id'], avito_id):
                            print(f"   ✅ Обновлено успешно")
                            
                            # Сохраняем результат
                            result = {
                                'avito_id': avito_id,
                                'metro_name': metro_info['name'],
                                'db_id': db_station['id'],
                                'db_name': db_station['name'],
                                'line_id': db_station['line_id'],
                                'old_avito_id': db_station['avito_id'],
                                'new_avito_id': avito_id,
                                'status': 'updated'
                            }
                            self.results.append(result)
                        else:
                            print(f"   ❌ Ошибка обновления")
                    else:
                        print(f"   ⚠️ Станция {metro_info['name']} не найдена в БД")
                        
                        # Сохраняем результат как не найденную
                        result = {
                            'avito_id': avito_id,
                            'metro_name': metro_info['name'],
                            'db_id': None,
                            'db_name': None,
                            'line_id': None,
                            'old_avito_id': None,
                            'new_avito_id': avito_id,
                            'status': 'not_found_in_db'
                        }
                        self.results.append(result)
                else:
                    print(f"   ❌ Станция не найдена на AVITO")
                    
                    # Сохраняем результат как не найденную
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
                    self.results.append(result)
                
                # Небольшая пауза между запросами
                time.sleep(1)
            
            # Сохраняем результаты
            self.save_results()
            
            return True
            
        except Exception as e:
            print(f"❌ Ошибка обработки: {e}")
            return False
        finally:
            if self.driver:
                self.driver.quit()
                print("🔒 Браузер закрыт")
    
    def save_results(self):
        """Сохраняет результаты в файл"""
        try:
            result_data = {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_target_ids': len(self.all_target_ids),
                'total_processed': len(self.results),
                'target_avito_ids': self.all_target_ids,
                'results': self.results
            }
            
            filename = f"targeted_metro_avito_id_results_{time.strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2)
            
            print(f"\n💾 Результаты сохранены в {filename}")
            
            # Выводим статистику
            updated = len([r for r in self.results if r['status'] == 'updated'])
            not_found_db = len([r for r in self.results if r['status'] == 'not_found_in_db'])
            not_found_avito = len([r for r in self.results if r['status'] == 'not_found_on_avito'])
            
            print(f"\n📊 СТАТИСТИКА:")
            print(f"   Целевых avito_id: {len(self.all_target_ids)}")
            print(f"   Обновлено в БД: {updated}")
            print(f"   Не найдено в БД: {not_found_db}")
            print(f"   Не найдено на AVITO: {not_found_avito}")
            print(f"   Всего обработано: {len(self.results)}")
            
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
    
    updater = TargetedMetroUpdater()
    updater.database_url = database_url
    
    # Запускаем целевое обновление
    await updater.process_targeted_metros()

if __name__ == "__main__":
    asyncio.run(main())
