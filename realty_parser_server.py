"""
HTTP API сервер для парсинга недвижимости
Предназначен для использования с Fastify (Node.js)
"""

import re
import json
import asyncio
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass, asdict
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# Импортируем парсер Avito
try:
    from avito_parser_integration import AvitoCardParser
    AVITO_AVAILABLE = True
except ImportError:
    AVITO_AVAILABLE = False
    print("⚠️ Модуль avito_parser_integration не найден, парсинг Avito недоступен")

# Заголовки для HTTP-запросов
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/115.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'ru-RU,ru;q=0.9',
}

@dataclass
class PropertyData:
    """Структурированные данные о недвижимости"""
    # Основная информация
    rooms: Optional[int] = None
    price: Optional[float] = None
    total_area: Optional[float] = None
    living_area: Optional[float] = None
    kitchen_area: Optional[float] = None
    floor: Optional[str] = None
    total_floors: Optional[int] = None
    
    # Характеристики
    bathroom: Optional[str] = None
    balcony: Optional[str] = None
    renovation: Optional[str] = None
    construction_year: Optional[int] = None
    house_type: Optional[str] = None
    ceiling_height: Optional[float] = None
    furniture: Optional[str] = None
    
    # Расположение
    address: Optional[str] = None
    metro_station: Optional[str] = None
    metro_time: Optional[int] = None
    
    # Дополнительно
    tags: Optional[List[str]] = None
    description: Optional[str] = None
    photo_urls: Optional[List[str]] = None
    
    # Метаданные
    source: Optional[str] = None  # 'avito' или 'cian'
    url: Optional[str] = None
    status: Optional[str] = None
    views_today: Optional[int] = None
    total_views: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Преобразует в словарь для JSON"""
        return asdict(self)
    
    def to_json(self) -> str:
        """Преобразует в JSON строку"""
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)

# Pydantic модели для FastAPI
class ParseUrlsRequest(BaseModel):
    urls: List[str]

class ParseTextRequest(BaseModel):
    text: str

class ParseResponse(BaseModel):
    success: bool
    data: List[PropertyData]
    total: int
    message: str
    timestamp: str

class RealtyParserAPI:
    """API класс для парсинга недвижимости"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
    
    def is_avito_url(self, url: str) -> bool:
        """Определяет, является ли ссылка ссылкой на Avito"""
        return 'avito.ru' in url.lower()
    
    def is_cian_url(self, url: str) -> bool:
        """Определяет, является ли ссылка ссылкой на Cian"""
        return 'cian.ru' in url.lower()
    
    def get_url_source(self, url: str) -> str:
        """Возвращает источник ссылки"""
        if self.is_avito_url(url):
            return 'avito'
        elif self.is_cian_url(url):
            return 'cian'
        else:
            return 'unknown'
    
    async def parse_property(self, url: str) -> Optional[PropertyData]:
        """
        Универсальный метод для парсинга объявлений
        Возвращает структурированные данные PropertyData
        """
        try:
            if self.is_avito_url(url):
                return await self._parse_avito_property(url)
            elif self.is_cian_url(url):
                return await self._parse_cian_property(url)
            else:
                print(f"⚠️ Неизвестный источник ссылки: {url}")
                return None
        except Exception as e:
            print(f"❌ Ошибка парсинга {url}: {e}")
            return None
    
    async def parse_properties_batch(self, urls: List[str]) -> List[PropertyData]:
        """
        Пакетный парсинг множественных объявлений
        Возвращает список PropertyData объектов
        """
        results = []
        for i, url in enumerate(urls, 1):
            try:
                print(f"🔄 Парсим объявление {i}/{len(urls)}: {url}")
                property_data = await self.parse_property(url)
                if property_data:
                    results.append(property_data)
                    print(f"✅ Объявление успешно спарсено")
                else:
                    print(f"❌ Не удалось спарсить объявление")
            except Exception as e:
                print(f"❌ Ошибка при парсинге {url}: {e}")
                continue
        
        print(f"📊 Всего успешно спарсено: {len(results)} из {len(urls)}")
        return results
    
    async def _parse_avito_property(self, url: str) -> Optional[PropertyData]:
        """Парсит объявление с Avito"""
        if not AVITO_AVAILABLE:
            print("❌ Парсер Avito недоступен")
            return None
        
        try:
            print(f"🏠 Парсим объявление Avito: {url}")
            
            # Создаем парсер Avito
            parser = AvitoCardParser()
            
            # Парсим полную страницу объявления
            parsed_data = parser.parse_avito_page(url)
            if not parsed_data:
                print("❌ Не удалось спарсить данные объявления Avito")
                return None
            
            # Преобразуем данные в формат для БД
            db_data = parser.prepare_data_for_db(parsed_data)
            if not db_data:
                print("❌ Не удалось подготовить данные для БД")
                return None
            
            # Создаем структурированный объект
            property_data = PropertyData(
                rooms=db_data.get('rooms'),
                price=db_data.get('price'),
                total_area=db_data.get('total_area'),
                living_area=db_data.get('living_area'),
                kitchen_area=db_data.get('kitchen_area'),
                floor=db_data.get('floor'),
                total_floors=db_data.get('total_floors'),
                bathroom=db_data.get('bathroom'),
                balcony=db_data.get('balcony'),
                renovation=db_data.get('renovation'),
                construction_year=db_data.get('construction_year'),
                house_type=db_data.get('house_type'),
                ceiling_height=db_data.get('ceiling_height'),
                furniture=db_data.get('furniture'),
                address=db_data.get('address'),
                metro_station=db_data.get('metro_station'),
                metro_time=db_data.get('metro_time'),
                tags=db_data.get('tags'),
                description=db_data.get('description'),
                photo_urls=db_data.get('photo_urls'),
                source='avito',
                url=url,
                status='active',
                views_today=db_data.get('today_views'),
                total_views=db_data.get('total_views')
            )
            
            print(f"✅ Объявление Avito успешно спарсено")
            return property_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга объявления Avito: {e}")
            return None
        finally:
            # Закрываем браузер
            if 'parser' in locals() and parser.driver:
                try:
                    parser.cleanup()
                except:
                    pass
    
    async def _parse_cian_property(self, url: str) -> Optional[PropertyData]:
        """Парсит объявление с Cian"""
        try:
            print(f"🏠 Парсим объявление Cian: {url}")
            
            # Получаем страницу
            response = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.session.get(url)
            )
            response.raise_for_status()
            
            # Парсим HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Извлекаем данные
            data = self._extract_cian_data(soup, url)
            
            # Создаем структурированный объект
            property_data = PropertyData(
                rooms=data.get('Комнат'),
                price=data.get('Цена_raw'),
                total_area=data.get('Общая площадь'),
                living_area=data.get('Жилая площадь'),
                kitchen_area=data.get('Площадь кухни'),
                floor=data.get('Этаж'),
                total_floors=data.get('Всего этажей'),
                bathroom=data.get('Санузел'),
                balcony=data.get('Балкон/лоджия'),
                renovation=data.get('Ремонт'),
                construction_year=data.get('Год постройки'),
                house_type=data.get('Тип дома'),
                ceiling_height=data.get('Высота потолков'),
                furniture=data.get('Мебель'),
                address=data.get('Адрес'),
                metro_station=data.get('Метро'),
                metro_time=data.get('Минут метро'),
                tags=data.get('Метки'),
                description=data.get('Описание'),
                photo_urls=data.get('photo_urls', []),
                source='cian',
                url=url,
                status=data.get('Статус', 'active'),
                views_today=data.get('Просмотров сегодня'),
                total_views=data.get('Всего просмотров')
            )
            
            print(f"✅ Объявление Cian успешно спарсено")
            return property_data
            
        except Exception as e:
            print(f"❌ Ошибка парсинга объявления Cian: {e}")
            return None
    
    def _extract_cian_data(self, soup: BeautifulSoup, url: str) -> Dict[str, Any]:
        """Извлекает данные из HTML страницы Cian"""
        data = {'URL': url}
        
        # Определяем возможную блокировку
        page_text = soup.get_text(" ", strip=True).lower()
        is_blocked = bool(re.search(r"подтвердите, что запросы.*не робот|похожи на автоматические", page_text))
        if is_blocked:
            data['Статус'] = None
        elif soup.find(string=re.compile(r"Объявление снято", re.IGNORECASE)):
            data['Статус'] = 'Снято'
        
        # Метки
        labels = [span.get_text(strip=True) for span in soup.select('div[data-name="LabelsLayoutNew"] > span span:last-of-type')]
        data['Метки'] = '; '.join(labels) if labels else None
        
        # Количество комнат
        h1 = soup.find('h1')
        if h1:
            m = re.search(r"(\d+)[^\d]*[-–]?комн", h1.get_text())
            if m:
                data['Комнат'] = self._extract_number(m.group(1))
        
        # Цена
        price_el = (
            soup.select_one('[data-name="NewbuildingPriceInfo"] [data-testid="price-amount"] span')
            or soup.select_one('[data-name="AsideGroup"] [data-testid="price-amount"] span')
        )
        if price_el:
            data['Цена_raw'] = self._extract_number(price_el.get_text())
            if 'Статус' not in data or data['Статус'] is None:
                data['Статус'] = 'Активно'
        
        # Основная информация
        summary = soup.select_one('[data-name="OfferSummaryInfoLayout"]')
        if summary:
            for item in summary.select('[data-name="OfferSummaryInfoItem"]'):
                ps = item.find_all('p')
                if len(ps) < 2:
                    continue
                key = ps[0].get_text(strip=True)
                val = ps[1].get_text(strip=True)
                kl = key.lower().strip()
                if key == 'Строительная серия':
                    data[key] = val
                    continue
                if kl == 'этаж': 
                    # Разбираем этаж на текущий и общий
                    floor_info = self._parse_floor_info(val)
                    data['Этаж'] = floor_info['current_floor']
                    data['Всего этажей'] = floor_info['total_floors']
                    continue
                if kl in ['санузел', 'балкон/лоджия', 'количество лифтов']:
                    data[key] = val
                    continue
                data[key] = self._extract_number(val) if re.search(r"\d", val) else val
        
        # Дополнительная информация
        cont = soup.find('div', {'data-name': 'ObjectFactoids'})
        if cont:
            lines = cont.get_text(separator='\n', strip=True).split('\n')
            for i in range(0, len(lines)-1, 2):
                key, val = lines[i].strip(), lines[i+1].strip()
                kl = key.lower().strip()
                if key == 'Строительная серия': 
                    data[key] = val
                    continue
                if kl == 'этаж' and 'Этаж' not in data: 
                    # Разбираем этаж на текущий и общий
                    floor_info = self._parse_floor_info(val)
                    data['Этаж'] = floor_info['current_floor']
                    data['Всего этажей'] = floor_info['total_floors']
                elif kl in ['санузел', 'балкон/лоджия', 'количество лифтов']: 
                    data[key] = val
                else: 
                    data[key] = self._extract_number(val) if re.search(r"\d", val) else val
        
        # Статистика просмотров
        stats_re = re.compile(r"([\d\s]+)\sпросмотр\S*,\s*(\d+)\sза сегодня,\s*(\d+)\sуникаль", re.IGNORECASE)
        st = soup.find(string=stats_re)
        if st:
            m = stats_re.search(st)
            data['Всего просмотров'], data['Просмотров сегодня'], data['Уникальных просмотров'] = (
                self._extract_number(m.group(1)), self._extract_number(m.group(2)), self._extract_number(m.group(3))
            )
        
        # География и метро
        geo = soup.select_one('div[data-name="Geo"]')
        if geo:
            span = geo.find('span', itemprop='name')
            addr = span['content'] if span and span.get('content') else ', '.join(
                a.get_text(strip=True) for a in geo.select('a[data-name="AddressItem"]')
            )
            parts = [s.strip() for s in addr.split(',') if s.strip()]
            data['Адрес'] = ', '.join(parts[-2:]) if len(parts) > 1 else addr
            
            # Метро
            stations = []
            for li in geo.select('ul[data-name="UndergroundList"] li[data-name="UndergroundItem"]'):
                st_el = li.find('a', href=True)
                tm_el = li.find('span', class_=re.compile(r".*underground_time.*"))
                if st_el and tm_el:
                    name = st_el.get_text(strip=True)
                    m = re.search(r"(\d+)", tm_el.get_text(strip=True))
                    stations.append((name, int(m.group(1)) if m else None))
            if stations:
                station, time_to = min(stations, key=lambda x: x[1] or float('inf'))
                data['Минут метро'] = f"{time_to} {station}"
        
        # Фотографии
        data['photo_urls'] = self._extract_cian_photos(soup)
        
        return data
    
    def _parse_floor_info(self, text: str) -> Dict[str, Optional[int]]:
        """Разбирает информацию об этаже на текущий и общий"""
        if not text:
            return {'current_floor': None, 'total_floors': None}
        
        s = str(text).replace('\u00A0', ' ').strip().lower()
        
        # Паттерн "13 из 37" или "13/37"
        m = re.search(r"(\d+)\s*(?:из|/)\s*(\d+)", s)
        if m:
            return {
                'current_floor': int(m.group(1)),
                'total_floors': int(m.group(2))
            }
        
        # Только текущий этаж
        m2 = re.search(r"(\d+)\b", s)
        if m2:
            return {
                'current_floor': int(m2.group(1)),
                'total_floors': None
            }
        
        return {'current_floor': None, 'total_floors': None}
    
    def _extract_cian_photos(self, soup: BeautifulSoup) -> List[str]:
        """Извлекает ссылки на фотографии с Cian"""
        photo_urls = []
        
        try:
            # Ищем галерею
            gallery = soup.find('div', {'data-name': 'GalleryInnerComponent'})
            if not gallery:
                return photo_urls
            
            # Основные изображения
            images = gallery.find_all('img', src=True)
            for img in images:
                src = img.get('src')
                if src and src.startswith('http') and 'cdn-cian.ru' in src:
                    photo_urls.append(src)
            
            # Изображения в background-image
            elements_with_bg = gallery.find_all(style=re.compile(r'background-image'))
            for elem in elements_with_bg:
                style = elem.get('style', '')
                bg_match = re.search(r'background-image:\s*url\(["\']?([^"\')\s]+)["\']?\)', style)
                if bg_match:
                    bg_url = bg_match.group(1)
                    if bg_url.startswith('http') and ('cdn-cian.ru' in bg_url or 'kinescopecdn.net' in bg_url):
                        photo_urls.append(bg_url)
            
            # Убираем дубликаты, сохраняя порядок
            seen = set()
            unique_photos = []
            for url in photo_urls:
                if url not in seen:
                    seen.add(url)
                    unique_photos.append(url)
            
            return unique_photos
            
        except Exception as e:
            print(f"Ошибка при извлечении фотографий Cian: {e}")
            return []
    
    def _extract_number(self, text: str) -> Optional[Union[int, float]]:
        """Извлекает число из текста"""
        if not text or text == '—':
            return None
        cleaned = re.sub(r"[^\d.,]", "", text)
        cleaned = cleaned.replace('\u00A0', '').replace(' ', '').replace(',', '.')
        try:
            return float(cleaned) if '.' in cleaned else int(cleaned)
        except ValueError:
            return None
    
    def cleanup(self):
        """Корректно закрывает ресурсы"""
        try:
            self.session.close()
        except Exception as e:
            print(f"⚠️ Ошибка при очистке ресурсов: {e}")
    
    def __del__(self):
        """Деструктор для автоматической очистки"""
        self.cleanup()

# Создаем экземпляр парсера
parser = RealtyParserAPI()

# Создаем FastAPI приложение
app = FastAPI(
    title="Realty Parser HTTP API",
    description="API для парсинга объявлений недвижимости с Avito и Cian",
    version="1.0.0"
)

# Настройка CORS для Fastify
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене укажите конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API endpoints
@app.post("/api/parse/urls", response_model=ParseResponse)
async def parse_by_urls(request: ParseUrlsRequest):
    """Парсинг по списку URL"""
    try:
        properties = await parser.parse_properties_batch(request.urls)
        
        return ParseResponse(
            success=True,
            data=properties,
            total=len(properties),
            message=f"Успешно спарсено {len(properties)} из {len(request.urls)} объявлений",
            timestamp=str(datetime.now())
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга: {str(e)}")

@app.post("/api/parse/text", response_model=ParseResponse)
async def parse_from_text(request: ParseTextRequest):
    """Парсинг объявлений из текста"""
    try:
        # Извлекаем URL из текста
        urls = extract_urls(request.text)
        if not urls:
            raise HTTPException(status_code=400, detail="URL не найдены в тексте")
        
        # Парсим объявления
        properties = await parser.parse_properties_batch(urls)
        
        return ParseResponse(
            success=True,
            data=properties,
            total=len(properties),
            message=f"Извлечено {len(urls)} URL, успешно спарсено {len(properties)} объявлений",
            timestamp=str(datetime.now())
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга из текста: {str(e)}")

@app.get("/api/parse/single")
async def parse_single_property(url: str):
    """Парсинг одного объявления по URL"""
    try:
        property_data = await parser.parse_property(url)
        if property_data:
            return {
                "success": True,
                "data": property_data.to_dict(),
                "message": "Объявление успешно спарсено"
            }
        else:
            raise HTTPException(status_code=400, detail="Не удалось спарсить объявление")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка парсинга: {str(e)}")

@app.get("/api/health")
async def health_check():
    """Проверка состояния API"""
    return {
        "status": "healthy",
        "service": "realty-parser-api",
        "avito_available": parser.is_avito_url("https://avito.ru"),
        "cian_available": parser.is_cian_url("https://cian.ru")
    }

@app.get("/api/sources")
async def get_supported_sources():
    """Получение списка поддерживаемых источников"""
    return {
        "sources": [
            {
                "name": "Avito",
                "domain": "avito.ru",
                "available": AVITO_AVAILABLE,
                "source_id": "avito"
            },
            {
                "name": "Cian", 
                "domain": "cian.ru",
                "available": True,
                "source_id": "cian"
            }
        ]
    }

# Функции для быстрого доступа (для использования в других Python модулях)
async def parse_property(url: str) -> Optional[PropertyData]:
    """Быстрый парсинг одного объявления"""
    return await parser.parse_property(url)

async def parse_properties_batch(urls: List[str]) -> List[PropertyData]:
    """Быстрый пакетный парсинг"""
    return await parser.parse_properties_batch(urls)

def extract_urls(raw_input: str) -> List[str]:
    """Извлекает URL из текста"""
    return re.findall(r'https?://[^\s,;]+', raw_input)

# Создаем экземпляр класса для использования в других модулях
realty_parser = RealtyParserAPI()

if __name__ == "__main__":
    # Запуск HTTP сервера
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8008,
        log_level="info"
    )
