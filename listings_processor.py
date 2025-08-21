import re, json
import pandas as pd
import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook,load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from io import BytesIO
from typing import List, Dict, Any, Tuple
import asyncio
from datetime import datetime


# Асинхронное сохранение в БД
from db_handler import save_listings, find_similar_ads_grouped, call_update_ad

# Импортируем модуль для работы с фотографиями
from photo_processor import photo_processor

# Заголовки для HTTP-запросов
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/115.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'ru-RU,ru;q=0.9',
}

def extract_number(text: str):
    if not text or text == '—':
        return None
    cleaned = re.sub(r"[^\d.,]", "", text)
    cleaned = cleaned.replace('\u00A0', '').replace(' ', '').replace(',', '.')
    try:
        return float(cleaned) if '.' in cleaned else int(cleaned)
    except ValueError:
        return None

async def export_listings_to_excel(listing_urls: list[str], user_id: int, output_path: str = None) -> tuple[BytesIO, int]:
    """
    Парсит список объявлений, сохраняет их в БД и возвращает Excel-файл и request_id.
    :param listing_urls: список URL объявлений
    :param user_id: ID пользователя для сохранения в БД
    :param output_path: опциональный путь для сохранения файла на диск
    :return: tuple (BytesIO с данными файла, request_id)
    """
    sess = requests.Session()
    rows = []
    for url in listing_urls:
        try:
            rows.append(parse_listing(url, sess))
        except Exception as e:
            print(f"Ошибка при парсинге {url}: {e}")

    # Сохраняем и получаем request_id
    request_id = await save_listings(rows, user_id)

    # Формируем DataFrame
    df = pd.DataFrame(rows)

    # Обработка цен
    if 'Цена_raw' in df.columns:
        df['Цена'] = df['Цена_raw']
        df = df.sort_values('Цена_raw')
        df.drop('Цена_raw', axis=1, inplace=True)

    # Порядок колонок
    ordered = [
        'Комнат', 'Цена', 'Общая площадь', 'Жилая площадь',
        'Площадь кухни', 'Санузел', 'Балкон/лоджия', 'Вид из окон',
        'Ремонт', 'Этаж', 'Год постройки', 'Строительная серия',
        'Тип дома', 'Тип перекрытий', 'Количество лифтов', 'Парковка',
        'Подъезды', 'Отопление', 'Аварийность', 'Газоснабжение',
        'Всего просмотров', 'Просмотров сегодня', 'Уникальных просмотров',
        'Адрес', 'Минут метро', 'Метки', 'Статус', 'Тип жилья', 'URL'
    ]
    df = df[[c for c in ordered if c in df.columns]]

    # Запись в BytesIO
    bio = BytesIO()
    df.to_excel(bio, index=False)
    bio.seek(0)

    # Если указан путь, сохраняем файл и форматируем столбец 'Цена'
    if output_path:
        with open(output_path, 'wb') as f:
            f.write(bio.getbuffer())

        wb = load_workbook(output_path)
        ws = wb.active
        # Выделяем заголовки
        for cell in ws[1]:
            cell.font = Font(bold=True)

        # Определяем колонку 'Цена' и задаем формат тысяч
        price_idx = df.columns.get_loc('Цена') + 1
        price_col = get_column_letter(price_idx)
        custom_format = '#,##0'
        for row in range(2, ws.max_row + 1):
            cell = ws[f"{price_col}{row}"]
            if isinstance(cell.value, (int, float)):
                cell.number_format = custom_format

        wb.save(output_path)

    return bio, request_id
# Полный парсинг страницы объявления

def parse_listing(url: str, session: requests.Session) -> dict:
    resp = session.get(url, headers=HEADERS)
    resp.encoding = 'utf-8'
    soup = BeautifulSoup(resp.text, 'html.parser')

    data = {'URL': url}
    # Определяем возможную блокировку (капча/антибот)
    page_text = soup.get_text(" ", strip=True).lower()
    is_blocked = bool(re.search(r"подтвердите, что запросы.*не робот|похожи на автоматические", page_text))
    if is_blocked:
        data['Статус'] = None
    elif soup.find(string=re.compile(r"Объявление снято", re.IGNORECASE)):
        data['Статус'] = 'Снято'
    labels = [span.get_text(strip=True) for span in soup.select('div[data-name="LabelsLayoutNew"] > span span:last-of-type')]
    data['Метки'] = '; '.join(labels) if labels else None
    h1 = soup.find('h1')
    if h1:
        m = re.search(r"(\d+)[^\d]*[-–]?комн", h1.get_text())
        if m:
            data['Комнат'] = extract_number(m.group(1))
    price_el = (
        soup.select_one('[data-name="NewbuildingPriceInfo"] [data-testid="price-amount"] span')
        or soup.select_one('[data-name="AsideGroup"] [data-testid="price-amount"] span')
    )
    if price_el:
        data['Цена_raw'] = extract_number(price_el.get_text())
        # Если статус ещё не определён и удалось найти цену — считаем объявление активным
        if 'Статус' not in data or data['Статус'] is None:
            data['Статус'] = 'Активно'

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
            if kl == 'этаж': data['Этаж'] = val; continue
            if kl in ['санузел', 'балкон/лоджия', 'количество лифтов']:
                data[key] = val; continue
            data[key] = extract_number(val) if re.search(r"\d", val) else val

    cont = soup.find('div', {'data-name': 'ObjectFactoids'})
    if cont:
        lines = cont.get_text(separator='\n', strip=True).split('\n')
        for i in range(0, len(lines)-1, 2):
            key, val = lines[i].strip(), lines[i+1].strip()
            kl = key.lower().strip()
            if key == 'Строительная серия': data[key] = val; continue
            if kl == 'этаж' and 'Этаж' not in data: data['Этаж'] = val
            elif kl in ['санузел', 'балкон/лоджия', 'количество лифтов']: data[key] = val
            else: data[key] = extract_number(val) if re.search(r"\d", val) else val

    stats_re = re.compile(r"([\d\s]+)\sпросмотр\S*,\s*(\d+)\sза сегодня,\s*(\d+)\sуникаль", re.IGNORECASE)
    st = soup.find(string=stats_re)
    if st:
        m = stats_re.search(st)
        data['Всего просмотров'], data['Просмотров сегодня'], data['Уникальных просмотров'] = (
            extract_number(m.group(1)), extract_number(m.group(2)), extract_number(m.group(3))
        )

    geo = soup.select_one('div[data-name="Geo"]')
    if geo:
        span = geo.find('span', itemprop='name')
        addr = span['content'] if span and span.get('content') else ', '.join(
            a.get_text(strip=True) for a in geo.select('a[data-name="AddressItem"]')
        )
        parts = [s.strip() for s in addr.split(',') if s.strip()]
        data['Адрес'] = ', '.join(parts[-2:]) if len(parts) > 1 else addr
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

    return data


def extract_urls(raw_input: str) -> tuple[list[str], int]:
    urls = re.findall(r'https?://[^\s,;]+', raw_input)
    return urls, len(urls)


async def check_and_update_ad_from_url(url: str, current_price: Any = None, current_is_active: Any = None) -> Dict[str, Any] | None:
    """
    Если ссылка относится к cian.ru — парсит её, определяет актуальность и цену с сайта.
    Обновляет БД вызовом CALL users.update_ad(p_price, p_is_actual, p_code, p_url_id):
      - p_is_actual всегда выставляется по странице: 1 если нет признака неактуальности, 0 если неактуально.
      - p_price передаётся только если отличается от текущего (иначе NULL, чтобы не менять).
    Возвращает dict с актуализированными полями (url, price, is_active), либо None если ссылка не cian.ru.
    """
    if "cian.ru" not in url:
        return None

    # Парсим объявление с сайта
    try:
        session = requests.Session()
        parsed = parse_listing(url, session)
    except Exception:
        # Если парсинг не удался — не блокируем общий процесс
        return None

    # Приводим к ожидаемым ключам для сравнения/обновления
    new_price_raw = parsed.get("Цена_raw") or parsed.get("Цена")
    try:
        new_price = int(new_price_raw) if new_price_raw is not None else None
    except Exception:
        new_price = None
    new_status_text = parsed.get("Статус")
    # Определяем актуальность только если статус распознан
    new_is_active_bool = None
    if isinstance(new_status_text, str):
        s = new_status_text.strip().lower()
        inactive_markers = ("снято", "снят", "неактив", "удален", "удалён", "нет в продаже")
        new_is_active_bool = not any(m in s for m in inactive_markers)
    new_is_actual = (1 if new_is_active_bool else 0) if new_is_active_bool is not None else None

    # Извлекаем url_id — последняя числовая последовательность в ссылке (устойчиво к query/anchor)
    digits = re.findall(r"/(\d+)(?:/|$)", url)
    url_id = int(digits[-1]) if digits else None

    # Формируем JSON для обновления
    # Определяем, есть ли реальное изменение относительно текущих значений из БД
    # Определяем, изменялась ли цена; статус (is_actual) всегда отправляем в БД по странице
    price_changed = False
    curr_price_int = None
    if current_price is not None:
        try:
            curr_price_int = int(current_price)
        except Exception:
            curr_price_int = None
    if curr_price_int is not None and new_price is not None and curr_price_int != int(new_price):
        price_changed = True
    # current_is_active может быть True/False либо 1/0 — используем только для сравнения цены

    # Если url_id найден и есть изменения — вызываем обновление в БД
    if url_id is not None:
        try:
            await call_update_ad(new_price if price_changed else None, new_is_actual, 4, url_id)
        except Exception as e:
            print(f"[DEBUG] call_update_ad failed for url_id={url_id}: {e}")

    # Возвращаем актуализированное представление
    result = {"url": url, "price": new_price}
    if new_is_active_bool is not None:
        result["is_active"] = new_is_active_bool
    return result

async def export_sim_ads(
    request_id: int,
    output_path: str = None
) -> Tuple[BytesIO, int]:
    """
    Формирует Excel вида:
      Адрес1
      Ссылка | Цена | Комнат | Создано | Обновлено | Активно | Владелец
      ... с "Активно" = да/нет, даты в формате DD.MM.YYYY ...
    Автонастройка ширины колонок по содержимому
    """
    groups: List[Dict[str, Any]] = await find_similar_ads_grouped(request_id)

    wb = Workbook()
    ws = wb.active
    ws.title = "Похожие объявления"
    bold = Font(bold=True)

    # Порядок ключей и их заголовки
    ordered_keys = ["url", "price", "rooms", "created", "updated", "is_active", "person_type"]
    header_map = {
        "url": "Ссылка",
        "price": "Цена",
        "rooms": "Комнат",
        "created": "Создано",
        "updated": "Обновлено",
        "is_active": "Активно",
        "person_type": "Владелец",
    }

    for grp in groups:
        addr = grp.get("address", "")
        ads_raw = grp.get("ads", [])

        # — Адрес жирным
        ws.append([addr])
        for cell in ws[ws.max_row]:
            cell.font = bold

        # — Распарсить, если строка
        if isinstance(ads_raw, str):
            try:
                ads_raw = json.loads(ads_raw)
            except json.JSONDecodeError:
                ads_raw = []

        # — Сформировать список dict
        ads: List[Dict[str, Any]] = []
        for ad in ads_raw:
            if isinstance(ad, dict):
                ads.append(ad)
            else:
                try:
                    ads.append(dict(ad))
                except Exception:
                    continue

        if not ads:
            ws.append([])  # разделитель
            continue

        # — Заголовки столбцов
        headers = [header_map[k] for k in ordered_keys]
        ws.append(headers)
        for cell in ws[ws.max_row]:
            cell.font = bold

        # — Данные по объявлениям (с проверкой и возможным обновлением из сайта для ссылок cian)
        for ad in ads:
            # DEBUG: проверка обработки объявлений
            try:
                dbg_url = ad.get("url")
                dbg_price = ad.get("price")
                dbg_is_active = ad.get("is_active")
                print(f"[DEBUG] check ad url={dbg_url!r} price={dbg_price!r} is_active={dbg_is_active!r}")
            except Exception:
                pass
            # Если ссылка — на cian.ru, проверяем актуальные данные и при необходимости обновляем в БД
            url_val = ad.get("url")
            if isinstance(url_val, str) and "cian.ru" in url_val:
                updated = await check_and_update_ad_from_url(
                    url_val,
                    current_price=ad.get("price"),
                    current_is_active=ad.get("is_active"),
                )
                if updated:
                    # Сравниваем с тем, что пришло из БД, и при отличии — подменяем значения в текущем кортеже
                    if updated.get("price") is not None and updated.get("price") != ad.get("price"):
                        ad["price"] = updated.get("price")
                    # ВАЖНО: статус активности в Excel берём с сайта, а не из БД
                    if "is_active" in updated:
                        ad["is_active"] = bool(updated.get("is_active"))

            row = []
            for k in ordered_keys:
                val = ad.get(k, "")
                if k == "is_active":
                    row.append("да" if val else "нет")
                elif k in ("created", "updated") and isinstance(val, str):
                    # приводим YYYY-MM-DDTHH:MM:SS к DD.MM.YYYY
                    date_part = val.split('T')[0]
                    parts = date_part.split('-')
                    if len(parts) == 3:
                        row.append(f"{parts[2]}.{parts[1]}.{parts[0]}")
                    else:
                        row.append(val)
                else:
                    row.append(val)
            ws.append(row)

        ws.append([])  # пустая строка между группами

    # Автонастройка ширины колонок
    for column_cells in ws.columns:
        max_length = max(
            len(str(cell.value)) if cell.value is not None else 0
            for cell in column_cells
        )
        col_letter = get_column_letter(column_cells[0].column)
        ws.column_dimensions[col_letter].width = max_length + 2

    # Сохраняем в BytesIO
    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)

    if output_path:
        with open(output_path, "wb") as f:
            f.write(bio.getbuffer())

    return bio, request_id

def extract_photo_urls(soup: BeautifulSoup) -> list[str]:
    """
    Извлекает ссылки на все фотографии из галереи CIAN
    """
    photo_urls = []
    
    try:
        # Ищем галерею по data-name="GalleryInnerComponent"
        gallery = soup.find('div', {'data-name': 'GalleryInnerComponent'})
        if not gallery:
            return photo_urls
        
        # Ищем все изображения в галерее
        # Основные изображения
        images = gallery.find_all('img', src=True)
        for img in images:
            src = img.get('src')
            if src and src.startswith('http') and 'cdn-cian.ru' in src:
                photo_urls.append(src)
        
        # Изображения в background-image (для видео и некоторых фото)
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
        print(f"Ошибка при извлечении фотографий: {e}")
        return []

def generate_html_gallery(listing_urls: list[str], user_id: int, subtitle: str = None) -> str:
    """
    Генерирует красивый HTML документ с галереей фотографий для объявлений
    """
    sess = requests.Session()
    html_parts = []
    
    html_parts.append("""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Подбор недвижимости</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
            .listing { background: white; margin: 20px 0; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            .listing h3 { color: #333; margin-top: 0; margin-bottom: 15px; }
            .listing p { margin: 8px 0; color: #555; }
            .listing strong { color: #333; }
            .main-title { color: #333; margin-bottom: 10px; }
            .subtitle { color: #666; font-size: 18px; margin-bottom: 30px; font-style: italic; }
            .photo-grid { 
                display: grid; 
                grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); 
                gap: 8px; 
                margin: 15px 0; 
                max-height: 600px; 
                overflow-y: auto; 
                padding: 10px;
                border: 1px solid #eee;
                border-radius: 8px;
            }
            .photo-item { position: relative; }
            .photo-item img { 
                width: 100%; 
                height: 140px; 
                object-fit: cover; 
                border-radius: 5px; 
                border: 2px solid transparent;
                transition: border-color 0.2s;
                background: #f8f9fa;
            }
            .photo-item img:hover { 
                border-color: #0066cc;
            }
            .photo-fallback { 
                width: 100%; 
                height: 140px; 
                display: flex; 
                flex-direction: column; 
                justify-content: center; 
                align-items: center;
            }
            .no-photos { color: #666; font-style: italic; }
            .photo-info { 
                background: #f8f9fa; 
                padding: 15px; 
                border-radius: 8px; 
                margin-top: 15px; 
                font-size: 14px;
                border-left: 4px solid #0066cc;
            }
            .photo-info strong { color: #333; }
            .photo-info small { line-height: 1.4; }
            
            /* Мобильная адаптация */
            @media (max-width: 768px) {
                body { margin: 10px; }
                .listing { padding: 15px; margin: 15px 0; }
                .photo-grid { 
                    grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); 
                    gap: 6px; 
                    padding: 8px;
                }
                .photo-item img, .photo-fallback { 
                    height: 120px; 
                }
                .main-title { font-size: 24px; }
                .subtitle { font-size: 16px; }
            }
        </style>
    </head>
    <body>
        <h1 class="main-title">🏠 Подбор недвижимости</h1>
    """)
    
    # Добавляем подзаголовок, если он есть
    if subtitle and subtitle.strip():
        html_parts.append(f'<h2 class="subtitle">{subtitle.strip()}</h2>')
    
    for i, url in enumerate(listing_urls, 1):
        try:
            # Парсим объявление
            listing_data = parse_listing(url, sess)
            
            # Извлекаем фотографии
            soup = BeautifulSoup(requests.get(url, headers=HEADERS).text, 'html.parser')
            photo_urls = extract_photo_urls(soup)
            
            html_parts.append(f"""
            <div class="listing">
                <h3>Вариант #{i}</h3>
            """)
            
            # Добавляем основную информацию
            if 'Комнат' in listing_data and listing_data['Комнат']:
                html_parts.append(f"<p><strong>Комнат:</strong> {listing_data['Комнат']}</p>")
            if 'Цена_raw' in listing_data and listing_data['Цена_raw']:
                html_parts.append(f"<p><strong>Цена:</strong> {listing_data['Цена_raw']:,} ₽</p>")
            
            # Добавляем этаж/этажность
            if 'Этаж' in listing_data and listing_data['Этаж']:
                html_parts.append(f"<p><strong>Этаж:</strong> {listing_data['Этаж']}</p>")
            
            # Добавляем метраж общий
            if 'Общая площадь' in listing_data and listing_data['Общая площадь']:
                html_parts.append(f"<p><strong>Общая площадь:</strong> {listing_data['Общая площадь']} м²</p>")
            
            # Добавляем кухню
            if 'Площадь кухни' in listing_data and listing_data['Площадь кухни']:
                html_parts.append(f"<p><strong>Кухня:</strong> {listing_data['Площадь кухни']} м²</p>")
            
            # Переименовываем "Метро" в "Минут до метро"
            if 'Минут метро' in listing_data and listing_data['Минут метро']:
                html_parts.append(f"<p><strong>Минут до метро:</strong> {listing_data['Минут метро']}</p>")
            
            # Добавляем фотографии
            if photo_urls:
                # Генерируем сетку фотографий (все фото без ограничений)
                photo_grid_html = photo_processor.generate_photo_grid_html([], 'url')
                
                # Заменяем пустую сетку на реальные фотографии
                html_parts.append(f'<div class="photo-grid">')
                for j, photo_url in enumerate(photo_urls):
                    html_parts.append(f"""
                    <div class="photo-item">
                        <img src="{photo_url}" alt="Фото {j+1}" 
                             onerror="this.style.display='none'; this.nextElementSibling.style.display='block';"
                             loading="lazy">
                        <div class="photo-fallback" style="display: none; background: #f0f0f0; border: 1px dashed #ccc; border-radius: 5px; padding: 20px; text-align: center; color: #666;">
                            <div>📷 Фото {j+1}</div>
                            <div style="font-size: 12px; margin-top: 5px;">
                                <a href="{photo_url}" target="_blank" style="color: #0066cc;">Открыть фото</a>
                            </div>
                        </div>
                    </div>
                    """)
                html_parts.append('</div>')
            else:
                html_parts.append('<p class="no-photos">📷 Фотографии не найдены</p>')
            
            html_parts.append('</div>')
            
        except Exception as e:
            html_parts.append(f"""
            <div class="listing">
                <h3>Вариант #{i}</h3>
                <p style="color: red;">Ошибка при парсинге: {str(e)}</p>
            </div>
            """)
    
    html_parts.append("""
    </body>
    </html>
    """)
    
    return ''.join(html_parts)

def generate_html_gallery_embedded(listing_urls: list[str], user_id: int, subtitle: str = None) -> str:
    """
    Генерирует HTML документ с встроенными изображениями в base64 для лучшей совместимости
    """
    sess = requests.Session()
    html_parts = []
    
    html_parts.append("""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Подбор недвижимости</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
            .listing { background: white; margin: 20px 0; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            .listing h3 { color: #333; margin-top: 0; margin-bottom: 15px; }
            .listing p { margin: 8px 0; color: #555; }
            .listing strong { color: #333; }
            .main-title { color: #333; margin-bottom: 10px; }
            .subtitle { color: #666; font-size: 18px; margin-bottom: 30px; font-style: italic; }
            .photo-grid { 
                display: grid; 
                grid-template-columns: repeat(auto-fill, minmax(180px, 1fr)); 
                gap: 8px; 
                margin: 15px 0; 
                max-height: 600px; 
                overflow-y: auto; 
                padding: 10px;
                border: 1px solid #eee;
                border-radius: 8px;
            }
            .photo-item { position: relative; }
            .photo-item img { 
                width: 100%; 
                height: 140px; 
                object-fit: cover; 
                border-radius: 5px; 
                border: 2px solid transparent;
                transition: border-color 0.2s;
            }
            .photo-item img:hover { 
                border-color: #0066cc;
            }
            .no-photos { color: #666; font-style: italic; }
            .photo-info { 
                background: #f8f9fa; 
                padding: 15px; 
                border-radius: 8px; 
                margin-top: 15px; 
                font-size: 14px;
                border-left: 4px solid #0066cc;
            }
            .photo-info strong { color: #333; }
            
            /* Мобильная адаптация */
            @media (max-width: 768px) {
                body { margin: 10px; }
                .listing { padding: 15px; margin: 15px 0; }
                .photo-grid { 
                    grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); 
                    gap: 6px; 
                    padding: 8px;
                }
                .photo-item img { 
                    height: 120px; 
                }
                .main-title { font-size: 24px; }
                .subtitle { font-size: 16px; }
            }
        </style>
    </head>
    <body>
        <h1 class="main-title">🏠 Подбор недвижимости</h1>
    """)
    
    # Добавляем подзаголовок, если он есть
    if subtitle and subtitle.strip():
        html_parts.append(f'<h2 class="subtitle">{subtitle.strip()}</h2>')
    
    for i, url in enumerate(listing_urls, 1):
        try:
            # Парсим объявление
            listing_data = parse_listing(url, sess)
            
            # Извлекаем фотографии
            soup = BeautifulSoup(requests.get(url, headers=HEADERS).text, 'html.parser')
            photo_urls = extract_photo_urls(soup)
            
            html_parts.append(f"""
            <div class="listing">
                <h3>Вариант #{i}</h3>
            """)
            
            # Добавляем основную информацию
            if 'Комнат' in listing_data and listing_data['Комнат']:
                html_parts.append(f"<p><strong>Комнат:</strong> {listing_data['Комнат']}</p>")
            if 'Цена_raw' in listing_data and listing_data['Цена_raw']:
                html_parts.append(f"<p><strong>Цена:</strong> {listing_data['Цена_raw']:,} ₽</p>")
            
            # Добавляем этаж/этажность
            if 'Этаж' in listing_data and listing_data['Этаж']:
                html_parts.append(f"<p><strong>Этаж:</strong> {listing_data['Этаж']}</p>")
            
            # Добавляем метраж общий
            if 'Общая площадь' in listing_data and listing_data['Общая площадь']:
                html_parts.append(f"<p><strong>Общая площадь:</strong> {listing_data['Общая площадь']} м²</p>")
            
            # Добавляем кухню
            if 'Площадь кухни' in listing_data and listing_data['Площадь кухни']:
                html_parts.append(f"<p><strong>Кухня:</strong> {listing_data['Площадь кухни']} м²</p>")
            
            # Переименовываем "Метро" в "Минут до метро"
            if 'Минут метро' in listing_data and listing_data['Минут метро']:
                html_parts.append(f"<p><strong>Минут до метро:</strong> {listing_data['Минут метро']}</p>")
            
            # Добавляем фотографии (встроенные в base64)
            if photo_urls:
                # Обрабатываем фотографии для встроенного HTML (все фото без ограничений)
                processed_photos = photo_processor.process_photos_for_embedded_html(photo_urls)
                
                # Генерируем сетку фотографий
                photo_grid_html = photo_processor.generate_photo_grid_html(processed_photos, 'embedded')
                html_parts.append(photo_grid_html)
            else:
                html_parts.append('<p class="no-photos">📷 Фотографии не найдены</p>')
            
            html_parts.append('</div>')
            
        except Exception as e:
            html_parts.append(f"""
            <div class="listing">
                <h3>Вариант #{i}</h3>
                <p style="color: red;">Ошибка при парсинге: {str(e)}</p>
            </div>
            """)
    
    html_parts.append("""
    </body>
    </html>
    """)
    
    return ''.join(html_parts)

if __name__ == '__main__':
    user_id = 12345
    urls = ["https://www.cian.ru/sale/flat/318826533/"]
    excel = asyncio.run(export_listings_to_excel(urls, user_id, output_path="listings_numeric.xlsx"))
    print("listings_numeric.xlsx создан.")
