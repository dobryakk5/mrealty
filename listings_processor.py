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
from db_handler import save_listings, find_similar_ads_grouped

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
    data['Статус'] = 'Снято' if soup.find(string=re.compile(r"Объявление снято", re.IGNORECASE)) else 'Активно'
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

        # — Данные по объявлениям
        for ad in ads:
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

if __name__ == '__main__':
    user_id = 12345
    urls = ["https://www.cian.ru/sale/flat/318826533/"]
    excel = asyncio.run(export_listings_to_excel(urls, user_id, output_path="listings_numeric.xlsx"))
    print("listings_numeric.xlsx создан.")
