import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from io import BytesIO
import asyncio

# Асинхронное сохранение в БД
from db_handler import save_listings

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


def format_price(raw):
    if raw is None or pd.isna(raw):
        return ''
    return f"{int(round(raw)):,}".replace(',', ' ')


def parse_listing(url: str, session: requests.Session) -> dict:
    resp = session.get(url, headers=HEADERS)
    resp.encoding = 'utf-8'
    soup = BeautifulSoup(resp.text, 'html.parser')

    data = {'URL': url}
    # Статус объявления
    data['Статус'] = 'Снято' if soup.find(string=re.compile(r"Объявление снято", re.IGNORECASE)) else 'Активно'
    # Метки
    labels = [span.get_text(strip=True) for span in soup.select('div[data-name="LabelsLayoutNew"] > span span:last-of-type')]
    data['Метки'] = '; '.join(labels) if labels else None
    # Комнатность из заголовка
    h1 = soup.find('h1')
    if h1:
        m = re.search(r"(\d+)[^\d]*[-–]?комн", h1.get_text())
        if m:
            data['Комнат'] = extract_number(m.group(1))
    # Цена
    price_el = (
        soup.select_one('[data-name="NewbuildingPriceInfo"] [data-testid="price-amount"] span')
        or soup.select_one('[data-name="AsideGroup"] [data-testid="price-amount"] span')
    )
    if price_el:
        data['Цена_raw'] = extract_number(price_el.get_text())

    # Обработка основных фактов в сводном блоке
    summary = soup.select_one('[data-name="OfferSummaryInfoLayout"]')
    if summary:
        for item in summary.select('[data-name="OfferSummaryInfoItem"]'):
            ps = item.find_all('p')
            if len(ps) < 2:
                continue
            key = ps[0].get_text(strip=True)
            val = ps[1].get_text(strip=True)
            kl = key.lower().strip()
            # Этаж
            if kl == 'этаж':
                data['Этаж'] = val
                print(f"DEBUG Этаж (сводка): '{val}'")
                continue
            # Текстовые поля, сохраняем как есть
            if kl in ['санузел', 'балкон/лоджия', 'количество лифтов']:
                data[key] = val
                continue
            # Остальные числовые или текстовые поля
            if re.search(r"\d", val):
                data[key] = extract_number(val) or val
            else:
                data[key] = val

    # Дополнительные факты в блоке ObjectFactoids
    cont = soup.find('div', {'data-name': 'ObjectFactoids'})
    if cont:
        lines = cont.get_text(separator='\n', strip=True).split('\n')
        for i in range(0, len(lines)-1, 2):
            key = lines[i].strip()
            val = lines[i+1].strip()
            kl = key.lower().strip()
            if kl == 'этаж' and 'Этаж' not in data:
                data['Этаж'] = val
                print(f"DEBUG Этаж (ObjectFactoids): '{val}'")
            elif kl in ['санузел', 'балкон/лоджия', 'количество лифтов']:
                data[key] = val
            else:
                if re.search(r"\d", val):
                    data[key] = extract_number(val) or val
                else:
                    data[key] = val

    # Статистика просмотров
    stats_re = re.compile(r"([\d\s]+)\sпросмотр\S*,\s*(\d+)\sза сегодня,\s*(\d+)\sуникаль", re.IGNORECASE)
    st = soup.find(string=stats_re)
    if st:
        m = stats_re.search(st)
        data['Всего просмотров'] = extract_number(m.group(1))
        data['Просмотров сегодня'] = extract_number(m.group(2))
        data['Уникальных просмотров'] = extract_number(m.group(3))

    # Адрес
    geo = soup.select_one('div[data-name="Geo"]')
    if geo:
        span = geo.find('span', itemprop='name')
        if span and span.get('content'):
            addr = span['content']
        else:
            addr = ', '.join(a.get_text(strip=True) for a in geo.select('a[data-name="AddressItem"]'))
        parts = [s.strip() for s in addr.split(',') if s.strip()]
        data['Адрес'] = ', '.join(parts[-2:]) if len(parts) > 1 else addr

    return data

async def export_listings_to_excel(listing_urls: list[str], output_path: str = None) -> BytesIO:
    sess = requests.Session()
    rows = []
    for url in listing_urls:
        try:
            rows.append(parse_listing(url, sess))
        except Exception as e:
            print(f"Ошибка при парсинге {url}: {e}")

    # Сохраняем в БД
    await save_listings(rows)

    df = pd.DataFrame(rows)
    print("DEBUG COLUMNS:", df.columns.tolist())

    if 'Цена_raw' in df.columns:
        df['Цена'] = df['Цена_raw'].apply(format_price)
        df = df.sort_values('Цена_raw')
        df.drop('Цена_raw', axis=1, inplace=True)

    ordered = [
        'Комнат', 'Тип жилья', 'Общая площадь', 'Жилая площадь', 'Площадь кухни',
        'Санузел', 'Балкон/лоджия', 'Вид из окон', 'Ремонт', 'Этаж', 'Год постройки',
        'Строительная серия', 'Тип дома', 'Тип перекрытий', 'Количество лифтов', 'Парковка',
        'Подъезды', 'Отопление', 'Аварийность', 'Газоснабжение',
        'Всего просмотров', 'Просмотров сегодня', 'Уникальных просмотров',
        'Адрес', 'Метки', 'Статус', 'URL'
    ]
    df = df[[c for c in ordered if c in df.columns]]

    bio = BytesIO()
    df.to_excel(bio, index=False)
    bio.seek(0)

    if output_path:
        with open(output_path, 'wb') as f:
            f.write(bio.getbuffer())
        write_styled_excel_file(output_path)

    return bio

# Остальные вспомогательные функции без изменений

def write_styled_excel_file(filename: str):
    wb = load_workbook(filename)
    ws = wb.active
    gray = Font(color="808080")
    bold = Font(bold=True)
    for cell in ws[1]:
        cell.font = bold
    status_col = get_column_letter(ws.max_column)
    for row in range(2, ws.max_row+1):
        if ws[f"{status_col}{row}"].value == 'Снято':
            for col in range(1, ws.max_column+1):
                ws[f"{get_column_letter(col)}{row}"].font = gray
    wb.save(filename)


def extract_urls(raw_input: str) -> tuple[list[str], int]:
    urls = re.findall(r'https?://[^\s,;]+', raw_input)
    return urls, len(urls)

if __name__ == '__main__':
    urls = ["https://www.cian.ru/sale/flat/318826533/"]
    excel = asyncio.run(export_listings_to_excel(urls, output_path="listings.xlsx"))
    print("listings.xlsx создан. Колонки:", pd.read_excel("listings.xlsx").columns.tolist())
