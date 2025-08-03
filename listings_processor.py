import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from io import BytesIO

# Заголовки для запросов
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
    c = re.sub(r"[^\d.,]", "", text).replace('\u00A0', '').replace(' ', '').replace(',', '.')
    try:
        return float(c) if '.' in c else int(c)
    except ValueError:
        return None


def format_price(raw):
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return ''
    return f"{int(round(raw)):,}".replace(',', ' ')


def truncate_lifts(val: str) -> str:
    parts = [part.strip() for part in re.split(r"[,;]", val) if part.strip()]
    truncated = [part[:3] for part in parts[:2]]
    return ', '.join(truncated)


def parse_listing(url: str, session: requests.Session) -> dict:
    resp = session.get(url, headers=HEADERS)
    resp.encoding = 'utf-8'
    soup = BeautifulSoup(resp.text, 'html.parser')
    data = {'URL': url}

    status_text = soup.find(string=re.compile(r"Объявление снято с публикации", re.IGNORECASE))
    data['Статус'] = 'Снято' if status_text else 'Активно'

    labels = []
    for child in soup.select('div[data-name="LabelsLayoutNew"] > span'):
        spans = child.find_all('span')
        if spans:
            lbl = spans[-1].get_text(strip=True)
            if lbl:
                labels.append(lbl)
    data['Метки'] = '; '.join(labels) if labels else None

    h1 = soup.find('h1')
    if h1:
        m = re.search(r"(\d+)[^\d]*[-–]?комн", h1.get_text())
        data['Комнат'] = extract_number(m.group(1)) if m else None

    price_el = (
        soup.select_one('div[data-name="NewbuildingPriceInfo"] [data-testid="price-amount"] span')
        or soup.select_one('div[data-name="AsideGroup"] div[data-name="PriceInfo"] [data-testid="price-amount"] span')
    )
    data['Цена_raw'] = extract_number(price_el.get_text() if price_el else None)

    summary = soup.select_one('[data-name="OfferSummaryInfoLayout"]')
    if summary:
        for item in summary.select('[data-name="OfferSummaryInfoItem"]'):
            ps = item.find_all('p')
            if len(ps) < 2:
                continue
            key = ps[0].get_text(strip=True)
            val = ps[1].get_text(strip=True)
            if key == 'Санузел':
                data[key] = val
            elif key.lower() == 'количество лифтов':
                data[key] = truncate_lifts(val)
            else:
                data[key] = extract_number(val) if re.search(r"\d", val) else val

    cont = soup.find('div', {'data-name': 'ObjectFactoids'})
    if cont:
        lines = cont.get_text(separator='\n', strip=True).split('\n')
        for i in range(0, len(lines)-1, 2):
            key = lines[i].strip()
            val = lines[i+1].strip()
            if not key:
                continue
            if key.lower() == 'количество лифтов':
                data[key] = truncate_lifts(val)
            else:
                data[key] = val
    
    stats_pattern = re.compile(
        r"([\d\s]+)\sпросмотр\S*,\s*(\d+)\sза сегодня,\s*(\d+)\sуникаль\S*",
        re.IGNORECASE
    )
    stats_text = soup.find(string=stats_pattern)
    if stats_text:
        m = stats_pattern.search(stats_text)
        data['Всего просмотров'] = extract_number(m.group(1))
        data['Просмотров сегодня'] = extract_number(m.group(2))
        data['Уникальных просмотров'] = extract_number(m.group(3))

    geo_div = soup.select_one('div[data-name="Geo"]')
    address = None
    if geo_div:
        addr_span = geo_div.find('span', attrs={'itemprop': 'name'})
        if addr_span and addr_span.get('content'):
            address = addr_span['content'].strip()
        else:
            parts = [a.get_text(strip=True) for a in geo_div.select('a[data-name="AddressItem"]')]
            address = ', '.join(parts) if parts else None
        # Оставляем только последние два сегмента адреса
        if address:
            segs = [seg.strip() for seg in address.split(',') if seg.strip()]
            address = ', '.join(segs[-2:]) if len(segs) > 1 else address
    data['Адрес'] = address

    return data


def export_listings_to_excel(listing_urls: list, output_path: str = None) -> BytesIO:
    sess = requests.Session()
    rows = []
    for url in listing_urls:
        try:
            rows.append(parse_listing(url, sess))
        except Exception as e:
            print(f"Ошибка при парсинге {url}: {e}")

    df = pd.DataFrame(rows)
    if 'Цена_raw' in df.columns:
        df['Цена'] = df['Цена_raw'].apply(format_price)
        df = df.sort_values(by='Цена_raw', ascending=True)
        df.drop(columns=['Цена_raw'], inplace=True)

    base = ['Комнат', 'Цена', 'Всего просмотров', 'Просмотров сегодня', 'Уникальных просмотров', 'Этаж']
    others = [c for c in df.columns if c not in base + ['Тип жилья', 'Метки', 'Статус', 'Адрес', 'URL']]
    cols = base + others + ['Тип жилья', 'Метки', 'Статус', 'Адрес', 'URL']
    df = df[[c for c in cols if c in df.columns]]

    bio = BytesIO()
    df.to_excel(bio, index=False)
    bio.seek(0)

    if output_path:
        with open(output_path, 'wb') as f:
            f.write(bio.getbuffer())
        write_styled_excel_file(output_path)

    return bio


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


def extract_urls(raw: str) -> tuple[list[str], int]:
    """Находит все вхождения ссылок, начинающихся с http или https, и возвращает их список и количество."""
    # Ищем подстроки, начинающиеся с http:// или https:// до пробела или разделителя
    urls = re.findall(r'https?://[^\s,;]+' , raw)
    return urls, len(urls)

if __name__ == '__main__':
    raw_input = input('Введите строки с URL-адресами (одно или несколько), возможно с любыми разделителями и текстом: ')
    listing_urls, count = extract_urls(raw_input)
    print(f"Принято ссылок: {count}")
    if listing_urls:
        excel = export_listings_to_excel(listing_urls)
        with open('listings.xlsx', 'wb') as f:
            f.write(excel.getbuffer())
        print('Файл listings.xlsx успешно создан.')
