import time
import re
import pandas as pd
from seleniumbase import Driver
from bs4 import BeautifulSoup

# === настройки ===
LISTING_URLS = [
    "https://www.cian.ru/sale/flat/319479127/",
    "https://www.cian.ru/sale/flat/319869461/",
    "https://www.cian.ru/sale/flat/309583631/"
]

# === функция извлечения числового значения из строки ===
def extract_number(text):
    if not text or text == '—':
        return None
    cleaned = re.sub(r"[^\d.,]", "", text)
    cleaned = cleaned.replace(' ', '').replace('\u00A0', '')
    cleaned = cleaned.replace(',', '.')
    try:
        if '.' in cleaned:
            return float(cleaned)
        return int(cleaned)
    except ValueError:
        return None

# === форматирование цены в денежный вид без дробных и валюты ===
def format_price(value):
    if pd.isna(value) or value is None:
        return ''
    v = int(round(value))
    return f"{v:,}".replace(',', ' ')

# === функция парсинга одного объявления ===
def parse_listing(url, driver):
    driver.open(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.get_page_source(), 'html.parser')

    data = {'URL': url}

    # Из заголовка получаем количество комнат
    title_tag = soup.find('h1')
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        m_rooms = re.search(r"(\d+)[^\d]*[-–]?комн", title_text)
        data['Комнат'] = extract_number(m_rooms.group(1)) if m_rooms else None
    else:
        data['Комнат'] = None

    # Цена (новостройка/вторичка)
    price_text = None
    new_price_span = soup.select_one(
        'div[data-name="NewbuildingPriceInfo"] [data-testid="price-amount"] span'
    )
    if new_price_span:
        price_text = new_price_span.get_text(' ', strip=True)
    else:
        price_span = soup.select_one(
            'div[data-name="AsideGroup"] div[data-name="PriceInfo"] [data-testid="price-amount"] span'
        )
        if price_span:
            price_text = price_span.get_text(' ', strip=True)
    data['Цена_raw'] = extract_number(price_text)

    # Адрес (убираем город Москва)
    address = None
    geo_div = soup.select_one('div[data-name="Geo"]')
    if geo_div:
        addr_span = geo_div.find('span', attrs={'itemprop': 'name'})
        if addr_span and addr_span.get('content'):
            address = addr_span['content'].strip()
        else:
            parts = [a.get_text(strip=True) for a in geo_div.select('a[data-name="AddressItem"]')]
            if parts:
                address = ', '.join(parts)
        if address and address.startswith('Москва,'):
            address = address.split(',', 1)[1].strip()
    data['Адрес'] = address

    # Метки
    labels = []
    for child in soup.select('div[data-name="LabelsLayoutNew"] > span'):
        text_spans = child.find_all('span')
        if text_spans:
            lbl = text_spans[-1].get_text(strip=True)
            if lbl:
                labels.append(lbl)
    data['Метки'] = '; '.join(labels) if labels else None

    # Обновлено
    updated = None
    upd_wrapper = soup.find(string=re.compile(r"Обновлено:"))
    if upd_wrapper:
        m = re.search(r"Обновлено:\s*((?:\d{1,2}\s\w+|сегодня|вчера)\s*,\s*\d{1,2}:\d{2})", upd_wrapper)
        if m:
            updated = m.group(1)
    data['Обновлено'] = updated

    # Статистика просмотров
    total, today, unique = None, None, None
    stats_pattern = re.compile(
        r"([\d\s]+)\sпросмотр\S*,\s*(\d+)\sза сегодня,\s*(\d+)\sуникаль\S*",
        re.IGNORECASE
    )
    stats = soup.find(string=stats_pattern)
    if stats:
        s = stats.strip()
        m = stats_pattern.search(s)
        if m:
            total = extract_number(m.group(1))
            today = extract_number(m.group(2))
            unique = extract_number(m.group(3))
    data['Всего просмотров'] = total
    data['Просмотров сегодня'] = today
    data['Уникальных просмотров'] = unique

    # Этаж
    floor = None
    for item in soup.select('[data-name="ObjectFactoidsItem"]'):
        spans = item.find_all('span')
        if len(spans) >= 2 and 'Этаж' in spans[0].get_text():
            floor = extract_number(spans[1].get_text(strip=True))
            break
    data['Этаж'] = floor

    # Сводная информация без названия блока
    summary = soup.select_one('[data-name="OfferSummaryInfoLayout"]')
    if summary:
        for group in summary.select('[data-name="OfferSummaryInfoGroup"]'):
            for item in group.select('[data-name="OfferSummaryInfoItem"]'):
                p_tags = item.find_all('p')
                if len(p_tags) >= 2:
                    key = p_tags[0].get_text(strip=True)
                    val_text = p_tags[1].get_text(strip=True)
                    if key in ['Общая площадь', 'Жилая площадь', 'Площадь кухни', 'Высота потолков']:
                        data[key] = extract_number(val_text)
                    else:
                        data[key] = val_text
    return data

# === главный запуск ===
def main():
    driver = Driver(uc=True, headless=True)
    try:
        all_data = []
        for url in LISTING_URLS:
            print(f"Парсим: {url}")
            record = parse_listing(url, driver)
            all_data.append(record)

                # Создаём DataFrame и сортируем по сырой цене
        df = pd.DataFrame(all_data)
        df = df.sort_values(by='Цена_raw', ascending=True)

        # Добавляем цену за квадратный метр (целое число)
        df['Цена за м2'] = df['Цена_raw'] // df['Общая площадь']

        # Преобразуем цену в формат денежных строк
        df['Цена'] = df['Цена_raw'].apply(format_price)

        # Перемещаем столбец "Цена" на третью позицию (индекс 2)
        price_col = df.pop('Цена')
        df.insert(2, 'Цена', price_col)
        # Затем вставляем цену за м2 после цены
        sqm_col = df.pop('Цена за м2')
        df.insert(3, 'Цена за м2', sqm_col)

        # Убираем вспомогательное поле Цена_raw
        df.drop(columns=['Цена_raw'], inplace=True)

        # Сохраняем в Excel
        output_file = 'listings.xlsx'
        df.to_excel(output_file, index=False)
        print(f"Данные сохранены в '{output_file}'")

    finally:
        driver.quit()

if __name__ == "__main__":
    main()
