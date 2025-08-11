import requests

url = "https://www.cian.ru/cian-api/site/v1/get-regions/"
headers = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/115.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'ru-RU,ru;q=0.9',
}

response = requests.get(url, headers=headers)
data = response.json()
# paste your `data` (response.json()) into the variable `data`
# например: data = response.json()

import json
import re

# вставьте сюда ваш response.json():
# data = response.json()
# либо прямо paste: data = {...}
# -----------------------------------

def find_regions_list(obj):
    """Найти первый вложенный список словарей, похожих на список регионов."""
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            return obj
    if isinstance(obj, dict):
        for v in obj.values():
            res = find_regions_list(v)
            if res:
                return res
    return None

def tokens_of_region(region):
    text = " ".join(
        str(region.get(k, "") or "") 
        for k in ("name", "fullName", "displayName", "fullNamePrepositional")
    ).lower()
    return re.findall(r'\w+', text, flags=re.UNICODE)

regions_list = find_regions_list(data)
if not regions_list:
    raise RuntimeError("Не удалось найти список регионов в data — вставьте полный JSON response.json().")

# Найдём записи, которые явно соответствуют городу "Москва"
moscow_entries = []
for r in regions_list:
    toks = tokens_of_region(r)
    if "москва" in toks:          # точное слово "москва" в любом текстовом поле
        moscow_entries.append(r)

# В ряде ответов Москва может присутствовать как id=1 — добавим запасной поиск по id=1, если не нашли:
if not moscow_entries:
    for r in regions_list:
        if r.get("id") == 1:
            moscow_entries.append(r)
            break

if not moscow_entries:
    print("Не найдено явной записи 'Москва' в полученном списке регионов.")
else:
    out = []
    # для каждой найденной записи собираем её и связанные районы
    for me in moscow_entries:
        mid = me.get("id")
        item = {"moscow_region": me, "districts_from_field": [], "districts_by_parentId": []}

        # 1) если есть явный ключ 'districts' — добавим
        if me.get("districts"):
            item["districts_from_field"] = me["districts"]

        # 2) ищем в общем списке элементы с parentId == mid
        if mid is not None:
            kids = [r for r in regions_list if r.get("parentId") == mid]
            item["districts_by_parentId"] = kids

        out.append(item)

    # Выводим как JSON (чтобы видеть кириллицу)
    print(json.dumps(out, ensure_ascii=False, indent=2))
