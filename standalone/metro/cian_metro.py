import xml.etree.ElementTree as ET
import json
import csv
from pathlib import Path
import requests

# --- Настройки: укажите либо url, либо path_to_file, либо xml_string ---
url = 'https://www.cian.ru/metros-moscow-v2.xml'
path_to_file = None
xml_string = None

# пример: xml_string = """<metro>...ваш XML...</metro>"""

# --- загрузка XML ---
if url:
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    content = resp.content
elif path_to_file:
    content = Path(path_to_file).read_bytes()
elif xml_string:
    content = xml_string.encode('utf-8')
else:
    raise RuntimeError("Укажите url или path_to_file или xml_string с содержимым XML.")

root = ET.fromstring(content)

# --- собрать линии ---
lines = []
# найдём все теги <line> в любом месте
for line in root.findall(".//line"):
    line_id = line.get("id")
    line_name = line.get("name") or ""
    line_color = line.get("color") or ""

    # внутри линии собираем теги <location> и <station> (подстраховка)
    stations = []
    for tag in ("location", "station"):
        for loc in line.findall(".//" + tag):
            sid = loc.get("id") or loc.get("sid") or loc.get("station_id")
            # название может быть в тексте элемента или в атрибуте name
            sname = (loc.text or "").strip()
            if not sname:
                sname = loc.get("name") or loc.get("title") or ""
            if sid:
                try:
                    sid = int(sid)
                except Exception:
                    pass
            stations.append({"id": sid, "name": sname})

    # удалить дубли (если одинаковые встретились дважды)
    seen = set()
    uniq_stations = []
    for s in stations:
        key = (str(s["id"]), s["name"])
        if key not in seen:
            seen.add(key)
            uniq_stations.append(s)

    lines.append({
        "line_id": line_id,
        "line_name": line_name,
        "line_color": line_color,
        "stations": uniq_stations
    })

# --- вывод JSON ---
print(json.dumps(lines, ensure_ascii=False, indent=2))

# --- сохранить CSV: одна строка = одна станция ---
csv_path = Path("stations.csv")
with csv_path.open("w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["line_id", "line_name", "station_id", "station_name"])
    for ln in lines:
        for st in ln["stations"]:
            writer.writerow([ln["line_id"], ln["line_name"], st["id"], st["name"]])

print(f"\nСохранено в {csv_path.resolve()} — всего линий: {len(lines)}, станций (в CSV):", 
      sum(len(ln["stations"]) for ln in lines))
