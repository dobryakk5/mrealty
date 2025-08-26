# Установка шрифтов для корректного отображения текста "МИЭЛЬ"

## Проблема
На сервере Linux может отсутствовать текст "МИЭЛЬ" на фотографиях из-за отсутствия системных шрифтов.

## Решение

### 1. Автоматический поиск шрифтов
Код автоматически ищет стандартные шрифты в следующем порядке:
- `/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf`
- `/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf`
- `/usr/share/fonts/truetype/ubuntu/Ubuntu-R.ttf`
- Дефолтный шрифт Pillow (если ничего не найдено)

### 2. Установка шрифтов на Ubuntu/Debian
```bash
# Установка стандартных шрифтов
sudo apt update
sudo apt install fonts-dejavu fonts-liberation fonts-ubuntu

# Проверка установки
fc-list | grep -i "dejavu\|liberation\|ubuntu"
```

### 3. Установка шрифтов на CentOS/RHEL
```bash
# Установка стандартных шрифтов
sudo yum install dejavu-fonts liberation-fonts

# Или для новых версий
sudo dnf install dejavu-fonts liberation-fonts
```

### 4. Установка шрифтов на Alpine Linux
```bash
# Установка стандартных шрифтов
apk add font-dejavu font-liberation
```

### 5. Проверка доступности шрифтов
```bash
# Проверка установленных шрифтов
fc-list

# Поиск конкретного шрифта
fc-list | grep -i "dejavu"
fc-list | grep -i "liberation"
```

### 6. Ручная установка шрифтов
Если стандартные пакеты недоступны, можно установить шрифты вручную:

```bash
# Создание директории для шрифтов
sudo mkdir -p /usr/share/fonts/truetype/custom

# Копирование TTF файлов
sudo cp your-font.ttf /usr/share/fonts/truetype/custom/

# Обновление кэша шрифтов
sudo fc-cache -fv
```

### 7. Проверка в Python
```python
from PIL import ImageFont

# Проверка доступности шрифтов
font_paths = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "/usr/share/fonts/truetype/ubuntu/Ubuntu-R.ttf"
]

for path in font_paths:
    try:
        font = ImageFont.truetype(path, 20)
        print(f"✅ Шрифт доступен: {path}")
    except:
        print(f"❌ Шрифт недоступен: {path}")
```

## Логирование
В коде добавлено логирование выбора шрифта:
- `✅ Используется шрифт: /path/to/font.ttf` - успешная загрузка
- `⚠️ Используется дефолтный шрифт (может быть некрасивым)` - fallback на дефолтный

## Рекомендации
1. **Установите стандартные шрифты** на сервере
2. **Проверьте логи** для диагностики проблем
3. **Используйте системные шрифты** вместо дефолтных для лучшего качества
