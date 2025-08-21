#!/bin/bash

echo "🧹 Быстрая очистка процессов Chromium..."

# Находим и убиваем процессы Chromium
echo "🔍 Ищем процессы Chromium..."
CHROME_PIDS=$(ps aux | grep -E "(chromium|chrome)" | grep -v grep | awk '{print $2}')

if [ -z "$CHROME_PIDS" ]; then
    echo "✅ Процессы Chromium не найдены"
    exit 0
fi

echo "📊 Найдено процессов: $(echo $CHROME_PIDS | wc -w)"
echo "💾 Потребление памяти:"
ps aux | grep -E "(chromium|chrome)" | grep -v grep | awk '{sum+=$6} END {print "   Total RSS:", sum/1024, "MB"}'

echo ""
echo "🧹 Убиваем процессы..."

# Убиваем каждый процесс
for pid in $CHROME_PIDS; do
    echo "   Убиваем PID $pid..."
    kill -9 $pid 2>/dev/null
done

echo ""
echo "✅ Очистка завершена"
echo "💡 Перезапустите парсер при необходимости"
