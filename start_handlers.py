# start_handlers.py
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
import textwrap

async def start_handler(message: Message):
    """Обработчик команды /start"""
    welcome_text = """
🏠 Добро пожаловать в бот для анализа недвижимости!

📋 **Как использовать:**

1️⃣ **Excel-отчет** - просто отправьте ссылки на объявления CIAN
   Пример: `https://www.cian.ru/rent/flat/123456/`

2️⃣ **Подбор с встроенными фото** - напишите "подбор" + ссылки
   Пример: `подбор https://www.cian.ru/rent/flat/123456/`
   ✅ Фото встроены в HTML, работают на всех устройствах

3️⃣ **Подбор с обычными ссылками** - напишите "подбор-" + ссылки  
   Пример: `подбор- https://www.cian.ru/rent/flat/123456/`
   ⚠️ Фото по ссылкам, могут не работать на мобильных

💡 **Совет:** Используйте "подбор" для лучшей совместимости на всех устройствах!
    """
    
    await message.answer(welcome_text)
