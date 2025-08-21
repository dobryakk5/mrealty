# start_handlers.py
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
import textwrap

async def on_start(message: Message):
    reply_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📘 инструкция")]],resize_keyboard=True)
    
    await message.answer("Здравствуйте!", reply_markup=reply_kb)
    await message.answer(
        textwrap.dedent("""\
            🏠 Присылайте список ссылок на объявления CIAN —
            сделаем Excel-отчёт по этим квартирам.
            
            🖼️ Для просмотра фотографий напишите "подбор" + ссылки.
            
            💡 Примеры:
            • Просто ссылки → Excel-отчет
            • "подбор ссылка1 ссылка2 ссылка3" → HTML с фото для всех ссылок
            • "подбор" в любом месте сообщения → HTML для всех ссылок
        """)
        ,reply_markup=reply_kb
    )
