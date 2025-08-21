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
            
            🖼️ Для просмотра фотографий напишите "подбор" + описание + ссылки.
            🔗 Для встроенных фото (лучше на мобильных): "подбор встроенные" + ссылки.
            
            💡 Примеры:
            • Просто ссылки → Excel-отчет
            • "подбор 2-комнатные квартиры ссылка1 ссылка2" → HTML с фото
            • "подбор встроенные ссылка1 ссылка2" → HTML с встроенными фото
            • "подбор" в любом месте сообщения → HTML для всех ссылок
        """)
        ,reply_markup=reply_kb
    )
