# start_handlers.py
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
import textwrap

async def on_start(message: Message):
    reply_kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📘 инструкция")]],resize_keyboard=True)
    
    await message.answer("Здравствуйте!", reply_markup=reply_kb)
    await message.answer(
        textwrap.dedent("""\
            Присылайте список ЦИАН ссылок —
            сделаем excel отчёт по этим квартирам.
        """)
        ,reply_markup=reply_kb
    )
