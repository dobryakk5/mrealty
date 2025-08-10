# handlers_common.py
from aiogram.types import Message
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types.input_file import BufferedInputFile
from aiogram import Bot, Dispatcher
#from db_handler import update_dictionary, get_today_purchases, get_user_purchases, update_last_purchase_field, get_last_purchase, delete_last_purchase
from start_handlers import on_start
import pandas as pd
import textwrap
#import matplotlib.pyplot as plt
from io import BytesIO
from datetime import datetime
import asyncpg
from asyncpg.exceptions import UniqueViolationError
import logging
#import uuid
#import redis
import os
from dotenv import load_dotenv


# Инициализация бота
load_dotenv()
BOT_TOKEN = os.getenv("API_TOKEN")
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


# Инициализация логгера
logger = logging.getLogger(__name__)


async def process_user_input(
    raw_text: str, 
    message: Message,
    handle_new_expense_func
):
    lower = raw_text.lower().strip()

    # Кнопка «Инструкция»
    if lower == "📘 инструкция":
        await message.answer(
        textwrap.dedent("""\
            💸 Добавить новую оплату: напиши «категория подкатегория цена».
            🧾 Загрузить позиции с чека: отправь фото QR-кода с чека
            🎙️ Загрузить простую транзакцию голосом: запиши голосовое.
            🛠️ Исправить поле в последней записи: введи "удали" или выбери новое значение
              – «Категория НовоеЗначение»
              – «Подкатегория НовоеЗначение»
              – «Цена НовоеЗначение»
            📋 Показать список сегодняшних оплат: меню «Список» 
            🔢 Выгрузить все оплаты в Excel: меню «Таблица» 
            💰 Добавить свои доходы: напиши "доход консультация 49505"
             - слово "доход" в начале говорит боту писать консультацию в доходы.

            Обновить бота 🔄: напиши /start 
        """)
        )
        return

    if lower == "📈 графики":
        await message.answer(
            "📊 Что показать?",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="🔘 Круг по категориям")],
                    [KeyboardButton(text="📊 Накопительно категория/день")],
                    [KeyboardButton(text="📊 Ежедневно категория/день")],
                    [KeyboardButton(text="🏠 Главное меню")]
                ],
                resize_keyboard=True
            )
        )
        return

    if lower == "💰 доходы":
        from db_handler import get_user_incomes_days
        rows = await get_user_incomes_days(message.from_user.id, 30)
        if not rows:
            return await message.answer("Доходов за последние 30 дней нет.")
        lines = [
            f"{r['source'][:25]:<25} {int(r['amount']):>10,}".replace(",", ".")
            for r in rows
        ]
        total = sum(int(r['amount']) for r in rows)
        lines.append("")
        lines.append(f"{'Итого за 30 дней:':<25} {total:>10,}".replace(",", "."))
        await message.answer(f"<pre>{chr(10).join(lines)}</pre>", parse_mode="HTML")
        return

    if lower == "🔘 круг по категориям":
        await show_pie_chart(message.from_user.id, message)
        return

    if lower == "📊 накопительно категория/день":
        await show_bar_chart_by_day(message.from_user.id, message)
        return

    if lower == "📊 ежедневно категория/день":
        await show_daily_bar_chart(message.from_user.id, message)
        return

    if lower == "🏠 главное меню":
        await on_start(message)
        return
    
    if lower in ("🚪 кабинет", "кабинет"):
        user_id = str(message.from_user.id)
        token = str(uuid.uuid4())

        r.setex(f"dash_token:{token}", 300, user_id)
        dash_url = f"https://ai5.space/auth?token={token}"

        # Создаем клавиатуру с кнопкой
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔓 Войти в кабинет", url=dash_url)]
        ])

        await message.answer(
            "🔒 Ваша ссылка для входа (действительна 5 минут):\n\n",
            reply_markup=keyboard,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
        return

    if lower == "📄 список":
        await show_today_purchases(message.from_user.id, message)
        return
    
    if lower == "удали":
        success = await delete_last_purchase(message.from_user.id)
        if success:
            await message.answer("✅ Последняя запись успешно удалена.")
        else:
            await message.answer("⚠️ Нет записей для удаления.")
        return


    if lower == "🔢 таблица":
        import os
        filename = "Fin_a_bot.xlsx"
        await export_purchases_to_excel(message.from_user.id, filename)
        with open(filename, 'rb') as f:
            file_data = f.read()
        await message.answer_document(BufferedInputFile(file_data, filename))
        os.remove(filename)
        return

    correction_commands = {
        "категория": "category",
        "подкатегория": "subcategory",
        "цена": "price"
    }

    for prefix, field in correction_commands.items():
        if lower.startswith(prefix):
            parts = raw_text.split(maxsplit=1)
            if len(parts) < 2 or not parts[1].strip():
                return await message.answer(f"❌ Укажите значение после «{prefix.capitalize()}»")
            return await handle_correction(field, parts[1].strip(), message)

    await handle_new_expense_func(raw_text, message)

