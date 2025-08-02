import re
import logging
from aiogram.types import Message
#from handlers_common import process_user_input, show_parser_result
#from parse_expense import parse_expense_t
#from db_handler import save_expense, save_income
from listings_processor import export_listings_to_excel
from aiogram.types.input_file import BufferedInputFile

logger = logging.getLogger(__name__)

async def handle_text_message(message: Message):
    text = message.text.strip()

    # Если пользователь нажал «Инструкция»
    if text == "📘 Инструкция":
        return await message.answer(
            "Чтобы получить отчёт по квартирам, пришлите ссылки ЦИАН вида\n"
            "https://www.cian.ru/sale/flat/320223651\n"
            "разделяйте ссылки пробелом или запятой."
        )
    
    # Проверяем наличие ссылок (разделитель — любая непробельная пунктуация или пробел)
    raw_urls = re.findall(r"https?://[^\s,;]+", text)
    # Обрезаем возможные завершающие знаки пунктуации
    urls = [u.rstrip('.,;') for u in raw_urls]
    if urls:
        await _handle_listings_export(urls, message)
        return
    await message.answer("🔍 Ссылки не обнаружены. Пожалуйста, пришлите текст с хотя бы одной ссылкой.")


async def _handle_listings_export(urls: list, message: Message):
    if not urls:
        return await message.answer("❗️ Не найдены ссылки для экспорта листингов.")
    try:
        bio = export_listings_to_excel(urls)
        bio.seek(0)
        tg_file = BufferedInputFile(bio.getvalue(), filename="listings.xlsx")
        await message.answer_document(tg_file)
    except Exception as e:
        logger.exception("Ошибка при экспорте листингов")
        await message.answer(f"❌ Не удалось сформировать файл: {e}")
