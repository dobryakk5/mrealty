import re
import logging
from aiogram.types import Message
from listings_processor import export_listings_to_excel, extract_urls, export_sim_ads
from aiogram.types.input_file import BufferedInputFile

logger = logging.getLogger(__name__)

async def handle_text_message(message: Message):
    text = message.text.strip()

    # Если пользователь нажал «Инструкция»
    if text == "📘 инструкция":
        return await message.answer(
            "Чтобы получить отчёт по квартирам, пришлите ссылки ЦИАН вида\n"
            "https://www.cian.ru/sale/flat/320223651\n"
            "В результате получите сравнительную excel таблицу для аналитики и редактирования"
        )
    
    # Извлекаем URL и их количество через extract_urls
    urls, count = extract_urls(text)
    if count:
        # Уведомляем пользователя о числе ссылок
        await message.answer(f"Принято ссылок: {count}")
        await _handle_listings_export(urls, message)
    else:
        await message.answer("🔍 Ссылки не обнаружены. Пожалуйста, пришлите текст с cian ссылкой.")


async def _handle_listings_export(urls: list[str], message: Message):
    if not urls:
        return await message.answer("❗️ Не найдены ссылки для экспорта листингов.")
    try:
        user_id = message.from_user.id

        # 1) Экспорт основных листингов
        bio, request_id = await export_listings_to_excel(urls, user_id)
        bio.seek(0)
        tg_file_main = BufferedInputFile(
            bio.getvalue(),
            filename="сравнение_квартир.xlsx"
        )
        await message.answer_document(tg_file_main)

        # 2) Экспорт похожих объявлений
        sim_bio, _ = await export_sim_ads(request_id)
        sim_bio.seek(0)
        tg_file_sim = BufferedInputFile(
            sim_bio.getvalue(),
            filename="Еще_объявления_по_этим_квартирам.xlsx"
        )
        await message.answer_document(tg_file_sim)

    except Exception as e:
        logger.exception("Ошибка при экспорте листингов")
        await message.answer(f"❌ Не удалось сформировать файл: {e}")


