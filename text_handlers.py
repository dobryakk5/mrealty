# text_handlers.py
from aiogram.types import Message, BufferedInputFile
from listings_processor import export_listings_to_excel, generate_html_gallery, generate_html_gallery_embedded, extract_urls
import io

async def handle_text_message(message: Message):
    text = message.text.strip()
    is_selection_request = "подбор" in text.lower()
    use_embedded = "подбор" in text.lower() and "подбор-" not in text.lower()  # "подбор" = встроенные, "подбор-" = обычные
    urls, url_count = extract_urls(text)

    if url_count == 0:
        await message.answer("📋 Отправьте ссылки на объявления CIAN для анализа.\n\n"
                             "💡 Для получения Excel-отчета просто отправьте ссылки.\n"
                             "🖼️ Для просмотра фотографий напишите 'подбор' + ссылки.\n"
                             "🔗 Для обычных ссылок: 'подбор-' + ссылки.")
        return

    await message.answer(f"🔍 Найдено {url_count} ссылок. Обрабатываю...")

    try:
        if is_selection_request:
            subtitle = None
            if "подбор" in text.lower():
                podbor_pos = text.lower().find("подбор")
                text_after_podbor = text[podbor_pos + 6:].strip()
                first_url_pos = -1
                for url in urls:
                    pos = text_after_podbor.find(url)
                    if pos != -1:
                        first_url_pos = pos
                        break
                if first_url_pos != -1:
                    subtitle = text_after_podbor[:first_url_pos].strip()
                else:
                    subtitle = text_after_podbor
                if subtitle:
                    subtitle = subtitle.replace("подбор", "").replace("подбор-", "").strip()

            if use_embedded:
                await message.answer("🔗 Генерирую подбор с встроенными фотографиями...")
                html_content = generate_html_gallery_embedded(urls, message.from_user.id, subtitle)
                filename = f"подбор_встроенные_фото_{message.from_user.id}.html"
                caption = f"🏠 Подбор недвижимости\n📊 Количество объявлений: {url_count}\n📁 Формат: HTML с встроенными изображениями"
            else:
                await message.answer("🖼️ Генерирую подбор с обычными ссылками на фото...")
                html_content = generate_html_gallery(urls, message.from_user.id, subtitle)
                filename = f"подбор_обычные_ссылки_{message.from_user.id}.html"
                caption = f"🏠 Подбор недвижимости (обычные ссылки)\n📊 Количество объявлений: {url_count}\n📁 Формат: HTML (откройте в браузере для просмотра)"

            html_file = io.BytesIO(html_content.encode('utf-8'))
            html_file.name = filename
            input_file = BufferedInputFile(html_file.getvalue(), filename=filename)
            await message.answer_document(input_file, caption=caption)

        else:
            await message.answer("📊 Генерирую Excel-отчет...")
            excel_file, request_id = await export_listings_to_excel(urls, message.from_user.id)
            input_file = BufferedInputFile(excel_file.getvalue(), filename=f"отчет_недвижимости_{message.from_user.id}.xlsx")
            await message.answer_document(input_file, caption=f"📊 Отчет по недвижимости\n"
                                                             f"📋 Количество объявлений: {url_count}\n"
                                                             f"🆔 ID запроса: {request_id}\n"
                                                             f"📁 Формат: Excel")

    except Exception as e:
        await message.answer(f"❌ Ошибка при обработке: {str(e)}\n\nПопробуйте еще раз или обратитесь к администратору.")
        print(f"Ошибка в handle_text_message: {e}")
