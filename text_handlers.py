# text_handlers.py
from aiogram.types import Message, BufferedInputFile
from listings_processor import export_listings_to_excel, generate_html_gallery, extract_urls
import io

async def handle_text_message(message: Message):
    """Обработчик текстовых сообщений"""
    
    text = message.text.strip()
    
    # Проверяем, содержит ли сообщение "подбор" (в любом месте)
    is_selection_request = "подбор" in text.lower()
    
    # Извлекаем URL из текста
    urls, url_count = extract_urls(text)
    
    if url_count == 0:
        await message.answer(
            "📋 Отправьте ссылки на объявления CIAN для анализа.\n\n"
            "💡 Для получения Excel-отчета просто отправьте ссылки.\n"
            "🖼️ Для просмотра фотографий напишите 'подбор' + ссылки."
        )
        return
    
    await message.answer(f"🔍 Найдено {url_count} ссылок. Обрабатываю...")
    
    try:
        if is_selection_request:
            # Генерируем HTML с фотографиями для ВСЕХ ссылок
            
            # Извлекаем подзаголовок (текст после "подбор" и до первой ссылки)
            subtitle = None
            if "подбор" in text.lower():
                # Находим позицию слова "подбор"
                podbor_pos = text.lower().find("подбор")
                # Берем текст после "подбор"
                text_after_podbor = text[podbor_pos + 6:].strip()  # 6 = длина слова "подбор"
                
                # Ищем первую ссылку в оставшемся тексте
                first_url_pos = -1
                for url in urls:
                    pos = text_after_podbor.find(url)
                    if pos != -1:
                        first_url_pos = pos
                        break
                
                # Если нашли ссылку, берем текст до неё как подзаголовок
                if first_url_pos != -1:
                    subtitle = text_after_podbor[:first_url_pos].strip()
                else:
                    # Если ссылки не найдены, берем весь текст после "подбор"
                    subtitle = text_after_podbor
            
            await message.answer("🖼️ Генерирую подбор с фотографиями...")
            
            html_content = generate_html_gallery(urls, message.from_user.id, subtitle)
            
            # Создаем файл для отправки
            html_file = io.BytesIO(html_content.encode('utf-8'))
            html_file.name = f"подбор_недвижимости_{message.from_user.id}.html"
            
            # Используем BufferedInputFile для корректной отправки
            input_file = BufferedInputFile(html_file.getvalue(), filename=f"подбор_недвижимости_{message.from_user.id}.html")
            
            await message.answer_document(
                input_file,
                caption=f"🏠 Подбор недвижимости\n"
                       f"📊 Количество объявлений: {url_count}\n"
                       f"📁 Формат: HTML (откройте в браузере для просмотра)"
            )
            
        else:
            # Генерируем Excel как раньше
            await message.answer("📊 Генерирую Excel-отчет...")
            
            excel_file, request_id = await export_listings_to_excel(urls, message.from_user.id)
            
            # Используем BufferedInputFile для корректной отправки
            input_file = BufferedInputFile(excel_file.getvalue(), filename=f"отчет_недвижимости_{message.from_user.id}.xlsx")
            
            await message.answer_document(
                input_file,
                caption=f"📊 Отчет по недвижимости\n"
                       f"📋 Количество объявлений: {url_count}\n"
                       f"🆔 ID запроса: {request_id}\n"
                       f"📁 Формат: Excel"
            )
            
    except Exception as e:
        await message.answer(
            f"❌ Ошибка при обработке: {str(e)}\n\n"
            "Попробуйте еще раз или обратитесь к администратору."
        )
        print(f"Ошибка в handle_text_message: {e}")
