# text_handlers.py
from aiogram.types import Message, BufferedInputFile
from listings_processor import listings_processor, export_listings_to_excel, extract_urls
from db_handler import find_similar_ads_grouped
import io
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font

async def handle_text_message(message: Message):
    text = message.text.strip()
    is_selection_request = "подбор" in text.lower()
    use_embedded = "подбор" in text.lower() and "подбор-" not in text.lower()  # "подбор" = встроенные, "подбор-" = обычные
    urls, url_count = extract_urls(text)
    
    # Извлекаем метро из первого URL для названий файлов
    metro_info = "метро"
    if urls:
        try:
            from listings_processor import listings_processor
            listing_info = listings_processor.extract_listing_info(urls[0])
            if listing_info.get('metro') and listing_info['metro'] != 'N/A':
                metro_info = listing_info['metro']
        except Exception as e:
            print(f"Ошибка при извлечении метро: {e}")
            metro_info = "метро"

    if url_count == 0:
        await message.answer("📋 Отправьте ссылки на объявления CIAN для анализа.\n\n"
                             "💡 Для получения Excel-отчета просто отправьте ссылки.\n"
                             "🖼️ Для просмотра фотографий напишите 'подбор' + ссылки.\n"
                             "🔗 Для обычных ссылок: 'подбор-' + ссылки.")
        return

    await message.answer(f"✅ Принято ссылок: {url_count}")



    try:
        if is_selection_request:
            subtitle = None
            max_photos_per_listing = None  # Максимальное количество фото на объявление
            
            if "подбор" in text.lower():
                podbor_pos = text.lower().find("подбор")
                
                # Ищем цифру перед словом "подбор"
                text_before_podbor = text[:podbor_pos].strip()
                import re
                photo_limit_match = re.search(r'(\d+)\s*$', text_before_podbor)
                if photo_limit_match:
                    max_photos_per_listing = int(photo_limit_match.group(1))
                    print(f"🔢 Ограничение фото: {max_photos_per_listing} на объявление")
                
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
                html_content, photo_stats = await listings_processor.generate_html_gallery_embedded(urls, message.from_user.id, subtitle, remove_watermarks=True, max_photos_per_listing=max_photos_per_listing)
                filename = f"Подбор_{metro_info}.html"
                caption = f"🏠 Подбор недвижимости"
                
                # Показываем статус загрузки фото для каждого объявления
                if max_photos_per_listing:
                    await message.answer(f"🔢 Ограничение: максимум {max_photos_per_listing} фото на объявление")
                
                for stat in photo_stats:
                    if stat['photo_count'] > 0:
                        await message.answer(f"📸 {stat['photo_count']} фото загружено из объявления {stat['listing_number']}")
                    else:
                        if 'error' in stat:
                            await message.answer(f"❌ Ошибка при загрузке фото из объявления {stat['listing_number']}: {stat['error']}")
                        else:
                            await message.answer(f"⚠️ Фото не найдены в объявлении {stat['listing_number']}")
            else:
                html_content = listings_processor.generate_html_gallery(urls, message.from_user.id, subtitle)
                filename = f"Подбор_{metro_info}.html"
                caption = f"🏠 Подбор недвижимости (обычные ссылки)"

            html_file = io.BytesIO(html_content.encode('utf-8'))
            html_file.name = filename
            input_file = BufferedInputFile(html_file.getvalue(), filename=filename)
            await message.answer_document(input_file, caption=caption)

        else:
            excel_file, request_id = await export_listings_to_excel(urls, message.from_user.id)
            input_file = BufferedInputFile(excel_file.getvalue(), filename=f"Сравнение_{metro_info}.xlsx")
            await message.answer_document(input_file, caption=f"📊 Сравнение недвижимости")
            
            # Создаем дополнительный Excel файл с похожими объявлениями
            try:
                similar_ads = await find_similar_ads_grouped(request_id)
                if similar_ads:
                    # Создаем Excel файл с похожими объявлениями
                    similar_excel = io.BytesIO()
                    
                    # Создаем DataFrame для похожих объявлений
                    import pandas as pd
                    import json
                    similar_data = []
                    for group in similar_ads:
                        address = group['address']
                        ads = group['ads']
                        
                        # Проверяем, что ads - это список, а не строка
                        if isinstance(ads, list):
                            ads_list = ads
                        elif isinstance(ads, str):
                            try:
                                # Пытаемся распарсить JSON строку
                                ads_list = json.loads(ads)
                                if not isinstance(ads_list, list):
                                    print(f"⚠️ ads после парсинга не является списком: {type(ads_list)}")
                                    continue
                            except json.JSONDecodeError as e:
                                print(f"⚠️ Ошибка парсинга JSON: {e}")
                                continue
                        else:
                            print(f"⚠️ ads не является списком или строкой: {type(ads)}")
                            continue
                        
                        # Обрабатываем список объявлений
                        for ad in ads_list:
                            if isinstance(ad, dict):
                                ad_copy = ad.copy()  # Создаем копию, чтобы не изменять оригинал
                                ad_copy['Адрес'] = address
                                similar_data.append(ad_copy)
                    
                    if similar_data:
                        # Создаем Excel файл с группировкой по адресам
                        wb = Workbook()
                        ws = wb.active
                        ws.title = "Похожие объявления"
                        
                        # Заголовки для объявлений
                        headers = ['URL', 'Цена', 'Комнат', 'Создано', 'Обновлено', 'Активно', 'Тип продавца']
                        header_row = 1
                        
                        # Группируем данные по адресам
                        current_row = 1
                        for group in similar_ads:
                            address = group['address']
                            ads = group['ads']
                            
                            # Парсим ads если это строка
                            if isinstance(ads, str):
                                try:
                                    ads_list = json.loads(ads)
                                except json.JSONDecodeError:
                                    continue
                            else:
                                ads_list = ads
                            
                            if not isinstance(ads_list, list):
                                continue
                            
                            # Добавляем адрес как заголовок (жирным шрифтом)
                            ws.cell(row=current_row, column=1, value=address)
                            ws.cell(row=current_row, column=1).font = Font(bold=True)
                            current_row += 1
                            
                            # Добавляем заголовки колонок
                            for col, header in enumerate(headers, 1):
                                ws.cell(row=current_row, column=col, value=header)
                                ws.cell(row=current_row, column=col).font = Font(bold=True)
                            current_row += 1
                            
                            # Добавляем объявления
                            for ad in ads_list:
                                if isinstance(ad, dict):
                                    ws.cell(row=current_row, column=1, value=ad.get('url', ''))
                                    ws.cell(row=current_row, column=2, value=ad.get('price', ''))
                                    ws.cell(row=current_row, column=3, value=ad.get('rooms', ''))
                                    ws.cell(row=current_row, column=4, value=ad.get('created', ''))
                                    ws.cell(row=current_row, column=5, value=ad.get('updated', ''))
                                    ws.cell(row=current_row, column=6, value=ad.get('is_active', ''))
                                    ws.cell(row=current_row, column=7, value=ad.get('person_type', ''))
                                    current_row += 1
                            
                            # Добавляем пустую строку между группами
                            current_row += 1
                        
                        # Сохраняем в BytesIO
                        similar_excel = io.BytesIO()
                        wb.save(similar_excel)
                        similar_excel.seek(0)
                        
                        similar_filename = f"похожие_объявления_{metro_info}.xlsx"
                        similar_input_file = BufferedInputFile(similar_excel.getvalue(), filename=similar_filename)
                        await message.answer_document(similar_input_file, caption=f"🔍 Похожие объявления")
                        print(f"✅ Создан Excel с {len(similar_data)} похожими объявлениями")
                    else:
                        print("⚠️ Нет данных для создания Excel с похожими объявлениями")
            except Exception as e:
                print(f"Ошибка при создании файла с похожими объявлениями: {e}")
                # Не отправляем сообщение об ошибке пользователю, чтобы не засорять чат

    except Exception as e:
        await message.answer(f"❌ Ошибка при обработке: {str(e)}\n\nПопробуйте еще раз или обратитесь к администратору.")
        print(f"Ошибка в handle_text_message: {e}")
