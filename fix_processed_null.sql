-- Исправляем процедуру process_cian_ads
-- Проблема: processed = NULL не устанавливается для записей без house_id
-- Решение: изменить логику обновления статуса

CREATE OR REPLACE PROCEDURE public.process_cian_ads(IN p_batch_size integer DEFAULT 1000)
 LANGUAGE plpgsql
AS $procedure$
            DECLARE
                now_ts TIMESTAMP := now();
                v_processed_count INTEGER := 0;
                v_total_count INTEGER;
                v_batch_count INTEGER;

            BEGIN
                -- 1. Подготовка временных таблиц
                TRUNCATE TABLE tmp_cian;
                
                CREATE TEMP TABLE tmp_cian_debug (
                    ad_id    INTEGER,
                    debug    JSONB,
                    success  BOOLEAN
                ) ON COMMIT DROP;

                -- 2. Получаем общее количество необработанных записей
                SELECT COUNT(*) INTO v_total_count FROM ads_cian WHERE processed IS FALSE;
                
                -- 3. Обработка одного батча
                -- Подготовка обогащённых данных из ads_cian с LIMIT
                -- Обрабатываем только записи с processed = FALSE
                CREATE TEMP TABLE tmp_cian_enriched ON COMMIT DROP AS
                SELECT 
                    c.id            AS ad_id,
                    c.url,
                    c.avitoid,
                    c.price,
                    c.rooms,
                    c.area,
                    c.floor,
                    c.total_floors,
                    c.complex,
                    c.min_metro,
                    c.address,
                    c.tags,
                    c.person_type,
                    c.person,
                    c.created_at,
                    c.source_created,
                    c.metro_id,
                    c.district_id,
                    -- Парсим адрес (упрощенная версия)
                    CASE 
                        WHEN c.address IS NOT NULL THEN
                            CASE 
                                WHEN c.address ~ '^[^,]+' THEN split_part(c.address, ',', 1)
                                ELSE c.address
                            END
                        ELSE NULL
                    END AS street,
                    'ул' AS street_type, -- По умолчанию улица
                    CASE 
                        WHEN c.address IS NOT NULL AND c.address ~ ',[^,]*$' THEN
                            split_part(c.address, ',', -1)
                        ELSE NULL
                    END AS house,
                    -- Определяем тип дома по complex
                    CASE 
                        WHEN c.complex ILIKE '%кирпич%' THEN 1
                        WHEN c.complex ILIKE '%панель%' THEN 2
                        WHEN c.complex ILIKE '%монолит%' THEN 3
                        WHEN c.complex ILIKE '%блок%' THEN 4
                        ELSE NULL
                    END AS house_type_id,
                    -- Определяем тип объекта
                    CASE 
                        WHEN c.object_type_id IS NOT NULL THEN c.object_type_id
                        ELSE 1 -- По умолчанию квартира
                    END AS object_type_id,
                    c.district_id AS ao_id
                FROM ads_cian c
                WHERE c.processed IS FALSE  -- Обрабатываем только необработанные записи
                ORDER BY c.id  -- Важно для стабильной пагинации
                LIMIT p_batch_size;

                -- Проверяем, есть ли данные в батче
                GET DIAGNOSTICS v_batch_count = ROW_COUNT;

                    -- 4. Вставка в tmp_cian только валидных записей (с house_id)
                    INSERT INTO tmp_cian (
                        ad_id, house_id, floor, rooms,
                        street, street_type, house,
                        town, total_floors, area, living_area, kitchen_area,
                        house_type_id, ao_id, built, metro_id, km_do_metro, min_metro,
                        source_id, object_type_id, nedvigimost_type_id,
                        url, person_type_id, price,
                        time_source_created, time_source_updated,
                        avitoid, is_actual, description
                    )
                    SELECT 
                        e.ad_id,
                        -- Определяем house_id как в оригинальной процедуре
                        COALESCE(
                            get_house_id_by_jk(e.complex, e.address),
                            gha.result_id
                        ) AS house_id,
                        
                        -- floor: валидация и приведение к smallint
                        CASE 
                            WHEN e.floor IS NOT NULL 
                             AND e.floor BETWEEN -32768 AND 32767
                            THEN e.floor::smallint
                            ELSE 0
                        END AS floor,

                        -- rooms: валидация и приведение к smallint
                        CASE 
                            WHEN e.rooms IS NOT NULL 
                             AND e.rooms BETWEEN -32768 AND 32767
                            THEN e.rooms::smallint
                            ELSE 0
                        END AS rooms,

                        e.street, 
                        -- street_type: ограничиваем длину для совместимости с tmp_flats_history
                        LEFT(e.street_type, 9) AS street_type,
                        e.house,
                        1 AS town, -- Москва

                        -- total_floors: валидация
                        CASE 
                            WHEN e.total_floors IS NOT NULL 
                             AND e.total_floors BETWEEN -32768 AND 32767
                            THEN e.total_floors::smallint
                            ELSE NULL
                        END AS total_floors,

                        e.area,
                        NULL AS living_area, -- Пока NULL, можно добавить позже
                        NULL AS kitchen_area, -- Пока NULL, можно добавить позже

                        -- house_type_id: приводим к smallint для совместимости
                        CASE 
                            WHEN e.house_type_id IS NOT NULL 
                             AND e.house_type_id BETWEEN -32768 AND 32767
                            THEN e.house_type_id::smallint
                            ELSE NULL
                        END AS house_type_id,
                        
                        -- ao_id: приводим к smallint для совместимости
                        CASE 
                            WHEN e.ao_id IS NOT NULL 
                             AND e.ao_id BETWEEN -32768 AND 32767
                            THEN e.ao_id::smallint
                            ELSE NULL
                        END AS ao_id,
                        
                        NULL AS built, -- Пока NULL, можно добавить позже
                        e.metro_id,
                        NULL AS km_do_metro, -- NULL, так как у нас есть min_metro
                        
                        -- min_metro: валидация и приведение к smallint
                        CASE 
                            WHEN e.min_metro IS NOT NULL 
                             AND e.min_metro BETWEEN -32768 AND 32767
                            THEN e.min_metro::smallint
                            ELSE NULL
                        END AS min_metro,
                        
                        4 AS source_id, -- CIAN
                        e.object_type_id,
                        NULL AS nedvigimost_type_id, -- Пока NULL
                        e.url,
                        e.person_type,
                        e.price, -- Оставляем bigint (совместимо с flats_history)
                        e.source_created AS time_source_created, -- Оставляем timestamp
                        e.created_at AS time_source_updated,
                        e.avitoid,
                        TRUE AS is_actual, -- Всегда TRUE (boolean) для tmp_cian, потом преобразуется в smallint
                        e.tags AS description
                    FROM tmp_cian_enriched e
                    LEFT JOIN LATERAL get_house_id_by_address(e.address) AS gha(result_id, street_found, house_part) ON TRUE
                    WHERE e.avitoid IS NOT NULL
                      AND COALESCE(
                            get_house_id_by_jk(e.complex, e.address),
                            gha.result_id
                          ) IS NOT NULL; -- Только записи с house_id

                    -- 5. Логирование неудачных записей для текущего батча - УПРОЩЕННЫЙ ДЕБАГ
                    INSERT INTO tmp_cian_debug(ad_id, debug, success)
                    SELECT
                        e.ad_id,
                        jsonb_build_object(
                            -- Информация о ЖК и парсинге адреса
                            'raw_complex', e.complex,
                            'complex_match_id', get_house_id_by_jk(e.complex, e.address),
                            'addr_match_id', gha.result_id,
                            'street_found', gha.street_found,
                            'house_part', gha.house_part,
                            'final_house_id', COALESCE(
                                get_house_id_by_jk(e.complex, e.address),
                                gha.result_id
                            ),
                            
                            -- Парсинг адреса
                            'raw_address', e.address,
                            'parsed_street', e.street,
                            'parsed_house', e.house,
                            
                            -- Результат определения house_id
                            'house_id_found', COALESCE(
                                get_house_id_by_jk(e.complex, e.address),
                                gha.result_id
                            ) IS NOT NULL
                        ),
                        FALSE
                    FROM tmp_cian_enriched e
                    LEFT JOIN LATERAL get_house_id_by_address(e.address) AS gha(result_id, street_found, house_part) ON TRUE
                    WHERE e.avitoid IS NULL
                       OR COALESCE(
                            get_house_id_by_jk(e.complex, e.address),
                            gha.result_id
                          ) IS NULL;

                    -- 6. Обновляем статус обработки в ads_cian
                    -- Успешно обработанные записи (с house_id) - processed = TRUE
                    UPDATE ads_cian 
                    SET processed = TRUE, 
                        proc_at = now(),
                        debug = NULL
                    WHERE id IN (
                        SELECT e.ad_id 
                        FROM tmp_cian_enriched e
                        WHERE e.avitoid IS NOT NULL
                          AND COALESCE(
                                get_house_id_by_jk(e.complex, e.address),
                                (SELECT gha.result_id FROM get_house_id_by_address(e.address) AS gha(result_id, street_found, house_part))
                              ) IS NOT NULL
                    );

                    -- ИСПРАВЛЕНИЕ: Невалидные записи (без house_id) - processed = NULL, с отладочной информацией
                    -- Убираем условие e.avitoid IS NOT NULL, чтобы обрабатывать все записи без house_id
                    UPDATE ads_cian 
                    SET processed = NULL, 
                        proc_at = now(),
                        debug = jsonb_build_object(
                            'error', 'house_id_not_found',
                            'complex', e.complex,
                            'address', e.address,
                            'complex_match_id', get_house_id_by_jk(e.complex, e.address),
                            'addr_match_id', (SELECT gha.result_id FROM get_house_id_by_address(e.address) AS gha(result_id, street_found, house_part))
                        )
                    FROM tmp_cian_enriched e
                    WHERE ads_cian.id = e.ad_id
                      AND COALESCE(
                            get_house_id_by_jk(e.complex, e.address),
                            (SELECT gha.result_id FROM get_house_id_by_address(e.address) AS gha(result_id, street_found, house_part))
                          ) IS NULL;

                    -- Записи с ошибками валидации - processed = TRUE, с отладочной информацией
                    UPDATE ads_cian 
                    SET processed = TRUE, 
                        proc_at = now(),
                        debug = d.debug
                    FROM tmp_cian_debug d
                    WHERE ads_cian.id = d.ad_id;

                    -- 7. Обновляем счетчик обработанных записей
                    v_processed_count := v_batch_count;
                    
                    -- 8. Логируем результат обработки батча
                    RAISE NOTICE 'Обработан батч: % записей из %', v_batch_count, v_total_count;
                    
                    -- 9. Очищаем временную таблицу
                    DROP TABLE IF EXISTS tmp_cian_enriched;

                -- 11. Логируем финальный результат
                RAISE NOTICE 'Импорт CIAN завершен. Обработано записей: % из %', v_processed_count, v_total_count;
                
                -- 12. Показываем статистику
                RAISE NOTICE 'Успешно импортировано: %', (SELECT COUNT(*) FROM tmp_cian);
                RAISE NOTICE 'Ошибок валидации: %', (SELECT COUNT(*) FROM tmp_cian_debug);
                
                -- 13. Показываем статистику по house_id и min_metro
                RAISE NOTICE 'Записей с house_id: %', (SELECT COUNT(*) FROM tmp_cian WHERE house_id IS NOT NULL);
                RAISE NOTICE 'Записей без house_id: %', (SELECT COUNT(*) FROM tmp_cian WHERE house_id IS NULL);
                RAISE NOTICE 'Записей с min_metro: %', (SELECT COUNT(*) FROM tmp_cian WHERE min_metro IS NOT NULL);
                RAISE NOTICE 'Записей без min_metro: %', (SELECT COUNT(*) FROM tmp_cian WHERE min_metro IS NULL);
                
                -- 14. Показываем статистику по статусу обработки
                RAISE NOTICE 'Записей с processed = TRUE: %', (SELECT COUNT(*) FROM ads_cian WHERE processed IS TRUE);
                RAISE NOTICE 'Записей с processed = FALSE: %', (SELECT COUNT(*) FROM ads_cian WHERE processed IS FALSE);
                RAISE NOTICE 'Записей с processed = NULL: %', (SELECT COUNT(*) FROM ads_cian WHERE processed IS NULL);
                RAISE NOTICE 'Записей с debug: %', (SELECT COUNT(*) FROM ads_cian WHERE debug IS NOT NULL);
                
                -- В продакшене можно раскомментировать COMMIT
                -- COMMIT;
            EXCEPTION
                WHEN OTHERS THEN
                    -- В продакшене можно раскомментировать ROLLBACK
                    -- ROLLBACK;
                    RAISE EXCEPTION 'Ошибка импорта CIAN: %', SQLERRM;
            END;
            $procedure$;
