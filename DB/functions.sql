-- DROP PROCEDURE public.process_cian_ads(int4);

CREATE OR REPLACE PROCEDURE public.process_cian_ads(IN p_batch_size integer DEFAULT 1000)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    now_ts TIMESTAMP := now();
    v_processed_count INTEGER := 0;
    v_total_count INTEGER;
    v_batch_count INTEGER;
    v_house_id INTEGER;
    v_complex_match_id INTEGER;
    v_addr_match_id INTEGER;
BEGIN
    -- 1. Подготовка временных таблиц
    TRUNCATE TABLE tmp_flats_history;
    
    CREATE TEMP TABLE tmp_cian_debug (
        ad_id    INTEGER,
        debug    JSONB,
        success  BOOLEAN
    ) ON COMMIT DROP;

    -- 2. Получаем общее количество необработанных записей
    SELECT COUNT(*) INTO v_total_count FROM ads_cian WHERE processed IS FALSE;

    -- Проверяем, есть ли необработанные записи
    IF v_total_count = 0 THEN
        RAISE NOTICE 'Нет необработанных записей CIAN для обработки';
        RETURN;
    END IF;
    
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
        -- Парсим адрес через функцию parse_address (правильный способ)
        CASE 
            WHEN c.address IS NOT NULL THEN
                (SELECT norm_name FROM public.parse_address(c.address) LIMIT 1)
            ELSE NULL
        END AS street,
        -- Определяем тип улицы через функцию parse_address
        CASE 
            WHEN c.address IS NOT NULL THEN
                (SELECT street_type FROM public.parse_address(c.address) LIMIT 1)
            ELSE 'ул'::varchar(9)  -- По умолчанию улица
        END AS street_type,
        CASE 
            WHEN c.address IS NOT NULL THEN
                (SELECT house_part FROM public.parse_address(c.address) LIMIT 1)
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
    
    IF v_batch_count = 0 THEN
        RAISE NOTICE 'Нет записей в батче для обработки';
        RETURN;
    END IF;

    -- 4. Создаем временную таблицу для хранения результатов парсинга адресов
    CREATE TEMP TABLE tmp_address_parsing ON COMMIT DROP AS
    SELECT 
        e.ad_id,
        e.complex,
        e.address,
        -- Безопасно вызываем функции с проверкой на null
        CASE 
            WHEN e.complex IS NOT NULL AND e.address IS NOT NULL 
            THEN get_house_id_by_jk(e.complex, e.address)
            ELSE NULL
        END AS complex_match_id,
        CASE 
            WHEN e.address IS NOT NULL 
            THEN (SELECT result_id FROM get_house_id_by_address(e.address) LIMIT 1)
            ELSE NULL
        END AS addr_match_id
    FROM tmp_cian_enriched e;

    -- 5. Вставка в tmp_flats_history только валидных записей (с house_id)
    INSERT INTO tmp_flats_history (
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
        COALESCE(ap.complex_match_id, ap.addr_match_id) AS house_id,
        
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
        -- street_type: приводим к varchar(9) для совместимости с tmp_flats_history
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
        -- price: приводим к numeric для совместимости с tmp_flats_history
        CASE 
            WHEN e.price IS NOT NULL 
             AND e.price > 0
            THEN e.price::numeric
            ELSE NULL
        END AS price,
        
        -- time_source_created: приводим к timestamp
        CASE 
            WHEN e.source_created IS NOT NULL 
            THEN e.source_created::timestamp
            ELSE NULL
        END AS time_source_created,
        
        -- time_source_updated: приводим к timestamp
        CASE 
            WHEN e.created_at IS NOT NULL 
            THEN e.created_at::timestamp
            ELSE NULL
        END AS time_source_updated,
        
        e.avitoid,
        CASE 
            WHEN TRUE THEN 1  -- Всегда 1 (активно) для CIAN
            ELSE 0
        END AS is_actual,
        e.tags AS description
    FROM tmp_cian_enriched e
    JOIN tmp_address_parsing ap ON e.ad_id = ap.ad_id
    WHERE e.avitoid IS NOT NULL
      AND COALESCE(ap.complex_match_id, ap.addr_match_id) IS NOT NULL; -- Только записи с house_id

    -- 6. Логирование неудачных записей для текущего батча
    INSERT INTO tmp_cian_debug(ad_id, debug, success)
    SELECT
        e.ad_id,
        jsonb_build_object(
            'error', 'house_id_not_found',
            'raw_complex', e.complex,
            'raw_address', e.address,
            'complex_match_id', ap.complex_match_id,
            'addr_match_id', ap.addr_match_id,
            'final_house_id', COALESCE(ap.complex_match_id, ap.addr_match_id)
        ),
        FALSE
    FROM tmp_cian_enriched e
    JOIN tmp_address_parsing ap ON e.ad_id = ap.ad_id
    WHERE e.avitoid IS NULL
       OR COALESCE(ap.complex_match_id, ap.addr_match_id) IS NULL;

    -- 7. Обновляем статус обработки в ads_cian
    -- Успешно обработанные записи (с house_id) - processed = TRUE
    UPDATE ads_cian 
    SET processed = TRUE, 
        proc_at = now(),
        debug = NULL
    WHERE id IN (
        SELECT e.ad_id 
        FROM tmp_cian_enriched e
        JOIN tmp_address_parsing ap ON e.ad_id = ap.ad_id
        WHERE e.avitoid IS NOT NULL
          AND COALESCE(ap.complex_match_id, ap.addr_match_id) IS NOT NULL
    );

    -- Невалидные записи (без house_id) - processed = NULL, с отладочной информацией
    UPDATE ads_cian 
    SET processed = NULL, 
        proc_at = now(),
        debug = d.debug
    FROM tmp_cian_debug d
    WHERE ads_cian.id = d.ad_id;

    -- 8. Обновляем счетчик обработанных записей
    v_processed_count := v_batch_count;
    
    -- 9. Логируем результат обработки батча
    RAISE NOTICE 'Обработан батч: % записей из %', v_batch_count, v_total_count;

    -- 10. Очищаем временные таблицы
    DROP TABLE IF EXISTS tmp_cian_enriched;
    DROP TABLE IF EXISTS tmp_address_parsing;

    -- 11. ВАЖНО: Вызываем batch_upsert() как в продакшн процедуре process_all_ads
    CALL batch_upsert();

    -- 12. Логируем финальный результат
    RAISE NOTICE 'Импорт CIAN завершен. Обработано записей: % из %', v_processed_count, v_total_count;
                                        
    -- 13. Показываем статистику
    RAISE NOTICE 'Успешно импортировано: %', (SELECT COUNT(*) FROM tmp_flats_history);
    RAISE NOTICE 'Ошибок валидации: %', (SELECT COUNT(*) FROM tmp_cian_debug);
    
    -- 14. Показываем статистику по house_id
    RAISE NOTICE 'Записей с house_id: %', (SELECT COUNT(*) FROM tmp_flats_history WHERE house_id IS NOT NULL);
    RAISE NOTICE 'Записей без house_id: %', (SELECT COUNT(*) FROM tmp_flats_history WHERE house_id IS NULL);
                                        
    -- В продакшене можно раскомментировать COMMIT
    -- COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        -- В продакшене можно раскомментировать ROLLBACK
        -- ROLLBACK;
        RAISE EXCEPTION 'Ошибка импорта CIAN: %', SQLERRM;
END;
$procedure$
;

-- Permissions

ALTER PROCEDURE public.process_cian_ads(int4) OWNER TO postgres;
GRANT ALL ON PROCEDURE public.process_cian_ads(int4) TO public;
GRANT ALL ON PROCEDURE public.process_cian_ads(int4) TO postgres;
GRANT ALL ON PROCEDURE public.process_cian_ads(int4) TO mwww;
