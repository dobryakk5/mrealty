-- DROP PROCEDURE public.process_cian_ads(int4);

CREATE OR REPLACE PROCEDURE public.process_cian_ads(IN p_batch_size integer DEFAULT 1000)
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    now_ts TIMESTAMP := now();
    v_processed_count INTEGER := 0;
    v_total_count INTEGER;
    v_batch_count INTEGER;
BEGIN
    ------------------------------------------------------------
    -- 1. Подготовка временных таблиц
    ------------------------------------------------------------
    TRUNCATE TABLE tmp_flats_history;

    CREATE TEMP TABLE tmp_cian_debug (
        ad_id    INTEGER,
        debug    JSONB,
        success  BOOLEAN
    ) ON COMMIT DROP;

    ------------------------------------------------------------
    -- 2. Отмечаем все записи, которые нужно пропустить по адресу
    ------------------------------------------------------------
    UPDATE ads_cian
    SET processed = TRUE,
        proc_at = now(),
        debug = jsonb_build_object(
            'skip_reason',
            CASE
                WHEN address ILIKE '%Московская область%' THEN 'Московская область'
                WHEN address ILIKE '%Новомосковский%' THEN 'Новомосковский'
            END
        )
    WHERE processed IS FALSE
      AND (address ILIKE '%Московская область%'
           OR address ILIKE '%Новомосковский%');

    ------------------------------------------------------------
    -- 3. Пересчитываем количество необработанных
    ------------------------------------------------------------
    SELECT COUNT(*) INTO v_total_count
    FROM ads_cian
    WHERE processed IS FALSE;

    IF v_total_count = 0 THEN
        RAISE NOTICE 'Нет необработанных записей (все исключены фильтром адреса)';
        RETURN;
    END IF;

    ------------------------------------------------------------
    -- 4. Формируем батч без исключённых адресов
    ------------------------------------------------------------
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
        CASE 
            WHEN c.address IS NOT NULL THEN
                (SELECT norm_name FROM public.parse_address(c.address) LIMIT 1)
            ELSE NULL
        END AS street,
        CASE 
            WHEN c.address IS NOT NULL THEN
                (SELECT street_type FROM public.parse_address(c.address) LIMIT 1)
            ELSE 'ул'
        END AS street_type,
        CASE 
            WHEN c.address IS NOT NULL THEN
                (SELECT house_part FROM public.parse_address(c.address) LIMIT 1)
            ELSE NULL
        END AS house,
        CASE 
            WHEN c.complex ILIKE '%кирпич%' THEN 1
            WHEN c.complex ILIKE '%панель%' THEN 2
            WHEN c.complex ILIKE '%монолит%' THEN 3
            WHEN c.complex ILIKE '%блок%' THEN 4
            ELSE NULL
        END AS house_type_id,
        CASE 
            WHEN c.object_type_id IS NOT NULL THEN c.object_type_id
            ELSE 1
        END AS object_type_id,
        c.district_id AS ao_id
    FROM ads_cian c
    WHERE c.processed IS FALSE
      AND (
            c.address IS NULL
            OR (
                c.address NOT ILIKE '%Московская область%'
                AND c.address NOT ILIKE '%Новомосковский%'
            )
          )
    ORDER BY c.id
    LIMIT p_batch_size;

    GET DIAGNOSTICS v_batch_count = ROW_COUNT;
    
    IF v_batch_count = 0 THEN
        RAISE NOTICE 'Нет записей в батче для обработки';
        RETURN;
    END IF;

    ------------------------------------------------------------
    -- 5. Адресный матчинг
    ------------------------------------------------------------
    CREATE TEMP TABLE tmp_address_parsing ON COMMIT DROP AS
    SELECT 
        e.ad_id,
        e.complex,
        e.address,
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

    ------------------------------------------------------------
    -- 6. Вставка валидных данных
    ------------------------------------------------------------
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
        COALESCE(ap.complex_match_id, ap.addr_match_id),
        CASE WHEN e.floor BETWEEN -32768 AND 32767 THEN e.floor::smallint ELSE 0 END,
        CASE WHEN e.rooms BETWEEN -32768 AND 32767 THEN e.rooms::smallint ELSE 0 END,
        e.street,
        LEFT(e.street_type, 9),
        e.house,
        1,
        CASE WHEN e.total_floors BETWEEN -32768 AND 32767 THEN e.total_floors::smallint ELSE NULL END,
        e.area,
        NULL,
        NULL,
        CASE WHEN e.house_type_id BETWEEN -32768 AND 32767 THEN e.house_type_id::smallint ELSE NULL END,
        CASE WHEN e.ao_id BETWEEN -32768 AND 32767 THEN e.ao_id::smallint ELSE NULL END,
        NULL,
        e.metro_id,
        NULL,
        CASE WHEN e.min_metro BETWEEN -32768 AND 32767 THEN e.min_metro::smallint ELSE NULL END,
        4,
        e.object_type_id,
        NULL,
        e.url,
        e.person_type,
        CASE WHEN e.price > 0 THEN e.price::numeric ELSE NULL END,
        e.source_created,
        e.created_at,
        e.avitoid,
        1,
        e.tags
    FROM tmp_cian_enriched e
    JOIN tmp_address_parsing ap ON ap.ad_id = e.ad_id
    WHERE e.avitoid IS NOT NULL
      AND COALESCE(ap.complex_match_id, ap.addr_match_id) IS NOT NULL;

    ------------------------------------------------------------
    -- 7. Логирование ошибок
    ------------------------------------------------------------
    INSERT INTO tmp_cian_debug(ad_id, debug, success)
    SELECT
        e.ad_id,
        jsonb_build_object(
            'error', 'house_id_not_found',
            'raw_complex', e.complex,
            'raw_address', e.address,
            'complex_match_id', ap.complex_match_id,
            'addr_match_id', ap.addr_match_id
        ),
        FALSE
    FROM tmp_cian_enriched e
    JOIN tmp_address_parsing ap ON e.ad_id = ap.ad_id
    WHERE e.avitoid IS NULL
       OR COALESCE(ap.complex_match_id, ap.addr_match_id) IS NULL;

    ------------------------------------------------------------
    -- 8. Обновление статусов
    ------------------------------------------------------------
    UPDATE ads_cian
    SET processed = TRUE,
        proc_at = now(),
        debug = NULL
    WHERE id IN (
        SELECT e.ad_id
        FROM tmp_cian_enriched e
        JOIN tmp_address_parsing ap ON ap.ad_id = e.ad_id
        WHERE e.avitoid IS NOT NULL
          AND COALESCE(ap.complex_match_id, ap.addr_match_id) IS NOT NULL
    );

    UPDATE ads_cian
    SET processed = NULL,
        proc_at = now(),
        debug = d.debug
    FROM tmp_cian_debug d
    WHERE ads_cian.id = d.ad_id;

    ------------------------------------------------------------
    -- 9. batch_upsert()
    ------------------------------------------------------------
    CALL batch_upsert();

    ------------------------------------------------------------
    -- 10. Логи
    ------------------------------------------------------------
    RAISE NOTICE 'Импорт завершён. В батче: %, всего осталось: %', v_batch_count, v_total_count;
    RAISE NOTICE 'Успешно импортировано: %', (SELECT COUNT(*) FROM tmp_flats_history);
    RAISE NOTICE 'Ошибок: %', (SELECT COUNT(*) FROM tmp_cian_debug);

EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Ошибка импорта CIAN: %', SQLERRM;
END;
$procedure$
;

-- Permissions

ALTER PROCEDURE public.process_cian_ads(int4) OWNER TO postgres;
GRANT ALL ON PROCEDURE public.process_cian_ads(int4) TO public;
GRANT ALL ON PROCEDURE public.process_cian_ads(int4) TO postgres;
GRANT ALL ON PROCEDURE public.process_cian_ads(int4) TO mwww;
