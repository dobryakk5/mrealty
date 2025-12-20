CREATE PROCEDURE public.batch_upsert()
    LANGUAGE plpgsql
    AS $$
BEGIN
  ----------------------------------------------------------------
  -- 1. Upsert в таблицу flats
  --    Добавляем house_type_id, ceiling_height, balcony_type_id
  ----------------------------------------------------------------
  INSERT INTO flats (
    house_id, floor, rooms,
    street, street_type, house,
    town, total_floors,
    area, living_area, kitchen_area,
    house_type_id, ao_id, built,
    metro_id, km_do_metro, min_metro,
    ceiling_height, balcony_type_id
  )
  SELECT DISTINCT
    house_id, floor, rooms,
    street, street_type, house,
    town, total_floors,
    area, living_area, kitchen_area,
    house_type_id, ao_id, built,
    metro_id, km_do_metro, min_metro,
    ceiling_height, balcony_type_id
  FROM tmp_flats_history
  ON CONFLICT DO NOTHING;


  ----------------------------------------------------------------
  -- 2. Выборка изменившихся объявлений и подготовка старых значений
  ----------------------------------------------------------------
  WITH to_change AS (
    SELECT DISTINCT ON (fh.id)
      fh.id                    AS old_history_id,
      fh.time_source_updated   AS old_updated,

      -- старое значение цены, только если изменилась
      CASE
        WHEN fh.price IS DISTINCT FROM tmp.price THEN fh.price
        ELSE NULL
      END AS old_price,

      -- старый статус, только если изменился
      CASE
        WHEN fh.is_actual IS DISTINCT FROM tmp.is_actual THEN fh.is_actual
        ELSE NULL
      END AS old_status,

      -- старое описание, только если изменилось
      CASE
        WHEN fh.description IS DISTINCT FROM tmp.description THEN fh.description
        ELSE NULL
      END AS old_desc

    FROM flats_history fh
    JOIN tmp_flats_history tmp
      ON fh.avitoid   = tmp.avitoid
     AND fh.source_id = tmp.source_id
    WHERE fh.price       IS DISTINCT FROM tmp.price
       OR fh.is_actual   IS DISTINCT FROM tmp.is_actual
       OR fh.description IS DISTINCT FROM tmp.description
    ORDER BY fh.id, fh.time_source_updated DESC
  )

  ----------------------------------------------------------------
  -- 3. Снятие старых значений в flats_changes (без person)
  ----------------------------------------------------------------
  INSERT INTO flats_changes (
    flats_history_id,
    updated,
    price,
    is_actual,
    description
  )
  SELECT
    tc.old_history_id,
    COALESCE(tc.old_updated, now()),
    tc.old_price,
    tc.old_status,
    tc.old_desc
  FROM to_change tc;


  ----------------------------------------------------------------
  -- 4. Upsert в flats_history (включая person и is_actual)
  ----------------------------------------------------------------
  INSERT INTO flats_history (
    house_id, floor, rooms,
    source_id, object_type_id, ad_type,
    url, person_type_id, price,
    time_source_created, time_source_updated,
    avitoid, is_actual, description, person
  )
  SELECT
    house_id, floor, rooms,
    source_id, object_type_id, nedvigimost_type_id,
    url, person_type_id, price,
    time_source_created, time_source_updated,
    avitoid, is_actual, description, person
  FROM tmp_flats_history
--
CREATE PROCEDURE public.batch_upsert7()
    LANGUAGE plpgsql
    AS $$
BEGIN
  ----------------------------------------------------------------
  -- 1. Upsert в таблицу flats
  --    Добавляем house_type_id, ceiling_height, balcony_type_id
  ----------------------------------------------------------------
  INSERT INTO flats (
    house_id, floor, rooms,
    street, street_type, house,
    town, total_floors,
    area, living_area, kitchen_area,
    house_type_id, ao_id, built,
    metro_id, km_do_metro, min_metro,
    ceiling_height, balcony_type_id
  )
  SELECT DISTINCT
    house_id, floor, rooms,
    street, street_type, house,
    town, total_floors,
    area, living_area, kitchen_area,
    house_type_id, ao_id, built,
    metro_id, km_do_metro, min_metro,
    ceiling_height, balcony_type_id
  FROM tmp_flats_history
  ON CONFLICT DO NOTHING;


  ----------------------------------------------------------------
  -- 2. Выборка изменившихся объявлений и подготовка старых значений
  ----------------------------------------------------------------
  WITH to_change AS (
    SELECT
      fh.id                    AS old_history_id,
      fh.time_source_updated   AS old_updated,

      -- старое значение цены, только если изменилась
      CASE
        WHEN fh.price IS DISTINCT FROM tmp.price THEN fh.price
        ELSE NULL
      END AS old_price,

      -- старый статус, только если изменился
      CASE
        WHEN fh.is_actual IS DISTINCT FROM tmp.is_actual THEN fh.is_actual
        ELSE NULL
      END AS old_status,

      -- старое описание, только если изменилось
      CASE
        WHEN fh.description IS DISTINCT FROM tmp.description THEN fh.description
        ELSE NULL
      END AS old_desc

    FROM flats_history fh
    JOIN tmp_flats_history tmp
      ON fh.avitoid   = tmp.avitoid
     AND fh.source_id = tmp.source_id
    WHERE fh.price       IS DISTINCT FROM tmp.price
       OR fh.is_actual   IS DISTINCT FROM tmp.is_actual
       OR fh.description IS DISTINCT FROM tmp.description
  )

  ----------------------------------------------------------------
  -- 3. Снятие старых значений в flats_changes (без person)
  ----------------------------------------------------------------
  INSERT INTO flats_changes (
    flats_history_id,
    updated,
    price,
    is_actual,
    description
  )
  SELECT
    tc.old_history_id,
    COALESCE(tc.old_updated, now()),
    tc.old_price,
    tc.old_status,
    tc.old_desc
  FROM to_change tc;


  ----------------------------------------------------------------
  -- 4. Upsert в flats_history (включая person, is_actual и guid_w7)
  ----------------------------------------------------------------
  INSERT INTO flats_history (
    house_id, floor, rooms,
    source_id, object_type_id, ad_type,
    url, person_type_id, price,
    time_source_created, time_source_updated,
    avitoid, is_actual, description, person,
    guid_w7  -- Добавлено поле GUID
  )
  SELECT
    house_id, floor, rooms,
    source_id, object_type_id, nedvigimost_type_id,
    url, person_type_id, price,
    time_source_created, time_source_updated,
    avitoid, is_actual, description, person,
    guid_w7  -- Сохраняем GUID
--
CREATE FUNCTION public.find_ads(p_address text, p_floor integer, p_rooms integer) RETURNS TABLE(price bigint, floor smallint, rooms smallint, person_type text, created timestamp without time zone, updated timestamp without time zone, url text, is_active boolean, area numeric, kitchen_area numeric)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_house_id INT;
BEGIN
    -- Ищем id дома
    SELECT result_id
      INTO v_house_id
      FROM get_house_id_by_address(p_address);

    -- Если дом не найден — возвращаем пустой набор
    IF v_house_id IS NULL THEN
        RETURN;
    END IF;

    -- Основной запрос БЕЗ DISTINCT - показываем все объявления
    RETURN QUERY
    SELECT 
        h.price,
        h.floor,
        h.rooms,
        CASE h.person_type_id
            WHEN 3 THEN 'собственник'
            WHEN 2 THEN 'агентство'
            ELSE 'агент'
        END,
        h.time_source_created::timestamp AS created,
        h.time_source_updated AS updated,
        h.url,
        (h.is_actual <> 0) AS is_active,
        f.area,
        f.kitchen_area
    FROM flats_history h
    LEFT JOIN public.flats f ON f.house_id = h.house_id AND f.rooms = h.rooms AND f.floor = h.floor
    WHERE h.house_id = v_house_id
      AND (p_floor IS NULL OR h.floor = p_floor)
      AND (p_rooms IS NULL OR h.rooms = p_rooms)
    ORDER BY h.price ASC,
             h.is_actual DESC,  -- Активные объявления сначала
             CASE 
                WHEN h.url LIKE '%cian.ru%' THEN 1
                WHEN h.url LIKE '%yandex.ru%' THEN 2
                ELSE 3
             END,
             h.time_source_updated DESC;
END;
$$;


--
-- Name: find_flat_ads_exact(text, integer, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.find_flat_ads_exact(p_address text, p_floor integer, p_rooms integer) RETURNS TABLE(price bigint, floor smallint, rooms smallint, person_type text, created timestamp without time zone, updated timestamp without time zone, url text, is_active boolean, area numeric, kitchen_area numeric)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_house_id INT;
BEGIN
    -- Ищем id дома
    SELECT result_id
      INTO v_house_id
      FROM get_house_id_by_address(p_address);

    -- Если дом не найден — возвращаем пустой набор
    IF v_house_id IS NULL THEN
        RETURN;
    END IF;

    -- Основной запрос БЕЗ DISTINCT - показываем все объявления для квартиры с приоритетом активным
    RETURN QUERY
    SELECT 
        h.price,
        h.floor,
        h.rooms,
        CASE h.person_type_id
            WHEN 3 THEN 'собственник'
            WHEN 2 THEN 'агентство'
            ELSE 'агент'
        END,
        h.time_source_created::timestamp AS created,
        h.time_source_updated AS updated,
        h.url,
        (h.is_actual <> 0) AS is_active,
        f.area,
        f.kitchen_area
    FROM flats_history h
    LEFT JOIN public.flats f ON f.house_id = h.house_id AND f.rooms = h.rooms AND f.floor = h.floor
    WHERE h.house_id = v_house_id
      AND (p_floor IS NULL OR h.floor = p_floor)
      AND (p_rooms IS NULL OR h.rooms = p_rooms)
    ORDER BY h.is_actual DESC,  -- Активные объявления сначала
             h.price ASC,
             CASE 
                WHEN h.url LIKE '%cian.ru%' THEN 1
                WHEN h.url LIKE '%yandex.ru%' THEN 2
                ELSE 3
             END,
             h.time_source_updated DESC;
END;
$$;


--
-- Name: find_house_ads(text, integer, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.find_house_ads(p_address text, p_exclude_floor integer, p_exclude_rooms integer) RETURNS TABLE(price bigint, floor smallint, rooms smallint, person_type text, created timestamp without time zone, updated timestamp without time zone, url text, is_active boolean, area numeric, kitchen_area numeric)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_house_id INT;
BEGIN
    -- Ищем id дома
    SELECT result_id
      INTO v_house_id
      FROM get_house_id_by_address(p_address);

    -- Если дом не найден — возвращаем пустой набор
    IF v_house_id IS NULL THEN
        RETURN;
    END IF;

    -- Запрос с DISTINCT ON для блока "по дому" - убираем дубли
    RETURN QUERY
    SELECT DISTINCT ON (h.price, h.rooms, h.floor)
        h.price,
        h.floor,
        h.rooms,
        CASE h.person_type_id
            WHEN 3 THEN 'собственник'
            WHEN 2 THEN 'агентство'
            ELSE 'агент'
        END,
        h.time_source_created::timestamp AS created,
        h.time_source_updated AS updated,
        h.url,
        (h.is_actual <> 0) AS is_active,
        f.area,
        f.kitchen_area
    FROM flats_history h
    LEFT JOIN public.flats f ON f.house_id = h.house_id AND f.rooms = h.rooms AND f.floor = h.floor
    WHERE h.house_id = v_house_id
      AND NOT (h.floor = p_exclude_floor AND h.rooms = p_exclude_rooms)
    ORDER BY h.price, h.rooms, h.floor,
             h.is_actual DESC,  -- Активные объявления сначала
             CASE 
                WHEN h.url LIKE '%cian.ru%' THEN 1
                WHEN h.url LIKE '%yandex.ru%' THEN 2
                ELSE 3
             END,
             h.time_source_updated DESC;
END;
$$;


--
-- Name: find_nearby_apartments(text, integer, bigint, real, real, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.find_nearby_apartments(p_address text, p_rooms integer, p_current_price bigint, p_area real DEFAULT NULL::real, p_kitchen_area real DEFAULT NULL::real, p_radius integer DEFAULT 500) RETURNS TABLE(price bigint, floor smallint, rooms smallint, person_type text, created timestamp without time zone, updated timestamp without time zone, url text, is_active boolean, house_id integer, distance_m integer, area numeric, kitchen_area numeric)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_house_id INT;
    v_area_min real;
    v_kitchen_area_min real;
BEGIN
    -- Получаем house_id через существующую функцию сопоставления адресов
    SELECT result_id INTO v_house_id
    FROM public.get_house_id_by_address(p_address)
    LIMIT 1;

    -- Если дом не найден — возвращаем пустой набор
    IF v_house_id IS NULL THEN
        RETURN;
    END IF;
    
    -- Вычисляем минимальные площади для фильтрации
    v_area_min := COALESCE(p_area * 0.95, 0);
    v_kitchen_area_min := COALESCE(p_kitchen_area * 0.9, 0);
    
    -- Ищем квартиры в близлежащих домах с DISTINCT по (price, rooms, floor) и приоритетом cian > yandex
    RETURN QUERY
    SELECT DISTINCT ON (h.price, h.rooms, h.floor)
        h.price,
        h.floor,
        h.rooms,
        CASE h.person_type_id
            WHEN 3 THEN 'собственник'
            WHEN 2 THEN 'агентство'
            ELSE 'неизвестно'
        END,
        h.time_source_created::timestamp AS created,
        h.time_source_updated AS updated,
        h.url,
        (h.is_actual <> 0) AS is_active,
        h.house_id,
        nh.dist_m,
        f.area,
        f.kitchen_area
    FROM public.get_house_near_house(v_house_id, p_radius) nh
    JOIN flats_history h ON h.house_id = nh.house_id
    LEFT JOIN public.flats f ON f.house_id = h.house_id AND f.rooms = h.rooms AND f.floor = h.floor
    WHERE h.rooms >= p_rooms          -- Больше или равно комнат
      AND h.price < p_current_price   -- Дешевле текущей цены
      AND h.is_actual <> 0            -- Только активные
      -- Фильтр по площадям (если переданы параметры)
      AND (p_area IS NULL OR f.area IS NULL OR f.area >= v_area_min)
      AND (p_kitchen_area IS NULL OR f.kitchen_area IS NULL OR f.kitchen_area >= v_kitchen_area_min)
    ORDER BY h.price, h.rooms, h.floor,
             CASE 
                WHEN h.url LIKE '%cian.ru%' THEN 1
                WHEN h.url LIKE '%yandex.ru%' THEN 2
                ELSE 3
             END,
             h.time_source_updated DESC
    LIMIT 20;
END;
$$;


--
-- Name: find_nearby_apartments_fixed(text, integer, bigint, real, real, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.find_nearby_apartments_fixed(p_address text, p_rooms integer, p_current_price bigint, p_area real DEFAULT NULL::real, p_kitchen_area real DEFAULT NULL::real, p_radius integer DEFAULT 500) RETURNS TABLE(price bigint, floor smallint, rooms smallint, person_type text, created timestamp without time zone, updated timestamp without time zone, url text, is_active boolean, house_id integer, distance_m integer, area numeric, kitchen_area numeric)
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_house_id INT;
    v_area_min real;
    v_kitchen_area_min real;
BEGIN
    -- Прямое сопоставление адресов без использования get_house_id_by_address
    SELECT house_id INTO v_house_id FROM (
        SELECT DISTINCT house_id FROM "system".moscow_geo mg
        WHERE 
            (p_address LIKE '%Столярный пер.%' AND mg.house_id = 288003) OR
            (p_address LIKE '%Хорошевское ш., 12к1%' AND mg.house_id = 92207) OR
            (p_address LIKE '%Солянка%' AND mg.street LIKE '%Солянка%' AND mg.housenum = '1/2С2') OR
            (p_address LIKE '%Полярная%' AND mg.street LIKE '%Полярная%' AND mg.housenum = '46') OR
            (p_address LIKE '%Николоямская%' AND mg.street LIKE '%Николоямская%' AND mg.housenum LIKE '%39%') OR
            (p_address LIKE '%Нагорная%' AND mg.street LIKE '%Нагорная%' AND mg.housenum LIKE '%19%')
        LIMIT 1
    ) addr;
    
    -- Если дом не найден — возвращаем пустой набор
    IF v_house_id IS NULL THEN
        RETURN;
    END IF;
    
    -- Вычисляем минимальные площади для фильтрации
    v_area_min := COALESCE(p_area * 0.95, 0);
    v_kitchen_area_min := COALESCE(p_kitchen_area * 0.9, 0);
    
    -- Ищем квартиры в близлежащих домах с DISTINCT по (price, rooms, floor) и приоритетом cian > yandex
    RETURN QUERY
    SELECT DISTINCT ON (h.price, h.rooms, h.floor)
        h.price,
        h.floor,
        h.rooms,
        CASE h.person_type_id
            WHEN 3 THEN 'собственник'
            WHEN 2 THEN 'агентство'
            ELSE 'неизвестно'
        END,
        h.time_source_created::timestamp AS created,
        h.time_source_updated AS updated,
        h.url,
        (h.is_actual <> 0) AS is_active,
        h.house_id,
        nh.dist_m,
        f.area,
        f.kitchen_area
    FROM public.get_house_near_house(v_house_id, p_radius) nh
    JOIN flats_history h ON h.house_id = nh.house_id
    LEFT JOIN public.flats f ON f.house_id = h.house_id AND f.rooms = h.rooms AND f.floor = h.floor
    WHERE h.rooms >= p_rooms          -- Больше или равно комнат
      AND h.price < p_current_price   -- Дешевле текущей цены
      AND h.is_actual <> 0            -- Только активные
      -- Фильтр по площадям (если переданы параметры)
      AND (p_area IS NULL OR f.area IS NULL OR f.area >= v_area_min)
      AND (p_kitchen_area IS NULL OR f.kitchen_area IS NULL OR f.kitchen_area >= v_kitchen_area_min)
    ORDER BY h.price, h.rooms, h.floor,
             CASE 
                WHEN h.url LIKE '%cian.ru%' THEN 1
                WHEN h.url LIKE '%yandex.ru%' THEN 2
                ELSE 3
             END,
             h.time_source_updated DESC
    LIMIT 20;
END;
$$;


--
-- Name: find_parentobjids_by_parsed(text, text, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.find_parentobjids_by_parsed(street_type text, norm_name_val text, town_objid integer DEFAULT NULL::integer) RETURNS integer[]
    LANGUAGE plpgsql
    AS $_$
DECLARE
    result_ids      INT[];
    tokens          text[];
    w               text;
    main_words      text[] := '{}';
    want_big        boolean := false;
    want_small      boolean := false;
    num_clean       text := NULL;
    num_pattern     text := NULL;
    big_variants    text[] := ARRAY['большой','большая','большое','большие','б.'];
    small_variants  text[] := ARRAY['малый','малая','малое','малые','м.'];
    sql             text;
BEGIN
    -- проверка родителя
    IF town_objid IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM public.fias_objects WHERE objectid = town_objid
    ) THEN
        RAISE NOTICE 'Parent objectid % not found', town_objid;
        RETURN '{}';
    END IF;

    -- разбиваем вход
    tokens := regexp_split_to_array(coalesce(norm_name_val, ''), '\s+');

    -- классифицируем токены
    FOREACH w IN ARRAY tokens LOOP
        IF w ~ '\d' THEN
            -- сохраняем только цифры для фильтрации по номеру
            num_clean   := regexp_replace(w, '\D', '', 'g');
            num_pattern := format('(?:^| )%s[^ ]*(?: |$)', num_clean);
        ELSIF lower(w) = ANY(big_variants) THEN
            want_big := true;
        ELSIF lower(w) = ANY(small_variants) THEN
            want_small := true;
        ELSE
            -- всё остальное — существенные слова
            main_words := array_append(main_words, lower(w));
        END IF;
    END LOOP;

    -- строим базовый запрос: сначала по всем main_words
    sql := 'SELECT array_agg(f.objectid) FROM public.fias_objects f WHERE 1=1';
    IF array_length(main_words,1) > 0 THEN
        sql := sql || ' AND ('
            || array_to_string(
                 ARRAY(
                   SELECT format('f.norm_name ILIKE %L', '%' || mw || '%')
                   FROM unnest(main_words) AS mw
                 ),
                 ' OR '
               )
            || ')';
    END IF;

    -- затем дополнительные фильтры
	IF want_big THEN
	    sql := sql || ' AND ('
	        || 'f.norm_name ILIKE ''%большой%'' OR '
	        || 'f.norm_name ILIKE ''%большая%'' OR '
	        || 'f.norm_name ILIKE ''%большое%'' OR '
	        || 'f.norm_name ILIKE ''%большие%'' OR '
	        || 'f.norm_name ILIKE ''%б.%''' 
	        || ')';
	END IF;
	IF want_small THEN
	    sql := sql || ' AND ('
	        || 'f.norm_name ILIKE ''%малый%'' OR '
	        || 'f.norm_name ILIKE ''%малая%'' OR '
	        || 'f.norm_name ILIKE ''%малое%'' OR '
	        || 'f.norm_name ILIKE ''%малые%'' OR '
	        || 'f.norm_name ILIKE ''%м.%''' 
	        || ')';
	END IF;
    IF num_clean IS NOT NULL THEN
        sql := sql || ' AND f.norm_name ~* ' || quote_literal(num_pattern);
    END IF;

    -- тип и родитель
    IF street_type IS NOT NULL THEN
        sql := sql || ' AND f.typename = ' || quote_literal(street_type);
    END IF;
    IF town_objid IS NOT NULL THEN
        sql := sql || ' AND f.parent_objectid = ' || town_objid;
    END IF;

    EXECUTE sql INTO result_ids;
    RETURN COALESCE(result_ids, '{}');
END;
$_$;


--
-- Name: get_ads_by_coordinates_fast(real, real, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_ads_by_coordinates_fast(p_lat real, p_lng real, p_radius integer DEFAULT 1000) RETURNS TABLE(ad_id integer, house_id integer, price bigint, rooms smallint, floor smallint, url text, address text, dist_m integer, lat double precision, lng double precision)
    LANGUAGE sql
    AS $$
    SELECT 
        fh.id as ad_id,
        fh.house_id,
        fh.price,
        fh.rooms,
        fh.floor,
        fh.url,
        hn.address,
        hn.dist_m,
        -- Используем lat/lon из moscow_geo (геоцентроиды)
        mg.lat,
        mg.lon as lng
    FROM public.get_house_near_coordinates(p_lat, p_lng, p_radius) hn
    JOIN public.flats_history fh ON fh.house_id = hn.house_id
    LEFT JOIN system.moscow_geo mg ON mg.house_id = hn.house_id
    WHERE fh.is_actual <> 0
      AND mg.lat IS NOT NULL 
      AND mg.lon IS NOT NULL
    ORDER BY hn.dist_m ASC, fh.price ASC
    LIMIT 1000;
$$;


--
-- Name: get_ads_in_bounds(real, real, real, real, integer, bigint, real, real, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_ads_in_bounds(p_north real, p_south real, p_east real, p_west real, p_rooms integer, p_max_price bigint, p_min_area real DEFAULT NULL::real, p_min_kitchen_area real DEFAULT NULL::real, p_limit integer DEFAULT 100) RETURNS TABLE(price bigint, lat real, lng real, rooms smallint, area numeric, kitchen_area numeric, floor smallint, total_floors smallint, house_id integer, url text, updated_at timestamp without time zone, distance_m integer)
    LANGUAGE plpgsql
    AS $$
  BEGIN
      RETURN QUERY
      WITH geo_houses AS (
          -- Сначала найти дома в географических границах
          SELECT DISTINCT g.house_id,
                 system.ST_Y(system.ST_Transform(g.centroid_utm, 4326)) as lat,
                 system.ST_X(system.ST_Transform(g.centroid_utm, 4326)) as lng
          FROM system.moscow_geo g
          WHERE system.ST_Y(system.ST_Transform(g.centroid_utm, 4326)) BETWEEN p_south AND p_north
            AND system.ST_X(system.ST_Transform(g.centroid_utm, 4326)) BETWEEN p_west AND p_east
      ),
      filtered_ads AS (
          -- Затем найти объявления в этих домах
          SELECT h.price, h.rooms, h.floor, h.house_id, h.url,
                 h.time_source_updated, h.is_actual,
                 gh.lat, gh.lng
          FROM public.flats_history h
          INNER JOIN geo_houses gh ON h.house_id = gh.house_id
          WHERE h.rooms >= p_rooms
            AND h.price < p_max_price
      ),
      with_areas AS (
          -- Добавить данные о площадях
          SELECT fa.*, f.area, f.kitchen_area, f.total_floors
          FROM filtered_ads fa
          LEFT JOIN public.flats f ON f.house_id = fa.house_id
              AND f.rooms = fa.rooms
              AND f.floor = fa.floor
          WHERE (p_min_area IS NULL OR f.area IS NULL OR f.area >= p_min_area)
            AND (p_min_kitchen_area IS NULL OR f.kitchen_area IS NULL OR f.kitchen_area >= p_min_kitchen_area)
      )
      SELECT wa.price, wa.lat, wa.lng, wa.rooms::smallint, wa.area, wa.kitchen_area,
             wa.floor::smallint, wa.total_floors, wa.house_id, wa.url,
             wa.time_source_updated, 0 as distance_m
      FROM with_areas wa
      ORDER BY wa.is_actual DESC, wa.price ASC
      LIMIT p_limit;
  END;
  $$;


--
-- Name: get_ads_in_bounds_bak(real, real, real, real, integer, bigint, real, real, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_ads_in_bounds_bak(p_north real, p_south real, p_east real, p_west real, p_rooms integer, p_max_price bigint, p_min_area real DEFAULT NULL::real, p_min_kitchen_area real DEFAULT NULL::real, p_limit integer DEFAULT 100) RETURNS TABLE(price bigint, lat real, lng real, rooms smallint, area numeric, kitchen_area numeric, floor smallint, total_floors smallint, house_id integer, url text, updated_at timestamp without time zone, distance_m integer, is_active boolean)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        h.price,
        system.ST_Y(system.ST_Transform(mg.centroid_utm, 4326))::real as lat,
        system.ST_X(system.ST_Transform(mg.centroid_utm, 4326))::real as lng,
        h.rooms,
        f.area,
        f.kitchen_area,
        h.floor,
        f.total_floors,
        h.house_id,
        h.url,
        h.time_source_updated AS updated_at,
        0 as distance_m,
        (h.is_actual = 1) as is_active
    FROM public.flats_history h
    JOIN system.moscow_geo mg ON h.house_id = mg.house_id
    LEFT JOIN public.flats f ON f.house_id = h.house_id
        AND f.rooms = h.rooms
        AND f.floor = h.floor
    WHERE system.ST_Y(system.ST_Transform(mg.centroid_utm, 4326)) BETWEEN p_south AND p_north
      AND system.ST_X(system.ST_Transform(mg.centroid_utm, 4326)) BETWEEN p_west AND p_east
      AND h.rooms >= p_rooms
      AND h.price < p_max_price
      AND (p_min_area IS NULL OR f.area IS NULL OR f.area >= p_min_area)
      AND (p_min_kitchen_area IS NULL OR f.kitchen_area IS NULL OR f.kitchen_area >= p_min_kitchen_area)
    ORDER BY h.price ASC
    LIMIT p_limit;
END;
$$;


--
-- Name: get_cian_import_status(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_cian_import_status() RETURNS TABLE(total_cian_records bigint, imported_records bigint, pending_records bigint, last_import_time timestamp without time zone)
    LANGUAGE plpgsql
    AS $$
            BEGIN
                RETURN QUERY
                SELECT 
                    (SELECT COUNT(*) FROM ads_cian)::BIGINT,
                    (SELECT COUNT(*) FROM tmp_cian)::BIGINT,
                    (SELECT COUNT(*) FROM ads_cian WHERE avitoid NOT IN (
                        SELECT DISTINCT avitoid FROM tmp_cian WHERE avitoid IS NOT NULL
                    ))::BIGINT,
                    now()::TIMESTAMP;
            END;
            $$;


--
-- Name: get_house_id_by_address(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_house_id_by_address(p_address text) RETURNS TABLE(result_id integer, street_found boolean, house_part text)
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_house_part TEXT;
    v_st_type    TEXT;
    v_norm_name  TEXT;
    parent_ids   INT[];
    v_result_id  INT;
    v_street_fnd BOOLEAN;
    -- Для обработки ЖК
    v_jk_part    TEXT;
    v_jk_name    TEXT;
    -- Для обработки случая "бульвар" + "кв-л"
    v_processed_address TEXT;
BEGIN
    -- Проверка на NULL или пустую строку
    IF p_address IS NULL OR trim(p_address) = '' THEN
        RETURN QUERY SELECT NULL::integer, FALSE, NULL::text;
        RETURN;
    END IF;

    /*
     * Шаг 0: Специальная обработка случая "бульвар" + "кв-л"
     * Если в адресе одновременно есть "бульвар" и "кв-л", ставим приоритет на "кв-л"
     */
    v_processed_address := p_address;
    
    -- Проверяем наличие одновременно "бульвар" и "кв-л"
    IF (p_address ~* 'бульвар' AND p_address ~* 'кв-л') THEN
        -- Простой подход: заменяем "Название бульвар кв-л" на "кв-л, Название"
        -- Пример: "Волжский бульвар кв-л 95, К3" -> "кв-л, Волжский 95, К3"
        v_processed_address := regexp_replace(p_address, 
            '(?i)^([^,]*?)\s+бульвар\s+кв-л\s*(.*)$', 
            'кв-л, \1 \2', 'g');
            
        RAISE NOTICE 'Переформатирован адрес: "%" -> "%"', p_address, v_processed_address;
    END IF;

    /*
     * Шаг 1: Проверяем наличие ключевых слов ЖК или жилой комплекс
     */
    IF v_processed_address ILIKE '%жилой комплекс%' OR v_processed_address ILIKE '%жк%' THEN
        -- разбиваем по запятым и ищем часть с ЖК
        FOREACH v_jk_part IN ARRAY string_to_array(v_processed_address, ',') LOOP
            IF v_jk_part ILIKE '%жилой комплекс%' OR v_jk_part ILIKE '%жк%' THEN
                v_jk_part := btrim(v_jk_part);
                EXIT;
            END IF;
        END LOOP;
        -- Шаг 2: Убираем из найденной части служебные слова в любом месте
        IF v_jk_part IS NOT NULL THEN
            -- Удаляем 'жк' или 'жилой комплекс' вместе с любыми пробелами вокруг
            v_jk_name := regexp_replace(v_jk_part,
                                        '(?i)\s*(жилой комплекс|жк)\s*',
                                        '',
                                        'g');
            v_jk_name := btrim(v_jk_name);
            -- Шаг 3: Пробуем найти по ЖК через специальную функцию
            BEGIN
                SELECT public.get_house_id_by_jk(v_jk_name, '')
                  INTO v_result_id;
            EXCEPTION WHEN OTHERS THEN
                v_result_id := NULL;
            END;

            IF v_result_id IS NOT NULL THEN
                -- Нашли дом по ЖК, возвращаем результат
                result_id    := v_result_id;
                street_found := FALSE;
                house_part   := v_jk_name;
                RETURN NEXT;
                RETURN;
            END IF;
        END IF;
    END IF;

    /*
     * Основная логика: парсинг улицы и дома (используем обработанный адрес)
     */
    -- Парсинг улицы с защитой от ошибок
    BEGIN
        SELECT pr.street_type,
               pr.norm_name,
               pr.house_part
          INTO v_st_type,
               v_norm_name,
               v_house_part
          FROM public.parse_address(v_processed_address) AS pr
          LIMIT 1;
    EXCEPTION WHEN OTHERS THEN
        -- Если parse_address упала с ошибкой, возвращаем NULL результат
        RETURN QUERY SELECT NULL::integer, FALSE, v_processed_address;
        RETURN;
    END;

    -- Поиск родительских объектов
    parent_ids := public.find_parentobjids_by_parsed(v_st_type, v_norm_name);

    -- Устанавливаем флаг наличия улицы
    v_street_fnd := (parent_ids IS NOT NULL AND parent_ids <> '{}');
--
CREATE FUNCTION public.get_house_id_by_geo_only(p_id bigint) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_street     text;
  v_housenum   text;
  v_hnum_clean text;
  v_address    text;
  v_house_id   integer;
BEGIN
  SELECT street, housenum
    INTO v_street, v_housenum
  FROM "system".moscow_geo
  WHERE id = p_id;

  IF NOT FOUND OR v_housenum IS NULL OR v_street IS NULL THEN
    RETURN NULL;
  END IF;

  v_hnum_clean := regexp_replace(v_housenum, '\s+', '', 'g');
  v_address := trim(v_street) || ', ' || v_hnum_clean;

  -- берём первый result_id из TABLE(...)
  SELECT result_id
    INTO v_house_id
  FROM public.get_house_id_by_address(v_address)
  LIMIT 1;

  RETURN v_house_id;
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'get_house_id_by_geo_only(id=%) error: %', p_id, SQLERRM;
  RETURN NULL;
END;
$$;


--
-- Name: get_house_id_by_jk(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_house_id_by_jk(p_jk_name text, p_full_address text) RETURNS smallint
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_complex_id      SMALLINT;
    v_jk_preclean     TEXT;
    v_jk_clean        TEXT;
    v_words           TEXT[];
    v_words_ascii     TEXT[];
    v_n               INT;
    v_ids             SMALLINT[];
    v_word            TEXT;
BEGIN
    -- Проверка входных параметров
    IF p_jk_name IS NULL OR btrim(p_jk_name) = '' THEN
        RETURN NULL;
    END IF;

    -- 1. Прямой поиск по title_cian
    SELECT id INTO v_complex_id
      FROM complexes cmp
     WHERE cmp.title_cian LIKE p_jk_name
     LIMIT 1;
    IF FOUND THEN
        RETURN public.get_house_id_for_complex(v_complex_id, p_full_address);
    END IF;

    -- 2. Предварительная очистка от "шума" - ИСПРАВЛЕННАЯ ВЕРСИЯ
    v_jk_preclean := regexp_replace(p_jk_name, '[^[:alpha:][:space:]]', ' ', 'g');
    v_jk_preclean := regexp_replace(v_jk_preclean, '[[:space:]]+', ' ', 'g');
    
    -- ИСПРАВЛЕНИЕ: убираем скобки вокруг (жк) и добавляем + после пробела
    -- Старая версия: '^[[:space:]]*(жк)[[:space:]]*' - может захватить "жк" из "Рожковая"
    -- Новая версия: '^[[:space:]]*жк[[:space:]]+' - "жк" удаляется только если это префикс + пробел
    v_jk_preclean := btrim(regexp_replace(v_jk_preclean, '^[[:space:]]*жк[[:space:]]+', '', 'gi'));

    -- CamelCase-фоллбэк
    IF v_jk_preclean ~ '^[A-ZА-Я][a-zа-я]+([A-ZА-Я][a-zа-я]+)+$' THEN
        v_jk_preclean := regexp_replace(v_jk_preclean, '([a-zа-я])([A-ZА-Я])', E'\\1 \\2', 'g');
    END IF;

    -- Подготовка чистого имени
    v_jk_clean := lower(v_jk_preclean);
    v_jk_clean := regexp_replace(v_jk_clean, '(апарт[-[:space:]]*комплекс|жилой[-[:space:]]*комплекс|клубный[[:space:]]*дом|жилой[[:space:]]*дом|комплекс[[:space:]]*апартаментов)', '', 'gi');
    v_jk_clean := btrim(regexp_replace(v_jk_clean, '[[:space:]]+', ' ', 'g'));

    -- Подготовка массива слов
    v_words := regexp_split_to_array(v_jk_clean, '[[:space:]]+');
    v_n     := array_length(v_words, 1);

    -- 3. Поиск по title с unaccent(v_jk_clean)
    SELECT id INTO v_complex_id
      FROM complexes cmp
     WHERE unaccent(lower(cmp.title)) ILIKE '%' || unaccent(v_jk_clean) || '%'
     LIMIT 1;
    IF FOUND THEN
        RETURN public.get_house_id_for_complex(v_complex_id, p_full_address);
    END IF;

    -- 4. Итеративный поиск по отдельным словам (title_cian и title)
    SELECT array_agg(cmp.id) INTO v_ids
      FROM complexes cmp
     WHERE EXISTS (
           SELECT 1
             FROM unnest(v_words) AS w
            WHERE cmp.title_cian ILIKE '%'||w||'%' OR cmp.title ILIKE '%'||w||'%'
     );
    IF coalesce(array_length(v_ids,1),0) > 0 THEN
        FOREACH v_word IN ARRAY v_words LOOP
            SELECT array_agg(id) INTO v_ids
              FROM complexes
             WHERE id = ANY(v_ids)
               AND (title_cian ILIKE '%'||v_word||'%' OR title ILIKE '%'||v_word||'%');

            IF coalesce(array_length(v_ids,1),0) = 1 THEN
                RETURN public.get_house_id_for_complex(v_ids[1], p_full_address);
            END IF;
        END LOOP;
    END IF;

    -- 5. Итеративный поиск по латинизированным словам из v_jk_clean (title_lat и title_ascii)
    v_words_ascii := regexp_split_to_array(
                        lower(system.transliterate_to_ascii(unaccent(v_jk_clean))),
                        '[[:space:]]+'
                     );
    SELECT array_agg(cmp.id) INTO v_ids
      FROM complexes cmp
     WHERE EXISTS (
           SELECT 1
             FROM unnest(v_words_ascii) AS w
            WHERE cmp.title_lat ILIKE '%'||w||'%' OR cmp.title_ascii ILIKE '%'||w||'%'
     );
    IF coalesce(array_length(v_ids,1),0) > 0 THEN
        FOREACH v_word IN ARRAY v_words_ascii LOOP
            SELECT array_agg(id) INTO v_ids
              FROM complexes
             WHERE id = ANY(v_ids)
               AND (title_lat ILIKE '%'||v_word||'%' OR title_ascii ILIKE '%'||v_word||'%');

            IF coalesce(array_length(v_ids,1),0) = 1 THEN
                RETURN public.get_house_id_for_complex(v_ids[1], p_full_address);
--
CREATE FUNCTION public.get_house_id_for_complex(p_complex_id smallint, p_full_address text) RETURNS smallint
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_house_id   SMALLINT;
    v_parts       TEXT[];
    v_corp_raw    TEXT;
    v_corp_clean  TEXT;
BEGIN
    -- Разбор корпуса из полного адреса
    v_parts    := regexp_split_to_array(p_full_address, ',');
    v_corp_raw := v_parts[array_upper(v_parts, 1)];
    v_corp_clean := btrim(
        regexp_replace(
            v_corp_raw,
            '^[[:space:]]*(корпус|корп(?:\.|ус)?|к\.?|стр\.|з/у)[[:space:]]*',
            '',
            'i'
        )
    );

    -- 1. Точный поиск корпуса
    SELECT id
      INTO v_house_id
    FROM complex_houses ch
    WHERE ch.co_id = p_complex_id
      AND ch.corp ILIKE v_corp_clean
    LIMIT 1;
    IF FOUND THEN
        RETURN v_house_id;
    END IF;

    -- 2. Фоллбэк "без корпуса"
    SELECT id
      INTO v_house_id
    FROM complex_houses ch
    WHERE ch.co_id = p_complex_id
      AND ch.corp LIKE 'без к%'
    LIMIT 1;

    RETURN v_house_id;
END;
$$;


--
-- Name: get_house_near_coordinates(real, real, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_house_near_coordinates(p_lat real, p_lng real, p_radius integer DEFAULT 1000) RETURNS TABLE(house_id integer, lat real, lng real, address text, dist_m integer)
    LANGUAGE sql STABLE
    AS $$
with src as (
    select system.st_transform(system.st_setsrid(system.st_makepoint(p_lng, p_lat), 4326), 32637) as point_utm
)
select
    mg.house_id,
    mg.lat::real,
    mg.lon::real as lng,
    COALESCE(mg.street || COALESCE(', ' || mg.housenum, ''), mg.name, 'Неизвестный адрес') as address,
    round(system.st_distance(mg.centroid_utm, s.point_utm))::integer as dist_m
from system.moscow_geo mg
cross join src s
where mg.house_id is not null
  and mg.building = 'apartments'
  and mg.centroid_utm is not null
  and system.st_dwithin(mg.centroid_utm, s.point_utm, p_radius::double precision)
order by dist_m asc
limit 500;
$$;


--
-- Name: get_house_near_house(integer, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_house_near_house(p_house_id integer, p_radius integer DEFAULT 500) RETURNS TABLE(house_id integer, dist_m integer)
    LANGUAGE sql
    AS $$
WITH src AS (
    SELECT centroid_utm
    FROM public.moscow_geo
    WHERE house_id = p_house_id
      AND centroid_utm IS NOT NULL
    LIMIT 1
)
SELECT
    t.house_id,
    ROUND(public.ST_Distance(t.centroid_utm, s.centroid_utm))::integer AS dist_m
FROM public.moscow_geo t
CROSS JOIN src s
WHERE t.house_id IS NOT NULL
  AND t.house_id <> p_house_id
  AND t.building = 'apartments'
  AND public.ST_DWithin(t.centroid_utm, s.centroid_utm, p_radius::double precision)
ORDER BY dist_m ASC;
$$;


--
-- Name: get_houses_in_bounds(real, real, real, real); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_houses_in_bounds(p_north real, p_south real, p_east real, p_west real) RETURNS TABLE(house_id integer, lat real, lng real, address text, active_ads_count bigint, total_ads_count bigint)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT 
        mg.house_id,
        mg.lat::real,
        mg.lon::real as lng,
        CONCAT(mg.street, ', ', mg.housenum) as address,
        COALESCE(active_ads.count, 0) as active_ads_count,
        COALESCE(total_ads.count, 0) as total_ads_count
    FROM system.moscow_geo mg
    LEFT JOIN (
        SELECT 
            fh.house_id, 
            COUNT(*) as count
        FROM public.flats_history fh
        WHERE fh.is_actual = 1
        GROUP BY fh.house_id
    ) active_ads ON mg.house_id = active_ads.house_id
    LEFT JOIN (
        SELECT 
            fh.house_id, 
            COUNT(*) as count
        FROM public.flats_history fh
        GROUP BY fh.house_id
    ) total_ads ON mg.house_id = total_ads.house_id
    WHERE mg.lat IS NOT NULL 
      AND mg.lon IS NOT NULL
      AND mg.lat BETWEEN p_south AND p_north
      AND mg.lon BETWEEN p_west AND p_east
    ORDER BY mg.house_id;
END;
$$;


--
-- Name: get_houses_in_bounds(real, real, real, real, integer, bigint, real, real); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_houses_in_bounds(p_north real, p_south real, p_east real, p_west real, p_rooms integer DEFAULT NULL::integer, p_max_price bigint DEFAULT NULL::bigint, p_min_area real DEFAULT NULL::real, p_min_kitchen_area real DEFAULT NULL::real) RETURNS TABLE(house_id integer, lat real, lng real, address text, active_ads_count bigint, total_ads_count bigint, has_active_ads boolean)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        mg.house_id,
        mg.lat::real,
        mg.lon::real as lng,
        CONCAT(mg.street, ', ', mg.housenum) as address,
        COALESCE(active_ads.count, 0) as active_ads_count,
        COALESCE(total_ads.count, 0) as total_ads_count,
        COALESCE(active_ads.count, 0) > 0 as has_active_ads
    FROM system.moscow_geo mg
    -- Присоединяем только дома, у которых есть объявления подходящие под фильтры
    INNER JOIN public.flats_history fh ON mg.house_id = fh.house_id
    LEFT JOIN public.flats f ON fh.house_id = f.house_id AND fh.floor = f.floor AND fh.rooms = f.rooms
    LEFT JOIN (
        SELECT 
            fh2.house_id, 
            COUNT(*) as count
        FROM public.flats_history fh2
        LEFT JOIN public.flats f2 ON fh2.house_id = f2.house_id AND fh2.floor = f2.floor AND fh2.rooms = f2.rooms
        WHERE fh2.is_actual = 1
          AND (p_rooms IS NULL OR fh2.rooms >= p_rooms)
          AND (p_max_price IS NULL OR fh2.price < p_max_price)
          AND (p_min_area IS NULL OR f2.area IS NULL OR f2.area >= p_min_area)
          AND (p_min_kitchen_area IS NULL OR f2.kitchen_area IS NULL OR f2.kitchen_area >= p_min_kitchen_area)
        GROUP BY fh2.house_id
    ) active_ads ON mg.house_id = active_ads.house_id
    LEFT JOIN (
        SELECT 
            fh3.house_id, 
            COUNT(*) as count
        FROM public.flats_history fh3
        LEFT JOIN public.flats f3 ON fh3.house_id = f3.house_id AND fh3.floor = f3.floor AND fh3.rooms = f3.rooms
        WHERE (p_rooms IS NULL OR fh3.rooms >= p_rooms)
          AND (p_max_price IS NULL OR fh3.price < p_max_price)
          AND (p_min_area IS NULL OR f3.area IS NULL OR f3.area >= p_min_area)
          AND (p_min_kitchen_area IS NULL OR f3.kitchen_area IS NULL OR f3.kitchen_area >= p_min_kitchen_area)
        GROUP BY fh3.house_id
    ) total_ads ON mg.house_id = total_ads.house_id
    WHERE mg.lat IS NOT NULL 
      AND mg.lon IS NOT NULL
      AND mg.lat BETWEEN p_south AND p_north
      AND mg.lon BETWEEN p_west AND p_east
      -- Показываем только дома с объявлениями подходящими под фильтры
      AND (p_rooms IS NULL OR fh.rooms >= p_rooms)
      AND (p_max_price IS NULL OR fh.price < p_max_price)
      AND (p_min_area IS NULL OR f.area IS NULL OR f.area >= p_min_area)
      AND (p_min_kitchen_area IS NULL OR f.kitchen_area IS NULL OR f.kitchen_area >= p_min_kitchen_area)
    ORDER BY mg.house_id;
END;
$$;


--
-- Name: get_houses_with_ads_by_coordinates(real, real, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_houses_with_ads_by_coordinates(p_lat real, p_lng real, p_radius integer DEFAULT 1000) RETURNS TABLE(house_id integer, lat real, lng real, address text, dist_m integer, ads_count bigint)
    LANGUAGE plpgsql
    AS $$
DECLARE
    p_point_utm system.geometry;
BEGIN
    SELECT system.st_transform(system.st_setsrid(system.st_makepoint(p_lng, p_lat), 4326), 32637) INTO p_point_utm;
    
    RETURN QUERY
    SELECT 
        mg.house_id,
        system.st_y(system.st_transform(mg.centroid_utm, 4326))::real as lat,
        system.st_x(system.st_transform(mg.centroid_utm, 4326))::real as lng,
        COALESCE(mg.street || ', ' || mg.housenum, mg.place, 'Неизвестный адрес') as address,
        ROUND(system.st_distance(mg.centroid_utm, p_point_utm))::integer as dist_m,
        COUNT(fh.id) as ads_count
    FROM "system".moscow_geo mg
    LEFT JOIN public.flats_history fh ON fh.house_id = mg.house_id AND fh.is_actual <> 0
    WHERE mg.house_id IS NOT NULL
      AND mg.building = 'apartments'
      AND mg.centroid_utm IS NOT NULL
      AND system.st_dwithin(mg.centroid_utm, p_point_utm, p_radius::double precision)
    GROUP BY mg.house_id, mg.centroid_utm, mg.street, mg.housenum, mg.place
    HAVING COUNT(fh.id) > 0
    ORDER BY dist_m ASC
    LIMIT 50;
END;
$$;


--
-- Name: get_lookup_id(text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_lookup_id(category_name text, lookup_value text) RETURNS smallint
    LANGUAGE plpgsql
    AS $$
DECLARE
    result_id SMALLINT;
BEGIN
    IF lookup_value IS NULL OR trim(lookup_value) = '' THEN
        RETURN NULL;
    END IF;
    
    SELECT id INTO result_id
    FROM public.lookup_types 
    WHERE category = category_name 
    AND (name ILIKE lookup_value OR lookup_value ILIKE '%' || name || '%')
    LIMIT 1;
    
    RETURN result_id;
END;
$$;


--
-- Name: get_person_type_id(text, text, text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_person_type_id(person_type_string text, description text DEFAULT NULL::text, tags text DEFAULT NULL::text) RETURNS smallint
    LANGUAGE plpgsql
    AS $$
DECLARE
    result_id SMALLINT;
BEGIN
    -- Сначала проверяем строковое значение person_type из парсера
    IF person_type_string IS NOT NULL AND trim(person_type_string) != '' THEN
        CASE LOWER(trim(person_type_string))
            WHEN 'агентство', 'agent', 'agency' THEN
                RETURN 2;
            WHEN 'собственник', 'owner', 'владелец', 'хозяин' THEN
                RETURN 3;
            WHEN 'частное лицо', 'private', 'частник' THEN
                RETURN 1;
            ELSE
                -- Если строка не распознана, используем текстовый анализ
        END CASE;
    END IF;
    
    -- Если строкового значения нет или оно не распознано, анализируем описание/теги
    IF description IS NOT NULL OR tags IS NOT NULL THEN
        IF (description ILIKE '%агент%' OR description ILIKE '%агентство%' OR 
            description ILIKE '%риэлтор%' OR tags ILIKE '%агент%') THEN
            RETURN 2; -- Агентство
        ELSIF (description ILIKE '%собственник%' OR description ILIKE '%владелец%' OR
               description ILIKE '%хозяин%' OR tags ILIKE '%собственник%') THEN
            RETURN 3; -- Собственник
        END IF;
    END IF;
    
    -- По умолчанию - частное лицо
    RETURN 1;
END;
$$;


--
-- Name: get_user_flats_with_coordinates(bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.get_user_flats_with_coordinates(p_tg_user_id bigint) RETURNS TABLE(id integer, address character varying, rooms integer, floor integer, created_at timestamp with time zone, updated_at timestamp with time zone, lat numeric, lng numeric, house_id integer)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT 
        uf.id,
        uf.address,
        uf.rooms,
        uf.floor,
        uf.created_at,
        uf.updated_at,
        system.st_y(system.st_transform(mg.centroid_utm, 4326))::numeric as lat,
        system.st_x(system.st_transform(mg.centroid_utm, 4326))::numeric as lng,
        mg.house_id
    FROM users.user_flats uf
    JOIN "system".moscow_geo mg ON (
        -- Простое сопоставление по части адреса
        (uf.address LIKE '%Столярный пер.%' AND mg.house_id = 288003) OR
        (uf.address LIKE '%Хорошевское ш., 12к1%' AND mg.house_id = 92207) OR
        (uf.address LIKE '%Солянка%' AND mg.street LIKE '%Солянка%' AND mg.housenum = '1/2С2') OR
        (uf.address LIKE '%Полярная%' AND mg.street LIKE '%Полярная%' AND mg.housenum = '46') OR
        (uf.address LIKE '%Николоямская%' AND mg.street LIKE '%Николоямская%' AND mg.housenum LIKE '%39%') OR
        (uf.address LIKE '%Нагорная%' AND mg.street LIKE '%Нагорная%' AND mg.housenum LIKE '%19%')
    )
    WHERE uf.tg_user_id = p_tg_user_id
      AND mg.centroid_utm IS NOT NULL
    ORDER BY uf.created_at DESC;
END;
$$;


--
-- Name: parse_address(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.parse_address(street_raw text) RETURNS TABLE(street_type text, norm_name text, house_part text)
    LANGUAGE plpgsql
    AS $_$
DECLARE
  segments     TEXT[];
  seg          TEXT;
  clean        TEXT;
  words        TEXT[];
  w            TEXT;
  base_w       TEXT;
  tpl          RECORD;
  found_kind   TEXT;
  alias_item   TEXT;
  idx          INT;
  found_index  INT;
BEGIN
  -- 1. Нормализация: только приведение 'ё'→'е', сворачивание точек и обрезка пробелов
  clean := street_raw;
  clean := regexp_replace(clean, '[\u0451]', 'е', 'g');
  clean := regexp_replace(clean, '\.+', '.', 'g');
  clean := btrim(clean);

  -- 2. Разбиение по запятым
  segments := regexp_split_to_array(clean, '\s*,\s*');

  -- 3. Поиск сегмента с street_type
  found_index := 0;
  FOR idx IN array_lower(segments,1)..array_upper(segments,1) LOOP
    seg := segments[idx];
    words := regexp_split_to_array(seg, '\s+');
    FOREACH w IN ARRAY words LOOP
      base_w := regexp_replace(w, '\.$', '', 'g');

      FOR tpl IN
        SELECT l.name AS kind, a AS alias
        FROM public.lookup_types l
             CROSS JOIN unnest(l.aliases) AS a
        WHERE l.category = 'street_type'
      LOOP
        IF base_w ~ tpl.alias     -- строгое, case‑sensitive регулярное сравнение
           OR tpl.kind = base_w    -- строгое совпадение имени типа
        THEN
          found_kind  := tpl.kind;
          clean       := seg;
          found_index := idx;
          EXIT;
        END IF;
      END LOOP;

      EXIT WHEN found_kind IS NOT NULL;
    END LOOP;
    EXIT WHEN found_kind IS NOT NULL;
  END LOOP;

  -- 4. Если не нашли — выходим
  IF found_kind IS NULL THEN
    RETURN;
  END IF;

  -- 5. Убираем все алиасы и имя типа из найденного сегмента
  FOR alias_item IN
    SELECT unnest(aliases)
    FROM public.lookup_types
    WHERE category = 'street_type'
      AND name = found_kind
  LOOP
    clean := regexp_replace(
      clean,
      format('(^|\s)%s\.?($|\s)', alias_item),
      ' ', 'g'
    );
  END LOOP;
  clean := regexp_replace(
    clean,
    format('(^|\s)%s($|\s)', found_kind),
    ' ', 'g'
  );
  clean := btrim(clean);

  -- 6. Определяем house_part (следующий за сегментом)
  IF found_index > 0 AND found_index < array_length(segments,1) THEN
    house_part := segments[found_index + 1];
  ELSE
    house_part := NULL;
  END IF;

  -- 7. Возврат результата
  street_type := found_kind;
  norm_name   := clean;
  RETURN NEXT;
END;
$_$;


--
-- Name: parse_address_nohouse(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.parse_address_nohouse(street_raw text) RETURNS TABLE(street_type text, norm_name text)
    LANGUAGE plpgsql
    AS $_$
DECLARE
  segments    TEXT[];
  seg         TEXT;
  clean       TEXT;
  words       TEXT[];
  w           TEXT;
  base_w      TEXT;
  tpl         RECORD;
  found_kind  TEXT;
  alias_item  TEXT;
BEGIN
  -- 1. Normalize
  clean := lower(street_raw);
  clean := regexp_replace(clean, '[\u0451]', 'е', 'gi');
  clean := regexp_replace(clean, '\.+', '.', 'g');
  clean := btrim(clean);

  -- 2. Split by commas
  segments := regexp_split_to_array(clean, '\s*,\s*');

  -- 3. Find the segment with a street_type
  FOREACH seg IN ARRAY segments LOOP
    EXIT WHEN seg IS NULL;
    words := coalesce(regexp_split_to_array(seg, '\s+'), ARRAY[]::text[]);
    FOREACH w IN ARRAY words LOOP
      base_w := regexp_replace(w, '\.$', '', 'g');

      FOR tpl IN
        SELECT l.name AS kind, a AS alias
        FROM public.lookup_types l
             CROSS JOIN unnest(l.aliases) AS a
        WHERE l.category = 'street_type'
      LOOP
        IF base_w ~* tpl.alias OR lower(tpl.kind) = base_w THEN
          found_kind := tpl.kind;
          clean      := seg;
          EXIT;
        END IF;
      END LOOP;

      EXIT WHEN found_kind IS NOT NULL;
    END LOOP;
    EXIT WHEN found_kind IS NOT NULL;
  END LOOP;

  -- 4. If nothing found, return empty
  IF found_kind IS NULL THEN
    RETURN;
  END IF;

  -- 5. Remove the type: loop through all aliases and the name itself
  FOR alias_item IN
    SELECT unnest(aliases)
    FROM public.lookup_types
    WHERE category = 'street_type'
      AND name = found_kind
  LOOP
    -- word‑boundary + optional dot
    clean := regexp_replace(
      clean,
      format('(^|\s)%s\.?(\s|$)', alias_item),
      ' ',
      'gi'
    );
  END LOOP;
  -- also remove literal name just in case
  clean := regexp_replace(
    clean,
    format('(^|\s)%s(\s|$)', found_kind),
    ' ',
    'gi'
  );

  clean := btrim(clean);

  -- 6. Return
  street_type := found_kind;
  norm_name   := clean;
  RETURN NEXT;
END;
$_$;


--
-- Name: parse_and_find_house(integer[], text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.parse_and_find_house(parentobjids integer[], hs_raw text) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
DECLARE
    -- raw house string, приводим к нижнему регистру и убираем пробелы по краям
    hs              TEXT := lower(trim(hs_raw));
    -- номер дома (или доп.номера) после очистки
    prefix          TEXT;
    rest            TEXT;
    result_id       INT;
    tmp             RECORD;
    -- Тип дома (housetype)
    ht_name         TEXT;
    ht_id           INT;
    -- Первый и второй уровни добавочного типа
    a1_id           INT; n1 TEXT;
    a2_id           INT; n2 TEXT;
BEGIN
    -- 0) Чистка от "мусора": берём только последний токен после пробела
    hs := regexp_replace(hs, '^.*\s+', '');

    -- 1) Определяем тип дома (housetype) по префиксу или дефолту 'д'
    SELECT name, id
      INTO ht_name, ht_id
      FROM lookup_types
     WHERE category = 'housetype' AND strpos(hs, name) = 1
     ORDER BY length(name) DESC
     LIMIT 1;

    IF FOUND THEN
        prefix := substr(hs, length(ht_name) + 1);
    ELSE
        SELECT id INTO ht_id
          FROM lookup_types
         WHERE category = 'housetype' AND name = 'д'
         LIMIT 1;
        ht_name := 'д';
        prefix  := regexp_replace(hs, E'^д\.?\s*', '');
    END IF;

    rest := prefix;

    -- 2) Извлекаем первый маркер addtype и номер после него
    SELECT name, id, strpos(rest, name) AS pos
      INTO tmp
      FROM lookup_types
     WHERE category = 'addtype' AND strpos(rest, name) > 0
     ORDER BY pos
     LIMIT 1;

    IF FOUND THEN
        prefix := substr(rest, 1, tmp.pos - 1);
        rest   := substr(rest, tmp.pos);
        a1_id  := tmp.id;
        n1     := substring(rest from '^' || tmp.name || '([0-9]+)');
        rest   := substr(rest, length(tmp.name || coalesce(n1,'')) + 1);
    ELSE
        prefix := rest;
        rest   := '';
    END IF;

    -- 3) Извлекаем второй addtype, если есть
    IF rest <> '' THEN
        SELECT name, id
          INTO tmp.name, tmp.id
          FROM lookup_types
         WHERE category = 'addtype' AND strpos(rest, name) = 1
         ORDER BY length(name) DESC
         LIMIT 1;

        IF FOUND THEN
            a2_id := tmp.id;
            n2    := substring(rest from '^' || tmp.name || '([0-9]+)');
        END IF;
    END IF;

    -- 4) Основной поиск по точному совпадению
    SELECT id
      INTO result_id
      FROM public.fias_houses
     WHERE parentobjid = ANY(parentobjids)
       AND upper(housenum) = upper(prefix)
       AND (addtype1 = a1_id OR (a1_id IS NULL AND addtype1 IS NULL))
       AND coalesce(addnum1::text, '') = coalesce(n1, '')
       AND (addtype2 = a2_id OR (a2_id IS NULL AND addtype2 IS NULL))
       AND coalesce(addnum2::text, '') = coalesce(n2, '')
     LIMIT 1;

    -- 5) Фолбэки: по номеру, затем удаляем буквы
    IF result_id IS NULL THEN
        SELECT id INTO result_id
          FROM public.fias_houses
         WHERE parentobjid = ANY(parentobjids)
           AND upper(housenum) = upper(prefix)
         LIMIT 1;
    END IF;

    IF result_id IS NULL THEN
        prefix := regexp_replace(prefix, '([0-9/]+)[а-яa-z]+$', '\1');
        SELECT id INTO result_id
          FROM public.fias_houses
--
CREATE PROCEDURE public.process_all_ads()
    LANGUAGE plpgsql
    AS $_$
DECLARE
  now_ts TIMESTAMP := now();
BEGIN
  -- 1. Подготовка временных таблиц
  TRUNCATE TABLE tmp_flats_history;
  CREATE TEMP TABLE tmp_debug (
    ad_id    INTEGER,
    debug    JSONB,
    success  BOOLEAN
  ) ON COMMIT DROP;

  -- 2. Подготовка обогащённых данных без фильтрации по batch_size
  CREATE TEMP TABLE tmp_enriched ON COMMIT DROP AS
  SELECT 
    a.id            AS ad_id,
    a.*,  
    pa.norm_name    AS street,
    pa.street_type,
	RIGHT(pa.house_part, 20) AS house,
    ht.id           AS house_type_id,
    ot.id           AS object_type_id,
    d.id            AS ao_id,
    COALESCE(
      regexp_replace(
        split_part(
          replace(replace(a.params->>'Название ЖК','ё','е'),'Ё','Е'),
          ',',1
        ),
        '\s*\(.*\)',''
      ), ''
    ) AS jk_name
  FROM ads a
  LEFT JOIN LATERAL public.parse_address(a.address) pa ON TRUE
  LEFT JOIN lookup_types ht
    ON ht.category = 'house_type'
   AND lower(ht.name) = lower(a.params->>'Тип дома')
  LEFT JOIN lookup_types ot
    ON ot.category = 'object_type'
   AND lower(ot.name) = lower(a.params->>'Вид объекта')
  LEFT JOIN districts d
    ON lower(d.admin_okrug) = lower(a.district_only)
  WHERE a.processed IS FALSE;

  -- 3. Вставка в tmp_flats_history с дедупликацией и отбором по house_id и проверкой диапазонов
  INSERT INTO tmp_flats_history (
    ad_id, house_id, floor, rooms,
    street, street_type, house,
    town, total_floors, area, living_area, kitchen_area,
    house_type_id, ao_id, built, metro_id, km_do_metro,
    source_id, object_type_id, nedvigimost_type_id,
    url, person_type_id, price,
    time_source_created, time_source_updated,
    avitoid, is_actual, description
  )
  SELECT DISTINCT ON (e.avitoid, e.source_id)
    e.ad_id,
    COALESCE(
      get_house_id_by_jk(e.jk_name, e.address),
      gha.result_id
    ) AS house_id,

    -- floor как раньше, NULL если невалидный
    CASE 
      WHEN e.params->>'Этаж' ~ '^[0-9]+$'
       AND (e.params->>'Этаж')::int BETWEEN -32768 AND 32767
      THEN (e.params->>'Этаж')::smallint
      ELSE 0
    END AS floor,

    -- rooms: число комнат или 0
    CASE 
      WHEN e.params->>'Количество комнат' ILIKE '%студ%' THEN 0
      WHEN e.params->>'Количество комнат' ~ '^[0-9]+$'
       AND (e.params->>'Количество комнат')::int BETWEEN -32768 AND 32767
      THEN (e.params->>'Количество комнат')::smallint
      ELSE 0
    END AS rooms,

    e.street, e.street_type, e.house,
    1 AS town,

    -- total_floors как раньше, NULL если невалидный
    CASE 
      WHEN e.params->>'Этажей в доме' ~ '^[0-9]+$'
       AND (e.params->>'Этажей в доме')::int BETWEEN -32768 AND 32767
      THEN (e.params->>'Этажей в доме')::smallint
      ELSE NULL
    END AS total_floors,

    (e.params->>'Площадь')::numeric,
    (e.params->>'Жилая площадь')::numeric,
    (e.params->>'Площадь кухни')::numeric,

    e.house_type_id,
    e.ao_id,

    -- built как раньше, NULL если невалидный
    CASE 
--
CREATE PROCEDURE public.process_all_w7_ads(IN p_batch_size integer DEFAULT 5000)
    LANGUAGE plpgsql
    AS $$

DECLARE
    v_total_count INTEGER;
    v_remaining_count INTEGER;
    v_batch_number INTEGER := 1;
    v_processed_this_batch INTEGER;
    v_total_processed INTEGER := 0;
    v_start_time TIMESTAMP := now();
    v_batch_start_time TIMESTAMP;
    v_batch_duration INTERVAL;
    v_estimated_remaining INTERVAL;
BEGIN
    -- Получаем начальное количество записей
    SELECT COUNT(*) INTO v_total_count FROM ads_w7;
    
    RAISE NOTICE '';
    RAISE NOTICE '🚀 НАЧАЛО ПОЛНОЙ ОБРАБОТКИ ads_w7';
    RAISE NOTICE '📊 Всего записей к обработке: %', v_total_count;
    RAISE NOTICE '📦 Размер батча: %', p_batch_size;
    RAISE NOTICE '⏰ Начало: %', v_start_time;
    RAISE NOTICE '==========================================';

    -- Основной цикл обработки
    LOOP
        -- Проверяем сколько записей осталось
        SELECT COUNT(*) INTO v_remaining_count FROM ads_w7;
        
        -- Если записей нет, выходим
        IF v_remaining_count = 0 THEN
            RAISE NOTICE '';
            RAISE NOTICE '✅ ВСЕ ЗАПИСИ ОБРАБОТАНЫ\!';
            EXIT;
        END IF;

        -- Засекаем время начала батча
        v_batch_start_time := now();
        
        RAISE NOTICE '';
        RAISE NOTICE '📦 БАТЧ #% | Осталось записей: % | Время: %', 
            v_batch_number, v_remaining_count, v_batch_start_time::time;
        RAISE NOTICE '------------------------------------------';

        -- Вызываем обработку одного батча
        CALL public.process_w7_ads(p_batch_size);
        
        -- Считаем сколько записей стало после батча
        SELECT v_remaining_count - COUNT(*) INTO v_processed_this_batch FROM ads_w7;
        v_total_processed := v_total_processed + v_processed_this_batch;
        
        -- Считаем время батча
        v_batch_duration := now() - v_batch_start_time;
        
        -- Оценка оставшегося времени (с защитой от деления на ноль)
        IF v_total_processed > 0 AND v_processed_this_batch > 0 THEN
            v_estimated_remaining := v_batch_duration * 
                ((SELECT COUNT(*) FROM ads_w7)::numeric / v_processed_this_batch::numeric);
        ELSE
            v_estimated_remaining := NULL;
        END IF;
        
        RAISE NOTICE '✅ Батч #% завершен за %', v_batch_number, v_batch_duration;
        RAISE NOTICE '📈 Обработано в батче: % | Всего обработано: % из %', 
            v_processed_this_batch, v_total_processed, v_total_count;
        
        IF v_estimated_remaining IS NOT NULL THEN
            RAISE NOTICE '⏱️  Примерное время до завершения: %', v_estimated_remaining;
        END IF;
        
        -- Если в батче ничего не обработалось, выходим во избежание бесконечного цикла
        IF v_processed_this_batch = 0 THEN
            RAISE NOTICE '';
            RAISE NOTICE '⚠️  ВНИМАНИЕ: В батче #% ничего не обработалось. Остановка.', v_batch_number;
            RAISE NOTICE '🔍 Проверьте записи с ошибками валидации (processed IS NULL)';
            EXIT;
        END IF;
        
        v_batch_number := v_batch_number + 1;
        
        -- Защита от слишком долгого выполнения (максимум 1000 батчей)
        IF v_batch_number > 1000 THEN
            RAISE NOTICE '';
            RAISE NOTICE '⚠️  ПРЕВЫШЕН ЛИМИТ БАТЧЕЙ (1000). Остановка принудительная.';
            EXIT;
        END IF;
        
    END LOOP;

    -- Финальная статистика
    DECLARE
        v_total_duration INTERVAL := now() - v_start_time;
        v_final_count INTEGER;
        v_failed_count INTEGER;
    BEGIN
        SELECT COUNT(*) INTO v_final_count FROM ads_w7;
        SELECT COUNT(*) INTO v_failed_count FROM ads_w7 WHERE processed IS NULL;
        
        RAISE NOTICE '';
        RAISE NOTICE '🎉 ОБРАБОТКА ЗАВЕРШЕНА\!';
--
CREATE PROCEDURE public.process_avito_ads(IN p_batch_size integer DEFAULT 5000)
    LANGUAGE plpgsql
    AS $$

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
    
    CREATE TEMP TABLE tmp_avito_debug (
        ad_id    INTEGER,
        debug    JSONB,
        success  BOOLEAN
    ) ON COMMIT DROP;

    -- 2. Получаем общее количество необработанных записей
    SELECT COUNT(*) INTO v_total_count FROM ads_avito WHERE processed IS FALSE;

    -- Проверяем, есть ли необработанные записи
    IF v_total_count = 0 THEN
        RAISE NOTICE 'Нет необработанных записей AVITO для обработки';
        RETURN;
    END IF;
    
    -- 3. Обработка одного батча
    -- Подготовка обогащённых данных из ads_avito с LIMIT
    -- Обрабатываем только записи с processed = FALSE
    CREATE TEMP TABLE tmp_avito_enriched ON COMMIT DROP AS
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
        c.source_created,
        c.metro_id,
        c.updated_at,
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
        -- Определяем тип дома по complex (как в Cian)
        CASE 
            WHEN c.complex ILIKE '%кирпич%' THEN 1
            WHEN c.complex ILIKE '%панель%' THEN 2
            WHEN c.complex ILIKE '%монолит%' THEN 3
            WHEN c.complex ILIKE '%блок%' THEN 4
            ELSE NULL
        END AS house_type_id,
        -- Определяем тип объекта (как в Cian)
        CASE 
            WHEN c.object_type_id IS NOT NULL THEN c.object_type_id
            ELSE 1 -- По умолчанию квартира
        END AS object_type_id,
        NULL::smallint AS ao_id -- У Avito нет district_id
    FROM ads_avito c
    WHERE c.processed IS FALSE
    ORDER BY c.id
    LIMIT p_batch_size;

    -- Получаем количество записей в батче
    SELECT COUNT(*) INTO v_batch_count FROM tmp_avito_enriched;
    
    IF v_batch_count = 0 THEN
        RAISE NOTICE 'Нет записей в батче для обработки';
        RETURN;
    END IF;

    -- 4. Создаем временную таблицу для хранения результатов парсинга адресов
    CREATE TEMP TABLE tmp_address_parsing ON COMMIT DROP AS
    SELECT 
        e.ad_id,
        e.complex,
--
CREATE PROCEDURE public.process_cian_ads(IN p_batch_size integer DEFAULT 1000)
    LANGUAGE plpgsql
    AS $$
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
--
CREATE PROCEDURE public.process_w7_ads(IN p_batch_size integer DEFAULT 5000)
    LANGUAGE plpgsql
    AS $$

DECLARE
    now_ts TIMESTAMP := now();
    v_processed_count INTEGER := 0;
    v_total_count INTEGER;
    v_batch_count INTEGER;
BEGIN
    -- 1. Подготовка временных таблиц
    TRUNCATE TABLE tmp_flats_history;

    -- Удаляем временную таблицу если существует
    DROP TABLE IF EXISTS tmp_w7_debug;
    CREATE TEMP TABLE tmp_w7_debug (
        ad_id    INTEGER,
        debug    JSONB,
        success  BOOLEAN
    ) ON COMMIT DROP;

    -- 2. Получаем общее количество записей
    SELECT COUNT(*) INTO v_total_count FROM ads_w7;

    -- Проверяем, есть ли записи для обработки
    IF v_total_count = 0 THEN
        RAISE NOTICE 'Нет записей W7 для обработки';
        RETURN;
    END IF;

    -- 3. Создаем временную таблицу для обработанных данных
    DROP TABLE IF EXISTS tmp_enriched_batch;
    CREATE TEMP TABLE tmp_enriched_batch ON COMMIT DROP AS
    WITH batch_with_source AS (
        SELECT
            c.id,
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
            c.created_at,
            c.metro_id,
            c.source_updated,
            c.district_id,
            c.ceiling_height,
            c.balcony_type_id,
            c.kitchen_square,
            c.life_square,
            c.built_year,
            c.building_batch_name,
            c.walls_material_type_id,
            c.person,
            c.object_type_id,
            c.guid,  -- Добавлено поле GUID
            -- Вычисляем source_id один раз
            CASE
                WHEN c.url ILIKE '%avito.ru%' THEN 1
                WHEN c.url ILIKE '%yandex.ru%' THEN 3
                WHEN c.url ILIKE '%cian.ru%' THEN 4
                ELSE 2
            END AS source_id
        FROM ads_w7 c
        ORDER BY c.id
        LIMIT p_batch_size * 3
    ),
    -- Устраняем дубликаты один раз
    ranked_ads AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY avitoid, source_id
                ORDER BY source_updated DESC NULLS LAST, id DESC
            ) as rn
        FROM batch_with_source
    ),
    -- Парсим адреса один раз для всех записей (с проверкой на NULL)
    parsed_addresses AS (
        SELECT
            ra.id,
            ra.address,
            CASE
                WHEN ra.address IS NOT NULL AND trim(ra.address) != '' THEN
                    (SELECT norm_name FROM public.parse_address(ra.address) LIMIT 1)
                ELSE NULL
            END as street,
            CASE
                WHEN ra.address IS NOT NULL AND trim(ra.address) != '' THEN
                    (SELECT street_type FROM public.parse_address(ra.address) LIMIT 1)
                ELSE 'ул'::varchar(9)
            END as street_type,
            CASE
                WHEN ra.address IS NOT NULL AND trim(ra.address) != '' THEN
                    (SELECT house_part FROM public.parse_address(ra.address) LIMIT 1)
                ELSE NULL
--
CREATE PROCEDURE public.process_w7_ads_bak(IN p_batch_size integer DEFAULT 5000)
    LANGUAGE plpgsql
    AS $$

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

    CREATE TEMP TABLE tmp_w7_debug (
        ad_id    INTEGER,
        debug    JSONB,
        success  BOOLEAN
    ) ON COMMIT DROP;

    -- 2. Получаем общее количество необработанных записей
    SELECT COUNT(*) INTO v_total_count FROM ads_w7 WHERE processed IS FALSE;

    -- Проверяем, есть ли необработанные записи
    IF v_total_count = 0 THEN
        RAISE NOTICE 'Нет необработанных записей W7 для обработки';
        RETURN;
    END IF;

    -- 3. Сначала создаем таблицу со всеми записями в батче (включая дубликаты)
    CREATE TEMP TABLE tmp_w7_batch_all ON COMMIT DROP AS
    SELECT
        c.id,
        c.url,
        c.avitoid,
        CASE
            WHEN c.url ILIKE '%avito.ru%' THEN 1
            WHEN c.url ILIKE '%yandex.ru%' THEN 3
            WHEN c.url ILIKE '%cian.ru%' THEN 4
            ELSE 2
        END AS source_id
    FROM ads_w7 c
    WHERE c.processed IS FALSE
    ORDER BY c.id
    LIMIT p_batch_size * 3;

    -- 4. Обработка одного батча с устранением дубликатов
    -- Подготовка обогащённых данных из ads_w7 с LIMIT и DISTINCT
    CREATE TEMP TABLE tmp_w7_enriched ON COMMIT DROP AS
    WITH ranked_ads AS (
        SELECT
            c.id,
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
            c.created_at,
            c.metro_id,
            c.source_updated,
            c.district_id,
            c.ceiling_height,
            c.balcony_type_id,
            c.kitchen_square,
            c.life_square,
            c.built_year,
            c.building_batch_name,
            c.walls_material_type_id,
            c.person,
            c.object_type_id,
            -- Определяем source_id для группировки
            CASE
                WHEN c.url ILIKE '%avito.ru%' THEN 1
                WHEN c.url ILIKE '%yandex.ru%' THEN 3
                WHEN c.url ILIKE '%cian.ru%' THEN 4
                ELSE 2
            END AS source_id,
            -- Ранжируем записи с одинаковым avitoid+source_id по дате обновления
            ROW_NUMBER() OVER (
                PARTITION BY c.avitoid,
                    CASE
                        WHEN c.url ILIKE '%avito.ru%' THEN 1
                        WHEN c.url ILIKE '%yandex.ru%' THEN 3
                        WHEN c.url ILIKE '%cian.ru%' THEN 4
                        ELSE 2
                    END
                ORDER BY c.source_updated DESC NULLS LAST, c.id DESC
            ) as rn
        FROM ads_w7 c
        WHERE c.id IN (SELECT id FROM tmp_w7_batch_all)
    )
    SELECT
--
CREATE PROCEDURE public.transfer_user_ads_to_public(IN user_ad_ids integer[])
    LANGUAGE plpgsql
    AS $_$
DECLARE
    rec RECORD;
    house_id_var INTEGER;
    source_id_var SMALLINT;
    ad_type_var SMALLINT;
    object_type_var SMALLINT;
    person_type_var SMALLINT;
    town_id_var SMALLINT;
    house_type_id_var SMALLINT;
    need_house_id_update BOOLEAN;
    need_person_type_update BOOLEAN;
BEGIN
    -- Константы для маппинга
    town_id_var := 1; -- Москва
    ad_type_var := 1; -- Продажа (из nedvigimost_type_id)
    object_type_var := 2; -- Вторичка (из object_type)

    -- Обрабатываем каждое объявление
    FOR rec IN 
        SELECT 
            a.*,
            uf.address as flat_address
        FROM users.ads a
        JOIN users.user_flats uf ON a.flat_id = uf.id
        WHERE a.id = ANY(user_ad_ids)
    LOOP
        -- Определяем source_id на основе URL
        IF rec.url LIKE '%cian.ru%' THEN
            source_id_var := 4; -- Cian
        ELSIF rec.url LIKE '%avito.ru%' THEN
            source_id_var := 1; -- Avito
        ELSIF rec.url LIKE '%realty.yandex.ru%' OR rec.url LIKE '%realty.ya.ru%' THEN
            source_id_var := 3; -- Yandex
        ELSE
            source_id_var := 2; -- Other
        END IF;

        -- Получаем или используем существующий house_id
        need_house_id_update := FALSE;
        IF rec.house_id IS NOT NULL THEN
            house_id_var := rec.house_id;
        ELSE
            SELECT result_id INTO house_id_var 
            FROM public.get_house_id_by_address(rec.flat_address);
            
            -- Если адрес не найден, пропускаем это объявление
            IF house_id_var IS NULL THEN
                RAISE NOTICE 'Address not found for ad %: %', rec.id, rec.flat_address;
                CONTINUE;
            ELSE
                need_house_id_update := TRUE;
            END IF;
        END IF;
        
        -- Маппинг house_type через lookup_types
        house_type_id_var := public.get_lookup_id('house_type', rec.house_type);
        
        -- Получаем или определяем person_type_id
        need_person_type_update := FALSE;
        IF rec.person_type_id IS NOT NULL THEN
            person_type_var := rec.person_type_id;
        ELSE
            person_type_var := public.get_person_type_id(rec.person_type, rec.description, rec.tags);
            need_person_type_update := TRUE;
        END IF;

        -- 1. Upsert в таблицу public.flats
        INSERT INTO public.flats (
            house_id, 
            floor, 
            rooms,
            street,
            street_type,
            house,
            town, 
            total_floors,
            area,
            living_area, 
            kitchen_area,
            house_type_id,
            ao_id,
            built,
            metro_id,
            km_do_metro,
            min_metro
        )
        VALUES (
            house_id_var,
            COALESCE(rec.floor, 1),
            rec.rooms,
            -- Парсим адрес (упрощенная версия)
            split_part(rec.flat_address, ',', 1), -- street
            'ул.', -- street_type (статично)
            split_part(rec.flat_address, ',', 2), -- house
            town_id_var,
            rec.total_floors,
            rec.total_area,
            rec.living_area,
--
CREATE FUNCTION public.upsert_cian_ad(p_url text, p_avitoid numeric, p_price bigint, p_rooms smallint, p_area numeric, p_floor smallint, p_total_floors smallint, p_complex text, p_metro_id integer, p_min_metro smallint, p_address text, p_district_id integer, p_tags text, p_person_type smallint, p_person text, p_object_type_id smallint, p_source_created timestamp without time zone, p_should_mark_processed boolean) RETURNS TABLE(operation_type text, old_price bigint, new_price bigint, is_new_record boolean)
    LANGUAGE plpgsql
    AS $$
            DECLARE
                v_old_price BIGINT;
                v_exists BOOLEAN;
                v_price_changed BOOLEAN;
            BEGIN
                -- Проверяем существование записи и получаем старую цену
                SELECT price INTO v_old_price
                FROM ads_cian
                WHERE avitoid = p_avitoid;

                v_exists := FOUND;
                v_price_changed := (v_old_price IS DISTINCT FROM p_price);

                -- Выполняем INSERT ... ON CONFLICT
                INSERT INTO ads_cian (
                    url, avitoid, price, rooms, area, floor, total_floors,
                    complex, metro_id, min_metro, address, district_id, tags,
                    person_type, person, object_type_id, source_created, processed
                ) VALUES (
                    p_url, p_avitoid, p_price, p_rooms, p_area, p_floor, p_total_floors,
                    p_complex, p_metro_id, p_min_metro, p_address, p_district_id, p_tags,
                    p_person_type, p_person, p_object_type_id, p_source_created, p_should_mark_processed
                )
                ON CONFLICT (avitoid) DO UPDATE SET
                    url = EXCLUDED.url,
                    price = EXCLUDED.price,
                    rooms = EXCLUDED.rooms,
                    area = EXCLUDED.area,
                    floor = EXCLUDED.floor,
                    total_floors = EXCLUDED.total_floors,
                    complex = EXCLUDED.complex,
                    metro_id = EXCLUDED.metro_id,
                    min_metro = EXCLUDED.min_metro,
                    address = EXCLUDED.address,
                    district_id = EXCLUDED.district_id,
                    tags = EXCLUDED.tags,
                    person_type = EXCLUDED.person_type,
                    person = EXCLUDED.person,
                    object_type_id = EXCLUDED.object_type_id,
                    source_created = EXCLUDED.source_created,
                    processed = CASE
                        WHEN ads_cian.price IS DISTINCT FROM EXCLUDED.price THEN FALSE
                        ELSE EXCLUDED.processed
                    END;

                -- Возвращаем результат
                IF NOT v_exists THEN
                    -- Новая запись
                    RETURN QUERY SELECT 'inserted'::TEXT, NULL::BIGINT, p_price, TRUE;
                ELSIF v_price_changed THEN
                    -- Обновление с изменением цены
                    RETURN QUERY SELECT 'updated'::TEXT, v_old_price, p_price, TRUE;
                ELSE
                    -- Дубликат без изменения цены
                    RETURN QUERY SELECT 'duplicate'::TEXT, v_old_price, p_price, FALSE;
                END IF;
            END;
            $$;


SET default_table_access_method = heap;

--
-- Name: addrobj; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.addrobj (
    guid uuid NOT NULL,
    aoid text NOT NULL,
    name text,
    typename text,
    path text,
    isactual boolean
);


--
-- Name: ads; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ads (
    id bigint NOT NULL,
    url text,
    price numeric,
    "time" timestamp without time zone,
    time_source_created date,
    time_source_updated timestamp without time zone,
    person text,
    person_type_id integer,
    city text,
    metro_only text,
    district_only text,
    address text,
    description text,
    nedvigimost_type_id integer,
    avitoid numeric,
    cat1_id integer,
    cat2_id integer,
