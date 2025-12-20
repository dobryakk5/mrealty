CREATE FUNCTION system.clean_tag(tag_text text) RETURNS text
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF tag_text IS NULL THEN
        RETURN NULL;
    END IF;
    
    -- Убираем лишние пробелы в начале и конце
    RETURN trim(tag_text);
END;
$$;


--
-- Name: cleanup_unused_tags(integer); Type: FUNCTION; Schema: system; Owner: -
--

CREATE FUNCTION system.cleanup_unused_tags(days_threshold integer DEFAULT 30) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    deleted_count INTEGER := 0;
BEGIN
    -- Деактивируем теги, которые не использовались более указанного количества дней
    UPDATE system.tags 
    SET is_active = false
    WHERE updated_at < CURRENT_TIMESTAMP - INTERVAL '1 day' * days_threshold
      AND usage_count = 0
      AND tag_category = 'автоматически_добавлен';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$;


--
-- Name: export_tags_as_json(); Type: FUNCTION; Schema: system; Owner: -
--

CREATE FUNCTION system.export_tags_as_json() RETURNS json
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN (
        SELECT json_agg(
            json_build_object(
                'tag_name', tag_name,
                'tag_category', tag_category,
                'tag_description', tag_description,
                'usage_count', usage_count,
                'is_active', is_active
            )
        )
        FROM system.tags
        WHERE is_active = true
        ORDER BY tag_category, tag_name
    );
END;
$$;


--
-- Name: extract_tags_from_string(text); Type: FUNCTION; Schema: system; Owner: -
--

CREATE FUNCTION system.extract_tags_from_string(tags_text text) RETURNS text[]
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF tags_text IS NULL OR tags_text = '' THEN
        RETURN ARRAY[]::TEXT[];
    END IF;
    
    -- Разбиваем строку по запятым, убираем пробелы и пустые элементы
    RETURN string_to_array(trim(tags_text), ',')::TEXT[];
END;
$$;


--
-- Name: get_all_active_tags(); Type: FUNCTION; Schema: system; Owner: -
--

CREATE FUNCTION system.get_all_active_tags() RETURNS TABLE(tag_name text, tag_category character varying, tag_description text)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT t.tag_name, t.tag_category, t.tag_description
    FROM system.tags t
    WHERE t.is_active = true
    ORDER BY t.tag_category, t.tag_name;
END;
$$;


--
-- Name: get_tags_by_category(character varying); Type: FUNCTION; Schema: system; Owner: -
--

CREATE FUNCTION system.get_tags_by_category(category_name character varying) RETURNS TABLE(tag_name text, tag_description text, usage_count integer)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT t.tag_name, t.tag_description, t.usage_count
    FROM system.tags t
    WHERE t.tag_category = category_name AND t.is_active = true
    ORDER BY t.usage_count DESC, t.tag_name;
END;
$$;


--
-- Name: get_tags_statistics(); Type: FUNCTION; Schema: system; Owner: -
--

CREATE FUNCTION system.get_tags_statistics() RETURNS TABLE(category character varying, total_tags integer, active_tags integer, total_usage bigint, last_updated timestamp without time zone)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COALESCE(t.tag_category, 'не_категоризирован') as category,
        COUNT(*) as total_tags,
        COUNT(*) FILTER (WHERE t.is_active = true) as active_tags,
        SUM(t.usage_count) as total_usage,
        MAX(t.updated_at) as last_updated
    FROM system.tags t
    GROUP BY t.tag_category
    ORDER BY total_usage DESC, total_tags DESC;
END;
$$;


--
-- Name: search_tags(text); Type: FUNCTION; Schema: system; Owner: -
--

CREATE FUNCTION system.search_tags(search_pattern text) RETURNS TABLE(tag_name text, tag_category character varying, tag_description text, usage_count integer)
    LANGUAGE plpgsql
    AS $$
BEGIN
    RETURN QUERY
    SELECT t.tag_name, t.tag_category, t.tag_description, t.usage_count
    FROM system.tags t
    WHERE t.is_active = true AND t.tag_name ILIKE '%' || search_pattern || '%'
    ORDER BY t.usage_count DESC, t.tag_name;
END;
$$;


--
-- Name: sync_tags_from_ads_avito(); Type: FUNCTION; Schema: system; Owner: -
--

CREATE FUNCTION system.sync_tags_from_ads_avito() RETURNS TABLE(new_tags_count integer, updated_tags_count integer, total_tags_count integer)
    LANGUAGE plpgsql
    AS $$
DECLARE
    new_tags INTEGER := 0;
    updated_tags INTEGER := 0;
    total_tags INTEGER := 0;
BEGIN
    -- Создаем временную таблицу с текущими тегами
    CREATE TEMP TABLE temp_current_tags AS
    SELECT DISTINCT 
        system.clean_tag(tag_item) as tag_name,
        COUNT(*) as usage_count
    FROM (
        SELECT unnest(system.extract_tags_from_string(tags)) as tag_item
        FROM ads_avito 
        WHERE tags IS NOT NULL AND tags != ''
    ) t
    WHERE system.clean_tag(tag_item) IS NOT NULL
    GROUP BY system.clean_tag(tag_item);

    -- Добавляем новые теги
    INSERT INTO system.tags (tag_name, tag_category, tag_description, usage_count)
    SELECT 
        t.tag_name,
        'автоматически_добавлен' as tag_category,
        'Тег автоматически добавлен при синхронизации' as tag_description,
        t.usage_count
    FROM temp_current_tags t
    WHERE NOT EXISTS (
        SELECT 1 FROM system.tags st WHERE st.tag_name = t.tag_name
    )
    ON CONFLICT (tag_name) DO NOTHING;
    
    GET DIAGNOSTICS new_tags = ROW_COUNT;

    -- Обновляем статистику использования существующих тегов
    UPDATE system.tags 
    SET 
        usage_count = t.usage_count,
        updated_at = CURRENT_TIMESTAMP
    FROM temp_current_tags t
    WHERE system.tags.tag_name = t.tag_name;
    
    GET DIAGNOSTICS updated_tags = ROW_COUNT;

    -- Получаем общее количество тегов
    SELECT COUNT(*) INTO total_tags FROM system.tags;

    -- Удаляем временную таблицу
    DROP TABLE temp_current_tags;

    -- Возвращаем статистику
    RETURN QUERY SELECT new_tags, updated_tags, total_tags;
END;
$$;


--
-- Name: transliterate_to_ascii(text); Type: FUNCTION; Schema: system; Owner: -
--

CREATE FUNCTION system.transliterate_to_ascii(in_text text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
    AS $$
DECLARE
  r system.transliterate_to_ascii_rules;
BEGIN
  FOR r IN SELECT chr, trans FROM system.transliterate_to_ascii_rules WHERE chr IN (
    SELECT source_chr
      FROM (
        SELECT unnest(regexp_split_to_array(in_text, '')) AS source_chr
      ) x
     WHERE ascii(x.source_chr) > 127
  )
  LOOP
    in_text = replace(in_text, r.chr, r.trans);
  END LOOP;

  RETURN trim(in_text);
END;
$$;


--
-- Name: update_tags_updated_at(); Type: FUNCTION; Schema: system; Owner: -
--

CREATE FUNCTION system.update_tags_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;


--
-- Name: webalize(text); Type: FUNCTION; Schema: system; Owner: -
--

CREATE FUNCTION system.webalize(in_string text) RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $$
  SELECT trim(BOTH '-' FROM regexp_replace(lower(system.transliterate_to_ascii(translate(in_string, '@°', 'a '))), '[^a-z0-9]+', '-', 'g'));
$$;


SET default_table_access_method = heap;

--
-- Name: tags; Type: TABLE; Schema: system; Owner: -
--

CREATE TABLE system.tags (
    id integer NOT NULL,
    tag_name text NOT NULL,
    tag_category character varying(50),
    tag_description text,
    usage_count integer DEFAULT 0,
    is_active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: active_tags; Type: VIEW; Schema: system; Owner: -
--

CREATE VIEW system.active_tags AS
 SELECT tag_name,
    tag_category,
    tag_description,
    usage_count
   FROM system.tags
  WHERE (is_active = true)
  ORDER BY tag_category, tag_name;


--
-- Name: avito_pagination_tracking; Type: TABLE; Schema: system; Owner: -
--

CREATE TABLE system.avito_pagination_tracking (
    id integer NOT NULL,
    metro_id integer NOT NULL,
    last_processed_page integer DEFAULT 0,
    total_pages_processed integer DEFAULT 0,
    last_updated timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: avito_pagination_tracking_id_seq; Type: SEQUENCE; Schema: system; Owner: -
--

CREATE SEQUENCE system.avito_pagination_tracking_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: avito_pagination_tracking_id_seq; Type: SEQUENCE OWNED BY; Schema: system; Owner: -
--

ALTER SEQUENCE system.avito_pagination_tracking_id_seq OWNED BY system.avito_pagination_tracking.id;


--
-- Name: parsing_progress; Type: TABLE; Schema: system; Owner: -
--

CREATE TABLE system.parsing_progress (
    id integer NOT NULL,
    property_type integer NOT NULL,
    time_period integer,
    current_metro_id integer NOT NULL,
    time_upd timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    status text DEFAULT 'active'::text,
    total_metros integer,
    processed_metros integer DEFAULT 0,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    source smallint
);


--
-- Name: parsing_progress_id_seq; Type: SEQUENCE; Schema: system; Owner: -
--

CREATE SEQUENCE system.parsing_progress_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
