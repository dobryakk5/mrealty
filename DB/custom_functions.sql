-- ============================================
-- build_flat_report / build_flat_report_analogs
-- ============================================

CREATE OR REPLACE FUNCTION users.build_flat_report(
    p_tg_user_id BIGINT,
    p_house_id   INTEGER,
    p_floor      INTEGER,
    p_rooms      INTEGER,
    p_radius_m   INTEGER DEFAULT 1500
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_report jsonb;

  v_area numeric;
  v_subject_price bigint;
  v_subject_ppm numeric;
  v_sub_kitchen numeric;
  v_sub_living numeric;
  v_sub_total_floors int;
  v_sub_floor int;
  v_sub_rooms int;
  v_sub_bathroom text;
  v_sub_balcony text;
  v_sub_renovation text;
  v_sub_year int;
  v_sub_house_type text;
  v_sub_ceiling numeric;
  v_sub_furniture text;
  v_sub_metro_station text;
  v_sub_metro_time smallint;
  v_sub_url text;

  v_p25_ppm numeric;
  v_p50_ppm numeric;
  v_p75_ppm numeric;

  v_cnt int;
  v_pos numeric;

  v_top_items jsonb;

  v_report_type text := 'near_rooms';
  v_params jsonb := '{}'::jsonb;
BEGIN
  -- субъект: берём из flats, при отсутствии — из users.ads, затем из flats_history
  WITH subject AS (
    SELECT *
    FROM (
      SELECT
        f.house_id, f.floor, f.rooms,
        f.floor AS subj_floor,
        f.rooms AS subj_rooms,
        COALESCE(f.area, ua.total_area, fh.area_m2) AS area_m2,
        COALESCE(ua.price, fh.price) AS subject_price,
        COALESCE(ua.updated_at, fh.time_source_updated) AS updated_at,
        ua.kitchen_area,
        ua.living_area,
        ua.total_floors,
        ua.bathroom,
        ua.balcony,
        ua.renovation,
        ua.construction_year,
        ua.house_type,
        ua.ceiling_height,
        ua.furniture,
        ua.metro_station,
        ua.metro_time::smallint,
        ua.url
      FROM public.flats f
      LEFT JOIN LATERAL (
        SELECT a.*
        FROM users.ads a
        WHERE a.house_id = f.house_id
          AND a.floor = f.floor
          AND a.rooms = f.rooms
        ORDER BY a.updated_at DESC NULLS LAST, a.created_at DESC NULLS LAST
        LIMIT 1
      ) ua ON TRUE
      LEFT JOIN LATERAL (
        SELECT price, NULL::numeric AS area_m2, time_source_updated
        FROM public.flats_history
        WHERE house_id = f.house_id
          AND floor = f.floor
          AND rooms = f.rooms
          AND is_actual <> 0
        ORDER BY time_source_updated DESC NULLS LAST
        LIMIT 1
      ) fh ON TRUE
      WHERE f.house_id = p_house_id
        AND f.floor = p_floor
        AND f.rooms = p_rooms
      UNION ALL
      SELECT
        a.house_id, a.floor, a.rooms,
        a.floor AS subj_floor,
        a.rooms AS subj_rooms,
        COALESCE(a.total_area, a.living_area) AS area_m2,
        a.price AS subject_price,
        a.updated_at,
        a.kitchen_area,
        a.living_area,
        a.total_floors,
        a.bathroom,
        a.balcony,
        a.renovation,
        a.construction_year,
        a.house_type,
        a.ceiling_height,
        a.furniture,
        a.metro_station,
        a.metro_time::smallint,
        a.url
      FROM users.ads a
      WHERE a.house_id = p_house_id
        AND a.floor = p_floor
        AND a.rooms = p_rooms
        AND a.price IS NOT NULL
      UNION ALL
      SELECT
        fh.house_id, fh.floor, fh.rooms,
        fh.floor AS subj_floor,
        fh.rooms AS subj_rooms,
        NULL::numeric AS area_m2,
        fh.price AS subject_price,
        fh.time_source_updated AS updated_at,
        NULL::numeric AS kitchen_area,
        NULL::numeric AS living_area,
        NULL::int AS total_floors,
        NULL::text AS bathroom,
        NULL::text AS balcony,
        NULL::text AS renovation,
        NULL::int AS construction_year,
        NULL::text AS house_type,
        NULL::numeric AS ceiling_height,
        NULL::text AS furniture,
        NULL::text AS metro_station,
        NULL::smallint AS metro_time,
        NULL::text AS url
      FROM public.flats_history fh
      WHERE fh.house_id = p_house_id
        AND fh.floor = p_floor
        AND fh.rooms = p_rooms
        AND fh.price IS NOT NULL
        AND fh.is_actual <> 0
    ) s
    ORDER BY updated_at DESC NULLS LAST
    LIMIT 1
  )
  SELECT
    area_m2, subject_price,
    subj_floor, subj_rooms,
    kitchen_area, living_area, total_floors,
    bathroom, balcony, renovation, construction_year,
    house_type, ceiling_height, furniture,
    metro_station, metro_time, url
  INTO
    v_area, v_subject_price,
    v_sub_floor, v_sub_rooms,
    v_sub_kitchen, v_sub_living, v_sub_total_floors,
    v_sub_bathroom, v_sub_balcony, v_sub_renovation, v_sub_year,
    v_sub_house_type, v_sub_ceiling, v_sub_furniture,
    v_sub_metro_station, v_sub_metro_time, v_sub_url
  FROM subject;

  IF v_subject_price IS NOT NULL AND v_area IS NOT NULL AND v_area > 0 THEN
    v_subject_ppm := (v_subject_price::numeric / v_area);
  ELSE
    v_subject_ppm := NULL;
  END IF;

  -- конкуренты из users.ads (по радиусу) + топ
  WITH near_houses AS (
    SELECT p_house_id AS house_id, 0::int AS dist_m
    UNION ALL
    SELECT nh.house_id, nh.dist_m
    FROM public.get_house_near_house(p_house_id, p_radius_m) nh
  ),
  ads_raw AS (
    SELECT
      a.*,
      nh.dist_m AS near_dist_m,
      lower(split_part(coalesce(a.url, ''), '?', 1)) AS norm_url
    FROM near_houses nh
    JOIN users.ads a ON a.house_id = nh.house_id
    WHERE a.price IS NOT NULL
      AND a.rooms = p_rooms
      AND (a.status IS NULL OR a.status = TRUE)
  ),
  ads_dedup AS (
    SELECT DISTINCT ON (COALESCE(norm_url, a.id::text))
      a.id AS ad_id,
      a.house_id,
      a.floor,
      a.rooms,
      COALESCE(a.near_dist_m, 0) AS dist_m,
      a.price,
      COALESCE(a.total_area, a.living_area) AS area_m2,
      a.kitchen_area,
      a.living_area,
      a.total_floors,
      a.bathroom,
      a.balcony,
      a.renovation,
      a.construction_year,
      a.house_type,
      a.ceiling_height,
      a.furniture,
      a.metro_station,
      a.metro_time::smallint AS metro_time,
      NULL::text AS metro_way,
      a.url,
      a.updated_at
    FROM ads_raw a
    ORDER BY COALESCE(norm_url, a.id::text), a.updated_at DESC NULLS LAST, a.id DESC
  ),
  comps AS (
    SELECT
      *,
      CASE
        WHEN area_m2 IS NOT NULL AND area_m2 > 0 THEN price::numeric / area_m2
        ELSE NULL
      END AS ppm
    FROM ads_dedup
  ),
  stats AS (
    SELECT
      COUNT(*)::int AS cnt,
      percentile_cont(0.25) WITHIN GROUP (ORDER BY ppm) AS p25_ppm,
      percentile_cont(0.50) WITHIN GROUP (ORDER BY ppm) AS p50_ppm,
      percentile_cont(0.75) WITHIN GROUP (ORDER BY ppm) AS p75_ppm
    FROM comps
    WHERE ppm IS NOT NULL
  ),
  pos AS (
    SELECT
      CASE
        WHEN v_subject_ppm IS NULL THEN NULL
        WHEN (SELECT cnt FROM stats) = 0 THEN NULL
        ELSE (
          SELECT (COUNT(*)::numeric / NULLIF((SELECT cnt FROM stats)::numeric,0))
          FROM comps
          WHERE ppm IS NOT NULL AND ppm <= v_subject_ppm
        )
      END AS position01
  ),
  top_items AS (
    SELECT
      COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'ad_id', ad_id,
            'house_id', house_id,
            'floor', floor,
            'rooms', rooms,
            'dist_m', dist_m,
            'price', price,
            'area_m2', area_m2,
            'kitchen_area', kitchen_area,
            'living_area', living_area,
            'total_floors', total_floors,
            'bathroom', bathroom,
            'balcony', balcony,
            'renovation', renovation,
            'construction_year', construction_year,
            'house_type', house_type,
            'ceiling_height', ceiling_height,
            'furniture', furniture,
            'metro_station', metro_station,
            'metro_time', metro_time,
            'metro_way', metro_way,
            'ppm', ppm,
            'url', url,
            'updated', updated_at
          )
          ORDER BY ppm ASC NULLS LAST, price ASC
        ),
        '[]'::jsonb
      ) AS items
    FROM (
      SELECT * FROM comps
      ORDER BY ppm ASC NULLS LAST, price ASC
      LIMIT 20
    ) t
  )
  SELECT
    s.cnt, s.p25_ppm, s.p50_ppm, s.p75_ppm,
    p.position01,
    ti.items
  INTO
    v_cnt, v_p25_ppm, v_p50_ppm, v_p75_ppm,
    v_pos,
    v_top_items
  FROM stats s
  CROSS JOIN pos p
  CROSS JOIN top_items ti;

  v_report :=
    jsonb_build_object(
      'meta', jsonb_build_object(
        'tg_user_id', p_tg_user_id,
        'key', jsonb_build_object('house_id', p_house_id, 'floor', p_floor, 'rooms', p_rooms),
        'radius_m', p_radius_m,
        'report_type', v_report_type,
        'params', v_params,
        'generated_at', now()
      ),
      'subject', jsonb_build_object(
        'area_m2', v_area,
        'price', v_subject_price,
        'floor', COALESCE(v_sub_floor, p_floor),
        'rooms', COALESCE(v_sub_rooms, p_rooms),
        'ppm', v_subject_ppm,
        'kitchen_area', v_sub_kitchen,
        'living_area', v_sub_living,
        'total_floors', v_sub_total_floors,
        'bathroom', v_sub_bathroom,
        'balcony', v_sub_balcony,
        'renovation', v_sub_renovation,
        'construction_year', v_sub_year,
        'house_type', v_sub_house_type,
        'ceiling_height', v_sub_ceiling,
        'furniture', v_sub_furniture,
        'metro_station', v_sub_metro_station,
        'metro_time', v_sub_metro_time,
        'url', v_sub_url,
        'dist_m', 0
      ),
      'market', jsonb_build_object(
        'competitors_count', v_cnt,
        'ppm_p25', v_p25_ppm,
        'ppm_p50', v_p50_ppm,
        'ppm_p75', v_p75_ppm,
        'position01', v_pos
      ),
      'top_competitors', v_top_items
    );

  INSERT INTO users.flat_reports (
    tg_user_id, house_id, floor, rooms, radius_m, report_date,
    report_type, params,
    report_json, created_at, updated_at
  )
  VALUES (
    p_tg_user_id, p_house_id, p_floor, p_rooms, p_radius_m, CURRENT_DATE,
    v_report_type, v_params,
    v_report, now(), now()
  )
  ON CONFLICT (tg_user_id, house_id, floor, rooms, radius_m, report_date, report_type, params)
  DO UPDATE SET
    report_json = EXCLUDED.report_json,
    updated_at = now();

  RETURN v_report;
END;
$$;


CREATE OR REPLACE FUNCTION users.build_flat_report_analogs(
    p_tg_user_id BIGINT,
    p_house_id   INTEGER,
    p_floor      INTEGER,
    p_rooms      INTEGER,
    p_radius_m   INTEGER DEFAULT 1500,
    p_area_pct   NUMERIC DEFAULT 0.15,
    p_floor_delta INTEGER DEFAULT 2,
    p_limit_top   INTEGER DEFAULT 30
) RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
  v_report jsonb;

  v_area numeric;
  v_subject_price bigint;
  v_subject_ppm numeric;
  v_sub_kitchen numeric;
  v_sub_living numeric;
  v_sub_total_floors int;
  v_sub_floor int;
  v_sub_rooms int;
  v_sub_bathroom text;
  v_sub_balcony text;
  v_sub_renovation text;
  v_sub_year int;
  v_sub_house_type text;
  v_sub_ceiling numeric;
  v_sub_furniture text;
  v_sub_metro_station text;
  v_sub_metro_time smallint;
  v_sub_url text;

  v_cnt int;
  v_p25_ppm numeric;
  v_p50_ppm numeric;
  v_p75_ppm numeric;
  v_pos numeric;

  v_params jsonb;
  v_top_items jsonb;
BEGIN
  v_params := jsonb_build_object(
    'area_pct', p_area_pct,
    'floor_delta', p_floor_delta,
    'limit_top', p_limit_top
  );

  WITH subject AS (
    SELECT *
    FROM (
      SELECT
        f.house_id, f.floor, f.rooms,
        f.floor AS subj_floor,
        f.rooms AS subj_rooms,
        COALESCE(f.area, ua.total_area, fh.area_m2) AS area_m2,
        COALESCE(ua.price, fh.price) AS subject_price,
        COALESCE(ua.updated_at, fh.time_source_updated) AS updated_at,
        ua.kitchen_area,
        ua.living_area,
        ua.total_floors,
        ua.bathroom,
        ua.balcony,
        ua.renovation,
        ua.construction_year,
        ua.house_type,
        ua.ceiling_height,
        ua.furniture,
        ua.metro_station,
        ua.metro_time::smallint,
        ua.url
      FROM public.flats f
      LEFT JOIN LATERAL (
        SELECT a.*
        FROM users.ads a
        WHERE a.house_id = f.house_id
          AND a.floor = f.floor
          AND a.rooms = f.rooms
        ORDER BY a.updated_at DESC NULLS LAST, a.created_at DESC NULLS LAST
        LIMIT 1
      ) ua ON TRUE
      LEFT JOIN LATERAL (
        SELECT price, NULL::numeric AS area_m2, time_source_updated
        FROM public.flats_history
        WHERE house_id = f.house_id
          AND floor = f.floor
          AND rooms = f.rooms
          AND is_actual <> 0
        ORDER BY time_source_updated DESC NULLS LAST
        LIMIT 1
      ) fh ON TRUE
      WHERE f.house_id = p_house_id
        AND f.floor = p_floor
        AND f.rooms = p_rooms
      UNION ALL
      SELECT
        a.house_id, a.floor, a.rooms,
        a.floor AS subj_floor,
        a.rooms AS subj_rooms,
        COALESCE(a.total_area, a.living_area) AS area_m2,
        a.price AS subject_price,
        a.updated_at,
        a.kitchen_area,
        a.living_area,
        a.total_floors,
        a.bathroom,
        a.balcony,
        a.renovation,
        a.construction_year,
        a.house_type,
        a.ceiling_height,
        a.furniture,
        a.metro_station,
        a.metro_time::smallint,
        a.url
      FROM users.ads a
      WHERE a.house_id = p_house_id
        AND a.floor = p_floor
        AND a.rooms = p_rooms
        AND a.price IS NOT NULL
      UNION ALL
      SELECT
        fh.house_id, fh.floor, fh.rooms,
        fh.floor AS subj_floor,
        fh.rooms AS subj_rooms,
        NULL::numeric AS area_m2,
        fh.price AS subject_price,
        fh.time_source_updated AS updated_at,
        NULL::numeric AS kitchen_area,
        NULL::numeric AS living_area,
        NULL::int AS total_floors,
        NULL::text AS bathroom,
        NULL::text AS balcony,
        NULL::text AS renovation,
        NULL::int AS construction_year,
        NULL::text AS house_type,
        NULL::numeric AS ceiling_height,
        NULL::text AS furniture,
        NULL::text AS metro_station,
        NULL::smallint AS metro_time,
        NULL::text AS url
      FROM public.flats_history fh
      WHERE fh.house_id = p_house_id
        AND fh.floor = p_floor
        AND fh.rooms = p_rooms
        AND fh.price IS NOT NULL
        AND fh.is_actual <> 0
    ) s
    ORDER BY updated_at DESC NULLS LAST
    LIMIT 1
  )
  SELECT
    area_m2, subject_price,
    subj_floor, subj_rooms,
    kitchen_area, living_area, total_floors,
    bathroom, balcony, renovation, construction_year,
    house_type, ceiling_height, furniture,
    metro_station, metro_time, url
  INTO
    v_area, v_subject_price,
    v_sub_floor, v_sub_rooms,
    v_sub_kitchen, v_sub_living, v_sub_total_floors,
    v_sub_bathroom, v_sub_balcony, v_sub_renovation, v_sub_year,
    v_sub_house_type, v_sub_ceiling, v_sub_furniture,
    v_sub_metro_station, v_sub_metro_time, v_sub_url
  FROM subject;

  IF v_subject_price IS NOT NULL AND v_area IS NOT NULL AND v_area > 0 THEN
    v_subject_ppm := v_subject_price::numeric / v_area;
  ELSE
  v_subject_ppm := NULL;
  END IF;

  WITH near_houses AS (
    SELECT p_house_id AS house_id, 0::int AS dist_m
    UNION ALL
    SELECT nh.house_id, nh.dist_m
    FROM public.get_house_near_house(p_house_id, p_radius_m) nh
  ),
  ads_raw AS (
    SELECT
      a.*,
      nh.dist_m AS near_dist_m,
      lower(split_part(coalesce(a.url, ''), '?', 1)) AS norm_url
    FROM near_houses nh
    JOIN users.ads a ON a.house_id = nh.house_id
    WHERE a.price IS NOT NULL
      AND a.rooms = p_rooms
      AND (a.status IS NULL OR a.status = TRUE)
  ),
  ads_filtered AS (
    SELECT
      a.*,
      COALESCE(a.total_area, a.living_area) AS area_m2,
      COALESCE(a.near_dist_m, 0) AS dist_m
    FROM ads_raw a
    WHERE (p_floor_delta IS NULL OR a.floor IS NULL OR abs(a.floor - p_floor) <= p_floor_delta)
      AND (
        v_area IS NULL
        OR COALESCE(a.total_area, a.living_area) IS NULL
        OR COALESCE(a.total_area, a.living_area) BETWEEN (v_area * (1 - p_area_pct)) AND (v_area * (1 + p_area_pct))
      )
      AND NOT (a.house_id = p_house_id AND a.floor = p_floor AND a.rooms = p_rooms)
  ),
  ads_dedup AS (
    SELECT DISTINCT ON (COALESCE(norm_url, a.id::text))
      a.id AS ad_id,
      NULL::bigint AS avitoid,
      NULL::bigint AS source_id,
      a.house_id,
      a.floor,
      a.rooms,
      a.dist_m,
      a.price,
      a.area_m2,
      a.kitchen_area,
      a.living_area,
      a.total_floors,
      a.bathroom,
      a.balcony,
      a.renovation,
      a.construction_year,
      a.house_type,
      a.ceiling_height,
      a.furniture,
      a.metro_station,
      a.metro_time::smallint AS metro_time,
      NULL::text AS metro_way,
      a.url,
      a.updated_at
    FROM ads_filtered a
    ORDER BY COALESCE(norm_url, a.id::text), a.updated_at DESC NULLS LAST, a.id DESC
  ),
  comps AS (
    SELECT
      *,
      CASE WHEN area_m2 IS NOT NULL AND area_m2 > 0 THEN price::numeric / area_m2 ELSE NULL END AS ppm
    FROM ads_dedup
  ),
  stats AS (
    SELECT
      COUNT(*)::int AS cnt,
      percentile_cont(0.25) WITHIN GROUP (ORDER BY ppm) AS p25_ppm,
      percentile_cont(0.50) WITHIN GROUP (ORDER BY ppm) AS p50_ppm,
      percentile_cont(0.75) WITHIN GROUP (ORDER BY ppm) AS p75_ppm
    FROM comps
    WHERE ppm IS NOT NULL
  ),
  pos AS (
    SELECT
      CASE
        WHEN v_subject_ppm IS NULL THEN NULL
        WHEN (SELECT cnt FROM stats) = 0 THEN NULL
        ELSE (
          SELECT COUNT(*)::numeric / NULLIF((SELECT cnt FROM stats)::numeric, 0)
          FROM comps
          WHERE ppm IS NOT NULL AND ppm <= v_subject_ppm
        )
      END AS position01
  ),
  top_items AS (
    SELECT
      COALESCE(
        jsonb_agg(
          jsonb_build_object(
            'ad_id', ad_id,
            'avitoid', avitoid,
            'source_id', source_id,
            'house_id', house_id,
            'floor', floor,
            'rooms', rooms,
            'dist_m', dist_m,
            'price', price,
            'area_m2', area_m2,
            'kitchen_area', kitchen_area,
            'living_area', living_area,
            'total_floors', total_floors,
            'bathroom', bathroom,
            'balcony', balcony,
            'renovation', renovation,
            'construction_year', construction_year,
            'house_type', house_type,
            'ceiling_height', ceiling_height,
            'furniture', furniture,
            'metro_station', metro_station,
            'metro_time', metro_time,
            'metro_way', metro_way,
            'ppm', ppm,
            'url', url,
            'updated', updated_at
          )
          ORDER BY ppm ASC NULLS LAST, price ASC
        ),
        '[]'::jsonb
      ) AS items
    FROM (
      SELECT * FROM comps
      ORDER BY ppm ASC NULLS LAST, price ASC
      LIMIT GREATEST(1, LEAST(p_limit_top, 200))
    ) t
  )
  SELECT
    s.cnt, s.p25_ppm, s.p50_ppm, s.p75_ppm,
    p.position01,
    ti.items
  INTO
    v_cnt, v_p25_ppm, v_p50_ppm, v_p75_ppm,
    v_pos,
    v_top_items
  FROM stats s
  CROSS JOIN pos p
  CROSS JOIN top_items ti;

  v_report :=
    jsonb_build_object(
      'meta', jsonb_build_object(
        'tg_user_id', p_tg_user_id,
        'key', jsonb_build_object('house_id', p_house_id, 'floor', p_floor, 'rooms', p_rooms),
        'radius_m', p_radius_m,
        'report_type', 'analogs',
        'params', v_params,
        'generated_at', now()
      ),
      'subject', jsonb_build_object(
        'area_m2', v_area,
        'price', v_subject_price,
        'floor', COALESCE(v_sub_floor, p_floor),
        'rooms', COALESCE(v_sub_rooms, p_rooms),
        'ppm', v_subject_ppm,
        'kitchen_area', v_sub_kitchen,
        'living_area', v_sub_living,
        'total_floors', v_sub_total_floors,
        'bathroom', v_sub_bathroom,
        'balcony', v_sub_balcony,
        'renovation', v_sub_renovation,
        'construction_year', v_sub_year,
        'house_type', v_sub_house_type,
        'ceiling_height', v_sub_ceiling,
        'furniture', v_sub_furniture,
        'metro_station', v_sub_metro_station,
        'metro_time', v_sub_metro_time,
        'url', v_sub_url,
        'dist_m', 0
      ),
      'market', jsonb_build_object(
        'competitors_count', v_cnt,
        'ppm_p25', v_p25_ppm,
        'ppm_p50', v_p50_ppm,
        'ppm_p75', v_p75_ppm,
        'position01', v_pos,
        'verdict',
          CASE
            WHEN v_subject_ppm IS NULL OR v_p50_ppm IS NULL THEN 'insufficient_data'
            WHEN v_p75_ppm IS NOT NULL AND v_subject_ppm > v_p75_ppm THEN 'overpriced'
            WHEN v_p25_ppm IS NOT NULL AND v_subject_ppm < v_p25_ppm THEN 'underpriced'
            ELSE 'in_market'
          END
      ),
      'top_competitors', v_top_items
    );

  INSERT INTO users.flat_reports (
    tg_user_id, house_id, floor, rooms, radius_m, report_date, report_type, params,
    report_json, created_at, updated_at
  )
  VALUES (
    p_tg_user_id, p_house_id, p_floor, p_rooms, p_radius_m, CURRENT_DATE,
    'analogs', v_params,
    v_report, now(), now()
  )
  ON CONFLICT (tg_user_id, house_id, floor, rooms, radius_m, report_date, report_type, params)
  DO UPDATE SET
    report_json = EXCLUDED.report_json,
    updated_at = now();

  RETURN v_report;
END;
$$;
