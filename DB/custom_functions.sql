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

  v_p25_ppm numeric;
  v_p50_ppm numeric;
  v_p75_ppm numeric;

  v_cnt int;
  v_pos numeric;

  v_top_items jsonb;

  v_report_type text := 'near_rooms';
  v_params jsonb := '{}'::jsonb;
BEGIN
  -- субъект: берём площадь из flats/ads, цену из users.ads (если есть) либо из flats_history
  WITH subject AS (
    SELECT
      f.house_id, f.floor, f.rooms,
      COALESCE(f.area, ua.total_area, fh.area_m2) AS area_m2,
      COALESCE(ua.price, fh.price) AS subject_price
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
      SELECT price, COALESCE(area, total_area) AS area_m2
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
    LIMIT 1
  )
  SELECT area_m2, subject_price
  INTO v_area, v_subject_price
  FROM subject;

  IF v_subject_price IS NOT NULL AND v_area IS NOT NULL AND v_area > 0 THEN
    v_subject_ppm := (v_subject_price::numeric / v_area);
  ELSE
    v_subject_ppm := NULL;
  END IF;

  -- конкуренты (near + same rooms) + топ
  WITH near_houses AS (
    SELECT p_house_id AS house_id, 0::int AS dist_m
    UNION ALL
    SELECT nh.house_id, nh.dist_m
    FROM public.get_house_near_house(p_house_id, p_radius_m) nh
  ),
  comps AS (
    SELECT
      fh.id AS ad_id,
      fh.house_id, fh.floor, fh.rooms,
      nh.dist_m,
      fh.price,
      COALESCE(f.area, ua.total_area) AS area_m2,
      CASE
        WHEN COALESCE(f.area, ua.total_area) IS NOT NULL
         AND COALESCE(f.area, ua.total_area) > 0
         AND fh.price IS NOT NULL
        THEN fh.price::numeric / COALESCE(f.area, ua.total_area)
        ELSE NULL
      END AS ppm,
      fh.url,
      fh.time_source_updated
    FROM near_houses nh
    JOIN public.flats_history fh
      ON fh.house_id = nh.house_id
     AND fh.is_actual <> 0
     AND fh.rooms = p_rooms
    LEFT JOIN public.flats f
      ON f.house_id = fh.house_id AND f.floor = fh.floor AND f.rooms = fh.rooms
    LEFT JOIN LATERAL (
      SELECT a.total_area
      FROM users.ads a
      WHERE a.house_id = fh.house_id
        AND a.floor = fh.floor
        AND a.rooms = fh.rooms
      ORDER BY a.updated_at DESC NULLS LAST
      LIMIT 1
    ) ua ON TRUE
    WHERE fh.price IS NOT NULL
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
            'ppm', ppm,
            'url', url,
            'updated', time_source_updated
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
        'ppm', v_subject_ppm
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
    SELECT
      f.house_id, f.floor, f.rooms,
      COALESCE(f.area, ua.total_area, fh.area_m2) AS area_m2,
      COALESCE(ua.price, fh.price) AS subject_price
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
      SELECT price, COALESCE(area, total_area) AS area_m2
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
    LIMIT 1
  )
  SELECT area_m2, subject_price
  INTO v_area, v_subject_price
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
  comps_raw AS (
    SELECT
      fh.id AS ad_id,
      fh.avitoid, fh.source_id,
      fh.house_id, fh.floor, fh.rooms,
      nh.dist_m,
      fh.price,
      COALESCE(f.area, ua.total_area) AS area_m2,
      fh.url,
      fh.time_source_updated
    FROM near_houses nh
    JOIN public.flats_history fh
      ON fh.house_id = nh.house_id
     AND fh.is_actual <> 0
     AND fh.price IS NOT NULL
    LEFT JOIN public.flats f
      ON f.house_id = fh.house_id AND f.floor = fh.floor AND f.rooms = fh.rooms
    LEFT JOIN LATERAL (
      SELECT a.total_area
      FROM users.ads a
      WHERE a.house_id = fh.house_id
        AND a.floor = fh.floor
        AND a.rooms = fh.rooms
      ORDER BY a.updated_at DESC NULLS LAST
      LIMIT 1
    ) ua ON TRUE
  ),
  comps AS (
    SELECT
      *,
      CASE WHEN area_m2 IS NOT NULL AND area_m2 > 0 THEN price::numeric / area_m2 ELSE NULL END AS ppm
    FROM comps_raw
    WHERE rooms = p_rooms
      AND (p_floor_delta IS NULL OR abs(floor - p_floor) <= p_floor_delta)
      AND (
        v_area IS NULL
        OR area_m2 IS NULL
        OR area_m2 BETWEEN (v_area * (1 - p_area_pct)) AND (v_area * (1 + p_area_pct))
      )
      AND NOT (house_id = p_house_id AND floor = p_floor AND rooms = p_rooms)
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
            'ppm', ppm,
            'url', url,
            'updated', time_source_updated
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
        'ppm', v_subject_ppm
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
