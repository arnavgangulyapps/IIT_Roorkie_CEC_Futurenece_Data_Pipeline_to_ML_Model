-- include/sql/stg_stores.sql
-- Staging layer for the store master.
--
-- Masters are rebuilt wholesale (CREATE OR REPLACE) rather than upserted: they
-- are small, they arrive complete every day, and a full rebuild is trivially
-- idempotent. Only `orders` needs a true upsert, because only orders receive
-- an incremental feed with corrections.
--
-- @master_sev@ is 'Critical' or 'Warning' depending on the master-data
-- severity policy — see include/nammamart_dq.py for why that is switchable and
-- what the lenient default costs/saves. Under 'Warning', a blank zone is
-- repaired to 'UNKNOWN' so S07's 176 orders stay visible in the COO's zone
-- report instead of being deleted.

CREATE OR REPLACE TABLE stg_stores AS
WITH ranked AS (
    SELECT *, row_number() OVER (
               PARTITION BY store_id
               ORDER BY _loaded_at DESC, _file_seq DESC, _row_num DESC) AS _rn,
           count(*) OVER (PARTITION BY store_id) AS _copies
    FROM raw_stores
),
typed AS (
    SELECT
        nullif(trim(store_id), '')                       AS store_id,
        trim(store_name)                                 AS store_name,
        coalesce(nullif(trim(zone), ''), 'UNKNOWN')      AS zone,
        trim(area)                                       AS area,
        pincode,
        TRY_CAST(open_date AS DATE)                      AS open_date,
        mask_name(manager_name)                          AS manager_name,
        TRY_CAST(capacity_orders_per_day AS INTEGER)     AS capacity_orders_per_day,
        zone                                             AS raw_zone,
        _copies, _source_file, _run_id, _loaded_at
    FROM ranked WHERE _rn = 1
),
checked AS (
    SELECT *,
        concat_ws('; ',
            CASE WHEN store_id IS NULL THEN 'Critical:store_id_present' END,
            CASE WHEN open_date > CAST('@now_ts@' AS DATE)
                 THEN 'Critical:open_date_not_future' END
        ) AS critical_reason,
        concat_ws('; ',
            CASE WHEN _copies > 1
                 THEN 'Warning:store_id_had_duplicates' END,
            CASE WHEN is_blank(raw_zone)
                 THEN '@master_sev@:zone_present' END,
            CASE WHEN capacity_orders_per_day IS NULL OR capacity_orders_per_day <= 0
                 THEN '@master_sev@:capacity_positive' END,
            CASE WHEN NOT regexp_matches(coalesce(pincode, ''), '^[0-9]{6}$')
                 THEN 'Warning:pincode_6_digits' END,
            CASE WHEN zone NOT IN ('North', 'South', 'East', 'West')
                 THEN 'Warning:zone_in_known_set' END
        ) AS other_reason
    FROM typed
)
SELECT store_id, store_name, zone, area, pincode, open_date, manager_name,
       capacity_orders_per_day,
       CASE WHEN critical_reason <> ''                       THEN 'QUARANTINED'
            WHEN other_reason LIKE '%Critical:%'             THEN 'QUARANTINED'
            WHEN other_reason <> ''                          THEN 'WARNING'
            ELSE 'PASS' END                                  AS dq_status,
       nullif(concat_ws('; ', nullif(critical_reason, ''),
                              nullif(other_reason, '')), '') AS failure_reason,
       _source_file, _run_id, _loaded_at
FROM checked;
