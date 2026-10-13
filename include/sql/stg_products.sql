-- include/sql/stg_products.sql
-- Staging layer for the product catalogue.
--
-- A blank category is repaired to 'UNCATEGORISED' under the lenient policy so
-- the 48 orders on those 3 products stay in the margin report, visibly
-- unattributed, rather than disappearing from it.

CREATE OR REPLACE TABLE stg_products AS
WITH ranked AS (
    SELECT *, row_number() OVER (
               PARTITION BY product_id
               ORDER BY _loaded_at DESC, _file_seq DESC, _row_num DESC) AS _rn,
           count(*) OVER (PARTITION BY product_id) AS _copies
    FROM raw_products
),
typed AS (
    SELECT
        nullif(trim(product_id), '')                            AS product_id,
        trim(product_name)                                      AS product_name,
        coalesce(nullif(trim(category), ''), 'UNCATEGORISED')    AS category,
        trim(sub_category)                                      AS sub_category,
        trim(brand)                                             AS brand,
        TRY_CAST(mrp AS DOUBLE)                                 AS mrp,
        TRY_CAST(cost_price AS DOUBLE)                          AS cost_price,
        trim(unit)                                              AS unit,
        upper(trim(is_perishable)) IN ('TRUE', 'Y', 'YES', '1') AS is_perishable,
        TRY_CAST(launch_date AS DATE)                           AS launch_date,
        category                                                AS raw_category,
        _copies, _source_file, _run_id, _loaded_at
    FROM ranked WHERE _rn = 1
),
checked AS (
    SELECT *,
        concat_ws('; ',
            CASE WHEN product_id IS NULL THEN 'Critical:product_id_present' END
        ) AS critical_reason,
        concat_ws('; ',
            CASE WHEN _copies > 1
                 THEN 'Warning:product_id_had_duplicates' END,
            CASE WHEN is_blank(raw_category)
                 THEN '@master_sev@:category_present' END,
            CASE WHEN mrp IS NULL OR mrp <= 0
                 THEN '@master_sev@:mrp_positive' END,
            CASE WHEN cost_price > mrp
                 THEN '@master_sev@:cost_not_above_mrp' END,
            CASE WHEN cost_price IS NULL
                 THEN '@master_sev@:cost_price_present' END,
            CASE WHEN unit NOT IN ('kg', 'pack', 'unit')
                 THEN 'Warning:unit_in_allowed_set' END,
            CASE WHEN launch_date > CAST('@now_ts@' AS DATE)
                 THEN 'Warning:launch_date_not_future' END
        ) AS other_reason
    FROM typed
)
SELECT product_id, product_name, category, sub_category, brand, mrp, cost_price,
       unit, is_perishable, launch_date,
       CASE WHEN critical_reason <> ''           THEN 'QUARANTINED'
            WHEN other_reason LIKE '%Critical:%' THEN 'QUARANTINED'
            WHEN other_reason <> ''              THEN 'WARNING'
            ELSE 'PASS' END                      AS dq_status,
       nullif(concat_ws('; ', nullif(critical_reason, ''),
                              nullif(other_reason, '')), '') AS failure_reason,
       _source_file, _run_id, _loaded_at
FROM checked;
