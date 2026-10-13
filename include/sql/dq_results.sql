-- include/sql/dq_results.sql
-- Part D, the ELT half: one `dq_results` row per check, per run — including
-- the checks that passed.
--
-- Recording only failures would make a passing check indistinguishable from a
-- check that never ran, which defeats the point of a quality scorecard you
-- trend over time.
--
-- Two inputs:
--   _check_registry — the declared checks, per dataset, with their dimension
--                     and the severity they carry under the active policy
--   _reasons        — the failures staging actually recorded, unpacked from
--                     the semicolon-separated failure_reason strings
--
-- A LEFT JOIN from the registry gives rows_failed = 0 / status = 'PASS' for
-- everything that passed. Any check that appears in staging but not in the
-- registry lands with dimension 'Unmapped', and the DAG fails the task if that
-- happens — that is the drift alarm between the two.

CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    dag_id        VARCHAR,
    check_name    VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,
    checked_at    VARCHAR
);

-- Restate this run rather than appending, so a re-run does not double the
-- scorecard (the same rule the ETL writer follows).
DELETE FROM dq_results WHERE run_id = '@run_id@' AND dag_id = '@dag_id@';

-- '@master_sev@' is substituted with 'Critical' or 'Warning' by the DAG,
-- matching the master-data severity policy in force for this run.
CREATE OR REPLACE TEMP TABLE _check_registry AS SELECT * FROM (VALUES
    -- orders
    ('orders', 'customer_id_present',            'Completeness', 'Critical'),
    ('orders', 'order_ts_present',               'Completeness', 'Critical'),
    ('orders', 'delivered_has_delivery_minutes', 'Completeness', 'Critical'),
    ('orders', 'order_id_unique',                'Uniqueness',   'Critical'),
    ('orders', 'order_id_had_duplicates',        'Uniqueness',   'Warning'),
    ('orders', 'status_in_allowed_set',          'Validity',     'Critical'),
    ('orders', 'payment_mode_mappable',          'Validity',     'Warning'),
    ('orders', 'order_ts_parses',                'Validity',     'Critical'),
    ('orders', 'discount_pct_0_to_100',          'Validity',     'Critical'),
    ('orders', 'quantity_positive',              'Accuracy',     'Critical'),
    ('orders', 'unit_price_positive',            'Accuracy',     'Critical'),
    ('orders', 'amount_matches_formula',         'Accuracy',     'Critical'),
    ('orders', 'delivery_minutes_plausible',     'Accuracy',     'Critical'),
    ('orders', 'customer_id_known',              'Consistency',  'Critical'),
    ('orders', 'product_id_known',               'Consistency',  'Critical'),
    ('orders', 'store_id_known',                 'Consistency',  'Critical'),
    ('orders', 'order_ts_not_future',            'Timeliness',   'Critical'),
    -- payments
    ('payments', 'payment_id_present',           'Completeness', 'Critical'),
    ('payments', 'gateway_ref_present',          'Completeness', 'Warning'),
    ('payments', 'payment_id_unique',            'Uniqueness',   'Critical'),
    ('payments', 'payment_id_had_duplicates',    'Uniqueness',   'Warning'),
    ('payments', 'one_payment_per_order',        'Uniqueness',   'Warning'),
    ('payments', 'payment_mode_mappable',        'Validity',     'Warning'),
    ('payments', 'payment_status_known',         'Validity',     'Critical'),
    ('payments', 'amount_paid_positive',         'Accuracy',     'Critical'),
    ('payments', 'order_id_known',               'Consistency',  'Warning'),
    ('payments', 'success_amount_matches_order', 'Consistency',  'Warning'),
    ('payments', 'payment_ts_not_future',        'Timeliness',   'Critical'),
    -- customers
    ('customers', 'customer_id_present',         'Completeness', 'Critical'),
    ('customers', 'city_present',                'Completeness', 'Warning'),
    ('customers', 'customer_id_unique',          'Uniqueness',   'Critical'),
    ('customers', 'customer_id_had_duplicates',  'Uniqueness',   'Warning'),
    ('customers', 'email_format',                'Validity',     'Warning'),
    ('customers', 'phone_10_digits',             'Validity',     'Warning'),
    ('customers', 'pincode_6_digits',            'Validity',     'Warning'),
    ('customers', 'loyalty_tier_known',          'Consistency',  'Warning'),
    ('customers', 'signup_date_not_future',      'Timeliness',   '@master_sev@'),
    -- products
    ('products', 'product_id_present',           'Completeness', 'Critical'),
    ('products', 'category_present',             'Completeness', '@master_sev@'),
    ('products', 'product_id_unique',            'Uniqueness',   'Critical'),
    ('products', 'product_id_had_duplicates',    'Uniqueness',   'Warning'),
    ('products', 'unit_in_allowed_set',          'Validity',     'Warning'),
    ('products', 'mrp_positive',                 'Accuracy',     '@master_sev@'),
    ('products', 'cost_not_above_mrp',           'Accuracy',     '@master_sev@'),
    ('products', 'cost_price_present',           'Consistency',  '@master_sev@'),
    ('products', 'launch_date_not_future',       'Timeliness',   'Warning'),
    -- stores
    ('stores', 'store_id_present',               'Completeness', 'Critical'),
    ('stores', 'zone_present',                   'Completeness', '@master_sev@'),
    ('stores', 'store_id_unique',                'Uniqueness',   'Critical'),
    ('stores', 'store_id_had_duplicates',        'Uniqueness',   'Warning'),
    ('stores', 'pincode_6_digits',               'Validity',     'Warning'),
    ('stores', 'capacity_positive',              'Accuracy',     '@master_sev@'),
    ('stores', 'zone_in_known_set',              'Consistency',  'Warning'),
    ('stores', 'open_date_not_future',           'Timeliness',   'Critical')
) AS t(dataset, check_name, dimension, severity);

-- Explode every semicolon-separated reason into one row per (dataset, check).
CREATE OR REPLACE TEMP VIEW _reasons AS
WITH all_rows AS (
              SELECT 'orders'    AS dataset, failure_reason FROM stg_orders
    UNION ALL SELECT 'payments',   failure_reason FROM stg_payments
    UNION ALL SELECT 'customers',  failure_reason FROM stg_customers
    UNION ALL SELECT 'products',   failure_reason FROM stg_products
    UNION ALL SELECT 'stores',     failure_reason FROM stg_stores
),
exploded AS (
    SELECT dataset, trim(unnest(string_split(failure_reason, ';'))) AS reason
    FROM all_rows WHERE failure_reason IS NOT NULL
)
SELECT dataset,
       split_part(reason, ':', 1)                     AS severity,
       -- drop any ' (superseded copy)' style annotation after the check name
       split_part(split_part(reason, ':', 2), ' ', 1) AS check_name
FROM exploded
WHERE reason <> '';

CREATE OR REPLACE TEMP VIEW _failures AS
SELECT dataset, check_name, min(severity) AS severity, count(*) AS rows_failed
FROM _reasons GROUP BY dataset, check_name;

CREATE OR REPLACE TEMP VIEW _row_counts AS
              SELECT 'orders' AS dataset, count(*) AS rows_checked FROM stg_orders
    UNION ALL SELECT 'payments',  count(*) FROM stg_payments
    UNION ALL SELECT 'customers', count(*) FROM stg_customers
    UNION ALL SELECT 'products',  count(*) FROM stg_products
    UNION ALL SELECT 'stores',    count(*) FROM stg_stores;

-- Declared checks, with their actual outcome. A `*_unique` check's failures
-- are the superseded copies, which staging drops rather than keeps, so they are
-- counted from stg_dedup_stats instead of from the staging table.
INSERT INTO dq_results
SELECT
    '@run_id@', '@dag_id@',
    reg.dataset || '.' || reg.check_name,
    reg.dimension,
    reg.severity,
    c.rows_checked,
    coalesce(
        CASE WHEN reg.check_name LIKE '%\_unique' THEN d.superseded END,
        f.rows_failed,
        0)                                           AS rows_failed,
    CASE WHEN coalesce(
                 CASE WHEN reg.check_name LIKE '%\_unique' THEN d.superseded END,
                 f.rows_failed, 0) > 0
         THEN 'FAIL' ELSE 'PASS' END                 AS status,
    '@now_ts@'
FROM _check_registry reg
JOIN _row_counts c       ON c.dataset = reg.dataset
LEFT JOIN stg_dedup_stats d ON d.dataset = reg.dataset
LEFT JOIN _failures f
       ON f.dataset = reg.dataset AND f.check_name = reg.check_name

UNION ALL

-- Anything staging recorded that the registry does not declare. The DAG treats
-- a non-empty result here as a failure: the two rule lists have drifted.
SELECT '@run_id@', '@dag_id@',
       f.dataset || '.' || f.check_name, 'Unmapped', f.severity,
       c.rows_checked, f.rows_failed, 'FAIL', '@now_ts@'
FROM _failures f
JOIN _row_counts c ON c.dataset = f.dataset
WHERE NOT EXISTS (
    SELECT 1 FROM _check_registry reg
    WHERE reg.dataset = f.dataset AND reg.check_name = f.check_name
);

-- ── batch-level checks (Part F experiments 7, 8, 9) ──────────────────────────
--
-- The checks above are all row-level, and experiments 7, 8 and 9 showed that a
-- 98% volume collapse, a 100x price drift and 30-day-stale data all produce a
-- fully green ELT run. These three close that gap. Added after the experiments
-- found them, not before — the point of Part F is that running the thing finds
-- what reading it does not.
--
-- They are Warning severity: each describes the batch, not a row, so there is
-- no row to quarantine, and a human has to judge whether a 40% volume drop is
-- Diwali or a broken export.

-- Volume. Compared against this DAG's own trailing median rather than a fixed
-- floor, so it keeps working as NammaMart grows. The 500 fallback only applies
-- on the very first run, when there is no history to compare against.
INSERT INTO dq_results
WITH cur AS (
    SELECT rows_in_load AS n FROM stg_dedup_stats WHERE dataset = 'orders'
),
hist AS (
    SELECT median(rows_checked) AS typical FROM dq_results
    WHERE dag_id = '@dag_id@' AND run_id <> '@run_id@'
      AND check_name = 'orders.batch_volume_not_collapsed'
)
SELECT '@run_id@', '@dag_id@', 'orders.batch_volume_not_collapsed',
       'Volume', 'Warning', cur.n,
       CASE WHEN cur.n < coalesce(hist.typical, 500) * 0.5 THEN 1 ELSE 0 END,
       CASE WHEN cur.n < coalesce(hist.typical, 500) * 0.5 THEN 'FAIL' ELSE 'PASS' END,
       '@now_ts@'
FROM cur, hist;

-- Freshness. Measured on the rows this run actually loaded, not on the
-- cumulative staging table, which would look fresh forever once any recent
-- order had ever arrived.
INSERT INTO dq_results
WITH cur AS (
    SELECT max(TRY_CAST(order_ts AS TIMESTAMP)) AS newest,
           count(*) AS n
    FROM raw_orders WHERE _run_id = '@run_id@'
)
SELECT '@run_id@', '@dag_id@', 'orders.batch_freshness_within_1_day',
       'Timeliness', 'Warning', cur.n,
       CASE WHEN cur.newest IS NULL
                 OR cur.newest < CAST('@now_ts@' AS TIMESTAMP) - INTERVAL 1 DAY
            THEN 1 ELSE 0 END,
       CASE WHEN cur.newest IS NULL
                 OR cur.newest < CAST('@now_ts@' AS TIMESTAMP) - INTERVAL 1 DAY
            THEN 'FAIL' ELSE 'PASS' END,
       '@now_ts@'
FROM cur;

-- Distribution. Stores are compared against each other, so no history is
-- needed — which matters on day one. A store whose average delivered order
-- value is more than 5x or less than 1/5 of the median store's is flagged.
INSERT INTO dq_results
WITH per_store AS (
    SELECT store_id, avg(order_amount) AS mean_value
    FROM stg_orders
    WHERE dq_status <> 'QUARANTINED' AND order_status = 'DELIVERED'
      AND order_amount IS NOT NULL
    GROUP BY store_id
),
typical AS (SELECT median(mean_value) AS m FROM per_store),
flagged AS (
    SELECT count(*) AS n FROM per_store, typical
    WHERE typical.m > 0
      AND (per_store.mean_value > typical.m * 5
           OR per_store.mean_value < typical.m / 5)
)
SELECT '@run_id@', '@dag_id@', 'orders.store_value_distribution',
       'Distribution', 'Warning',
       (SELECT count(*) FROM per_store), flagged.n,
       CASE WHEN flagged.n > 0 THEN 'FAIL' ELSE 'PASS' END, '@now_ts@'
FROM flagged;
