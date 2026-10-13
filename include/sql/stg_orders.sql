-- include/sql/stg_orders.sql
-- Staging layer for orders: cast, standardise, apply the quality rules, and
-- UPSERT on order_id.
--
-- The upsert is the heart of criteria 5 and 6. `stg_orders` has a PRIMARY KEY
-- on order_id and the insert ends in ON CONFLICT DO UPDATE, so:
--   * re-running the DAG restates the same rows instead of appending  (5)
--   * the increment file's 10 corrections overwrite their September originals
--     while its 150 new orders are inserted                           (6)
--
-- Placeholders @master_sev@ and @now_ts@ are substituted by the DAG.
-- Row-level NULL handling deliberately mirrors the pandas checks in
-- dags/nammamart_etl_arnav_ganguly.py: a NULL quantity or discount must FAIL
-- its check, which plain SQL comparison would leave as NULL (= not failing).

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id          VARCHAR PRIMARY KEY,
    order_ts          TIMESTAMP,
    order_date        DATE,
    customer_id       VARCHAR,
    store_id          VARCHAR,
    product_id        VARCHAR,
    quantity          INTEGER,
    unit_price        DOUBLE,
    discount_pct      DOUBLE,
    order_amount      DOUBLE,
    payment_mode      VARCHAR,
    order_status      VARCHAR,
    channel           VARCHAR,
    delivery_minutes  DOUBLE,
    dq_status         VARCHAR,
    failure_reason    VARCHAR,
    _source_file      VARCHAR,
    _run_id           VARCHAR,
    _loaded_at        TIMESTAMP
);

-- One row per order_id: the most recently loaded copy wins, and within a
-- single file the last line wins — the same rule as the ETL's keep="last".
CREATE OR REPLACE TEMP VIEW _orders_ranked AS
SELECT *,
       row_number() OVER (
           PARTITION BY order_id
           ORDER BY _loaded_at DESC, _file_seq DESC, _row_num DESC
       ) AS _rn,
       count(*) OVER (PARTITION BY order_id) AS _copies
FROM raw_orders;

CREATE OR REPLACE TEMP VIEW _orders_typed AS
SELECT
    order_id,
    TRY_CAST(order_ts AS TIMESTAMP)                  AS order_ts,
    CAST(TRY_CAST(order_ts AS TIMESTAMP) AS DATE)    AS order_date,
    nullif(trim(customer_id), '')                    AS customer_id,
    nullif(trim(store_id), '')                       AS store_id,
    nullif(trim(product_id), '')                     AS product_id,
    TRY_CAST(quantity AS INTEGER)                    AS quantity,
    TRY_CAST(unit_price AS DOUBLE)                   AS unit_price,
    TRY_CAST(discount_pct AS DOUBLE)                 AS discount_pct,
    round(TRY_CAST(order_amount AS DOUBLE), 2)       AS order_amount,
    canon_payment_mode(payment_mode)                 AS payment_mode,
    upper(trim(order_status))                        AS order_status,
    trim(channel)                                    AS channel,
    TRY_CAST(delivery_minutes AS DOUBLE)             AS delivery_minutes,
    -- raw values kept for the checks that must judge the original text
    order_ts            AS raw_order_ts,
    order_status        AS raw_order_status,
    payment_mode        AS raw_payment_mode,
    delivery_minutes    AS raw_delivery_minutes,
    _copies, _source_file, _run_id, _loaded_at
FROM _orders_ranked
WHERE _rn = 1;

CREATE OR REPLACE TEMP VIEW _orders_checked AS
SELECT *,
    -- Critical: the row must not reach the mart
    concat_ws('; ',
        CASE WHEN customer_id IS NULL
             THEN 'Critical:customer_id_present' END,
        CASE WHEN is_blank(raw_order_ts)
             THEN 'Critical:order_ts_present' END,
        CASE WHEN order_status = 'DELIVERED' AND is_blank(raw_delivery_minutes)
             THEN 'Critical:delivered_has_delivery_minutes' END,
        CASE WHEN order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED')
             THEN 'Critical:status_in_allowed_set' END,
        CASE WHEN NOT is_blank(raw_order_ts) AND order_ts IS NULL
             THEN 'Critical:order_ts_parses' END,
        CASE WHEN discount_pct IS NULL OR discount_pct NOT BETWEEN 0 AND 100
             THEN 'Critical:discount_pct_0_to_100' END,
        CASE WHEN quantity IS NULL OR quantity <= 0
             THEN 'Critical:quantity_positive' END,
        CASE WHEN unit_price IS NULL OR unit_price <= 0
             THEN 'Critical:unit_price_positive' END,
        CASE WHEN abs(order_amount - round(
                      quantity * unit_price * (1 - discount_pct / 100), 2)) > 0.01
             THEN 'Critical:amount_matches_formula' END,
        CASE WHEN delivery_minutes >= 120
             THEN 'Critical:delivery_minutes_plausible' END,
        CASE WHEN customer_id IS NOT NULL AND customer_id NOT IN (
                      SELECT customer_id FROM stg_customers
                      WHERE dq_status <> 'QUARANTINED')
             THEN 'Critical:customer_id_known' END,
        CASE WHEN product_id IS NOT NULL AND product_id NOT IN (
                      SELECT product_id FROM stg_products
                      WHERE dq_status <> 'QUARANTINED')
             THEN 'Critical:product_id_known' END,
        CASE WHEN store_id IS NOT NULL AND store_id NOT IN (
                      SELECT store_id FROM stg_stores
                      WHERE dq_status <> 'QUARANTINED')
             THEN 'Critical:store_id_known' END,
        CASE WHEN order_ts > CAST('@now_ts@' AS TIMESTAMP)
             THEN 'Critical:order_ts_not_future' END
    ) AS critical_reason,
    -- Warning: the row loads, but the defect is recorded
    concat_ws('; ',
        CASE WHEN payment_mode IS NULL
             THEN 'Warning:payment_mode_mappable' END,
        CASE WHEN _copies > 1
             THEN 'Warning:order_id_had_duplicates' END
    ) AS warning_reason
FROM _orders_typed;

INSERT INTO stg_orders
SELECT
    order_id, order_ts, order_date, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount, payment_mode,
    order_status, channel, delivery_minutes,
    CASE WHEN critical_reason <> '' THEN 'QUARANTINED'
         WHEN warning_reason  <> '' THEN 'WARNING'
         ELSE 'PASS' END                                   AS dq_status,
    nullif(concat_ws('; ', nullif(critical_reason, ''),
                           nullif(warning_reason, '')), '') AS failure_reason,
    _source_file, _run_id, _loaded_at
FROM _orders_checked
ON CONFLICT (order_id) DO UPDATE SET
    order_ts         = excluded.order_ts,
    order_date       = excluded.order_date,
    customer_id      = excluded.customer_id,
    store_id         = excluded.store_id,
    product_id       = excluded.product_id,
    quantity         = excluded.quantity,
    unit_price       = excluded.unit_price,
    discount_pct     = excluded.discount_pct,
    order_amount     = excluded.order_amount,
    payment_mode     = excluded.payment_mode,
    order_status     = excluded.order_status,
    channel          = excluded.channel,
    delivery_minutes = excluded.delivery_minutes,
    dq_status        = excluded.dq_status,
    failure_reason   = excluded.failure_reason,
    _source_file     = excluded._source_file,
    _run_id          = excluded._run_id,
    _loaded_at       = excluded._loaded_at;

-- Criterion 4, the ELT half: the duplicate copies that lost the row_number
-- race are set aside with a reason rather than silently dropped. raw_orders
-- still holds them untouched; this table is the queryable index of what was
-- rejected and why.
-- Every column is cast to VARCHAR on both sides: one branch comes from the raw
-- text layer and the other from the typed staging table, and a quarantine
-- record must show the value exactly as it arrived anyway.
CREATE OR REPLACE TABLE stg_orders_quarantine AS
SELECT CAST(order_id AS VARCHAR)         AS order_id,
       CAST(order_ts AS VARCHAR)         AS order_ts,
       CAST(customer_id AS VARCHAR)      AS customer_id,
       CAST(store_id AS VARCHAR)         AS store_id,
       CAST(product_id AS VARCHAR)       AS product_id,
       CAST(quantity AS VARCHAR)         AS quantity,
       CAST(unit_price AS VARCHAR)       AS unit_price,
       CAST(discount_pct AS VARCHAR)     AS discount_pct,
       CAST(order_amount AS VARCHAR)     AS order_amount,
       CAST(payment_mode AS VARCHAR)     AS payment_mode,
       CAST(order_status AS VARCHAR)     AS order_status,
       'Critical:order_id_unique (superseded copy)' AS failure_reason,
       CAST(_source_file AS VARCHAR)     AS _source_file,
       CAST(_run_id AS VARCHAR)          AS _run_id,
       CAST(_loaded_at AS VARCHAR)       AS _loaded_at
FROM _orders_ranked
WHERE _rn > 1
UNION ALL
SELECT CAST(order_id AS VARCHAR), CAST(order_ts AS VARCHAR),
       CAST(customer_id AS VARCHAR), CAST(store_id AS VARCHAR),
       CAST(product_id AS VARCHAR), CAST(quantity AS VARCHAR),
       CAST(unit_price AS VARCHAR), CAST(discount_pct AS VARCHAR),
       CAST(order_amount AS VARCHAR), CAST(payment_mode AS VARCHAR),
       CAST(order_status AS VARCHAR), CAST(failure_reason AS VARCHAR),
       CAST(_source_file AS VARCHAR), CAST(_run_id AS VARCHAR),
       CAST(_loaded_at AS VARCHAR)
FROM stg_orders
WHERE dq_status = 'QUARANTINED';
