-- include/sql/stg_payments.sql
-- Staging layer for the payment gateway export.
--
-- Note which failures are Warning here. A payment whose order was rejected,
-- and a SUCCESS payment whose amount disagrees with its order, both LOAD.
-- They are the finance findings themselves — quarantining them would hide
-- real cash, which is the exact failure of the spreadsheet process this
-- pipeline replaces. They surface as named exceptions in
-- mart_payment_reconciliation instead.

CREATE OR REPLACE TABLE stg_payments AS
WITH ranked AS (
    SELECT *, row_number() OVER (
               PARTITION BY payment_id
               ORDER BY _loaded_at DESC, _file_seq DESC, _row_num DESC) AS _rn,
           count(*) OVER (PARTITION BY payment_id) AS _copies
    FROM raw_payments
),
typed AS (
    SELECT
        nullif(trim(payment_id), '')               AS payment_id,
        nullif(trim(order_id), '')                 AS order_id,
        TRY_CAST(payment_ts AS TIMESTAMP)          AS payment_ts,
        canon_payment_mode(payment_mode)           AS payment_mode,
        round(TRY_CAST(amount_paid AS DOUBLE), 2)  AS amount_paid,
        upper(trim(payment_status))                AS payment_status,
        nullif(trim(gateway_ref), '')              AS gateway_ref,
        _copies, _source_file, _run_id, _loaded_at
    FROM ranked WHERE _rn = 1
),
checked AS (
    SELECT t.*,
        concat_ws('; ',
            CASE WHEN t.payment_id IS NULL THEN 'Critical:payment_id_present' END,
            CASE WHEN t.payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED')
                 THEN 'Critical:payment_status_known' END,
            CASE WHEN t.amount_paid IS NULL OR t.amount_paid <= 0
                 THEN 'Critical:amount_paid_positive' END,
            CASE WHEN t.payment_ts > CAST('@now_ts@' AS TIMESTAMP)
                 THEN 'Critical:payment_ts_not_future' END
        ) AS critical_reason,
        concat_ws('; ',
            CASE WHEN t._copies > 1
                 THEN 'Warning:payment_id_had_duplicates' END,
            CASE WHEN t.gateway_ref IS NULL
                 THEN 'Warning:gateway_ref_present' END,
            CASE WHEN t.payment_mode IS NULL
                 THEN 'Warning:payment_mode_mappable' END,
            CASE WHEN count(*) OVER (PARTITION BY t.order_id) > 1
                 THEN 'Warning:one_payment_per_order' END,
            CASE WHEN o.order_id IS NULL
                 THEN 'Warning:order_id_known' END,
            CASE WHEN t.payment_status = 'SUCCESS' AND o.order_amount IS NOT NULL
                      AND abs(t.amount_paid - o.order_amount) > 0.01
                 THEN 'Warning:success_amount_matches_order' END
        ) AS other_reason
    FROM typed t
    LEFT JOIN stg_orders o
           ON o.order_id = t.order_id AND o.dq_status <> 'QUARANTINED'
)
SELECT payment_id, order_id, payment_ts, payment_mode, amount_paid,
       payment_status, gateway_ref,
       CASE WHEN critical_reason <> '' THEN 'QUARANTINED'
            WHEN other_reason <> ''    THEN 'WARNING'
            ELSE 'PASS' END            AS dq_status,
       nullif(concat_ws('; ', nullif(critical_reason, ''),
                              nullif(other_reason, '')), '') AS failure_reason,
       _source_file, _run_id, _loaded_at
FROM checked;
