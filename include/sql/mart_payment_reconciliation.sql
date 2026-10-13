-- include/sql/mart_payment_reconciliation.sql
-- Business question 4 and acceptance criterion 7: revenue by payment mode, and
-- every order/payment pair that does not reconcile, with a reason.
--
-- A FULL OUTER JOIN, not a LEFT JOIN. A LEFT JOIN from orders would silently
-- drop the 80 payments that reference no surviving order — money that arrived
-- with nothing to match it, which is the single finding finance most needs.
-- "Finance spends half a day each week finding out why" is this table.

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH paid AS (
    SELECT
        order_id,
        round(sum(CASE WHEN payment_status = 'SUCCESS'
                       THEN amount_paid ELSE 0 END), 2)      AS amount_collected,
        round(sum(CASE WHEN payment_status = 'REFUNDED'
                       THEN amount_paid ELSE 0 END), 2)      AS amount_refunded,
        count(*)                                             AS payment_rows,
        count(*) FILTER (WHERE payment_status = 'SUCCESS')   AS success_rows,
        max(payment_id)                                      AS sample_payment_id,
        bool_or(gateway_ref IS NULL)                         AS missing_gateway_ref
    FROM stg_payments
    WHERE dq_status <> 'QUARANTINED'
    GROUP BY order_id
),
ord AS (
    SELECT order_id, order_date, store_id, order_status, order_amount, payment_mode
    FROM stg_orders
    WHERE dq_status <> 'QUARANTINED'
),
joined AS (
SELECT
    coalesce(o.order_id, p.order_id)                          AS order_id,
    o.order_date,
    o.store_id,
    o.order_status,
    o.payment_mode,
    o.order_amount,
    coalesce(p.amount_collected, 0)                           AS amount_collected,
    coalesce(p.amount_refunded, 0)                            AS amount_refunded,
    round(coalesce(o.order_amount, 0) - coalesce(p.amount_collected, 0), 2)
                                                              AS variance,
    p.payment_rows,
    p.sample_payment_id,
    p.missing_gateway_ref,
    CASE
        WHEN o.order_id IS NULL
            THEN 'payment with no matching order'
        WHEN p.order_id IS NULL AND o.order_status = 'CANCELLED'
            THEN 'cancelled order, no payment expected'
        WHEN p.order_id IS NULL
            THEN 'delivered order with no payment recorded'
        WHEN o.order_status = 'RETURNED' AND p.amount_collected > 0
            THEN 'returned order still shows collected cash'
        WHEN p.success_rows > 1
            THEN 'more than one successful payment for one order'
        WHEN abs(coalesce(o.order_amount, 0) - coalesce(p.amount_collected, 0)) > 0.01
            THEN 'amount collected does not equal order amount'
        ELSE 'reconciled'
    END                                                       AS reason
FROM ord o
FULL OUTER JOIN paid p ON p.order_id = o.order_id
)
-- Keep only the exceptions: one row per mismatch, as the client asked. A
-- cancelled order with no payment is correct behaviour, not a mismatch, so it
-- is not an exception.
SELECT * FROM joined
WHERE reason NOT IN ('reconciled', 'cancelled order, no payment expected')
ORDER BY abs(variance) DESC;
