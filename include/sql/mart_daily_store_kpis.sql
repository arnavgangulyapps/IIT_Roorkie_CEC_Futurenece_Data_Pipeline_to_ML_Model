-- include/sql/mart_daily_store_kpis.sql
-- Business question 1: daily net revenue by zone and by store, delivered
-- orders only, after discount.
--
-- `WHERE dq_status <> 'QUARANTINED'` is the ELT equivalent of the ETL's
-- quarantine step. The difference that matters for Part E.3 (lineage): the bad
-- rows are still sitting in stg_orders and raw_orders, so "why isn't this
-- order in the total?" is one SELECT away instead of a hunt through CSV files.

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.zone,
    o.store_id,
    s.store_name,
    count(*)                                                   AS orders_total,
    count(*) FILTER (WHERE o.order_status = 'DELIVERED')        AS delivered_orders,
    count(*) FILTER (WHERE o.order_status = 'CANCELLED')        AS cancelled_orders,
    count(*) FILTER (WHERE o.order_status = 'RETURNED')         AS returned_orders,
    round(sum(CASE WHEN o.order_status = 'DELIVERED'
                   THEN o.order_amount ELSE 0 END), 2)          AS net_revenue,
    round(sum(CASE WHEN o.order_status = 'DELIVERED'
                   THEN o.order_amount - o.quantity * p.cost_price
                   ELSE 0 END), 2)                              AS gross_margin,
    round(avg(o.delivery_minutes)
          FILTER (WHERE o.order_status = 'DELIVERED'), 1)       AS avg_delivery_minutes,
    round(100.0 * count(*) FILTER (
              WHERE o.order_status = 'DELIVERED' AND o.delivery_minutes <= 15)
          / nullif(count(*) FILTER (WHERE o.order_status = 'DELIVERED'), 0), 1)
                                                                AS pct_within_15_min,
    -- Bonus: daily orders against the store's rated capacity.
    max(s.capacity_orders_per_day)                              AS capacity_orders_per_day,
    round(100.0 * count(*) / nullif(max(s.capacity_orders_per_day), 0), 1)
                                                                AS pct_of_capacity
FROM stg_orders o
LEFT JOIN stg_stores   s ON s.store_id   = o.store_id
LEFT JOIN stg_products p ON p.product_id = o.product_id
WHERE o.dq_status <> 'QUARANTINED'
GROUP BY o.order_date, s.zone, o.store_id, s.store_name
ORDER BY o.order_date, s.zone, o.store_id;
