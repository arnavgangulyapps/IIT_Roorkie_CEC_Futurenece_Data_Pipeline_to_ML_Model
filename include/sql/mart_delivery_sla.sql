-- include/sql/mart_delivery_sla.sql
-- Business question 3: average delivery time per store, and the % of delivered
-- orders inside 15 minutes — the "10-minute promise" KPI the COO tracks.
--
-- Only DELIVERED orders are in scope: a cancelled order has no delivery time,
-- and including them would drag the average toward whatever the nulls do.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.store_id,
    s.store_name,
    s.zone,
    count(*)                                             AS delivered_orders,
    round(avg(o.delivery_minutes), 1)                    AS avg_delivery_minutes,
    round(median(o.delivery_minutes), 1)                 AS median_delivery_minutes,
    min(o.delivery_minutes)                              AS fastest_minutes,
    max(o.delivery_minutes)                              AS slowest_minutes,
    count(*) FILTER (WHERE o.delivery_minutes <= 10)     AS within_10_min,
    count(*) FILTER (WHERE o.delivery_minutes <= 15)     AS within_15_min,
    round(100.0 * count(*) FILTER (WHERE o.delivery_minutes <= 15)
          / nullif(count(*), 0), 1)                      AS pct_within_15_min,
    round(100.0 * count(*) FILTER (WHERE o.delivery_minutes <= 10)
          / nullif(count(*), 0), 1)                      AS pct_within_10_min
FROM stg_orders o
LEFT JOIN stg_stores s ON s.store_id = o.store_id
WHERE o.dq_status <> 'QUARANTINED'
  AND o.order_status = 'DELIVERED'
  AND o.delivery_minutes IS NOT NULL
GROUP BY o.store_id, s.store_name, s.zone
ORDER BY pct_within_15_min DESC;
