-- include/sql/mart_category_margin.sql
-- Business question 2: revenue and gross margin by product category,
-- margin = revenue - quantity x cost_price.

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    count(*) FILTER (WHERE o.order_status = 'DELIVERED')   AS delivered_orders,
    sum(CASE WHEN o.order_status = 'DELIVERED'
             THEN o.quantity ELSE 0 END)                   AS units_sold,
    round(sum(CASE WHEN o.order_status = 'DELIVERED'
                   THEN o.order_amount ELSE 0 END), 2)     AS net_revenue,
    round(sum(CASE WHEN o.order_status = 'DELIVERED'
                   THEN o.quantity * p.cost_price ELSE 0 END), 2)
                                                           AS cost_of_goods,
    round(sum(CASE WHEN o.order_status = 'DELIVERED'
                   THEN o.order_amount - o.quantity * p.cost_price
                   ELSE 0 END), 2)                         AS gross_margin,
    round(100.0 * sum(CASE WHEN o.order_status = 'DELIVERED'
                           THEN o.order_amount - o.quantity * p.cost_price
                           ELSE 0 END)
          / nullif(sum(CASE WHEN o.order_status = 'DELIVERED'
                            THEN o.order_amount ELSE 0 END), 0), 1)
                                                           AS margin_pct
FROM stg_orders o
LEFT JOIN stg_products p ON p.product_id = o.product_id
WHERE o.dq_status <> 'QUARANTINED'
GROUP BY p.category
ORDER BY net_revenue DESC;
