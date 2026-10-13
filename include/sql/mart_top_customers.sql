-- include/sql/mart_top_customers.sql
-- Business question 5: top 10 customers by spend with their loyalty tier, and
-- no raw email or phone anywhere in the output.
--
-- The masked columns are taken from stg_customers, which is the only place
-- masking happens. This table cannot leak PII even if someone edits it later,
-- because the unmasked values are not in its inputs at all.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    c.customer_id,
    c.customer_name,
    c.email_masked,
    c.phone_masked,
    c.loyalty_tier,
    c.city,
    c.area,
    c.is_active,
    count(*) FILTER (WHERE o.order_status = 'DELIVERED')        AS delivered_orders,
    count(*) FILTER (WHERE o.order_status = 'RETURNED')         AS returned_orders,
    round(sum(CASE WHEN o.order_status = 'DELIVERED'
                   THEN o.order_amount ELSE 0 END), 2)          AS total_spend,
    round(avg(CASE WHEN o.order_status = 'DELIVERED'
                   THEN o.order_amount END), 2)                 AS avg_order_value,
    min(o.order_date)                                           AS first_order_date,
    max(o.order_date)                                           AS last_order_date
FROM stg_orders o
JOIN stg_customers c
  ON c.customer_id = o.customer_id AND c.dq_status <> 'QUARANTINED'
WHERE o.dq_status <> 'QUARANTINED'
GROUP BY c.customer_id, c.customer_name, c.email_masked, c.phone_masked,
         c.loyalty_tier, c.city, c.area, c.is_active
ORDER BY total_spend DESC
LIMIT 10;
