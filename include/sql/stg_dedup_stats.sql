-- include/sql/stg_dedup_stats.sql
-- How many copies each dataset's key had in the most recent load.
--
-- Staging keeps one row per key (latest copy wins) and annotates that survivor
-- with a Warning. The *rejected* copies are therefore not in stg_*, so the
-- Uniqueness check's rows_failed cannot be counted from there. This table
-- counts them once, from the raw layer, for dq_results to read.
--
-- Scoped to the latest `_loaded_at` because raw is append-only: without the
-- filter, a second run would report every earlier run's rows as duplicates.

CREATE OR REPLACE TABLE stg_dedup_stats AS
          SELECT 'orders'    AS dataset,
                 count(*) - count(DISTINCT order_id)    AS superseded,
                 count(*)                               AS rows_in_load
          FROM raw_orders
          WHERE _loaded_at = (SELECT max(_loaded_at) FROM raw_orders)
UNION ALL SELECT 'payments',
                 count(*) - count(DISTINCT payment_id),
                 count(*)
          FROM raw_payments
          WHERE _loaded_at = (SELECT max(_loaded_at) FROM raw_payments)
UNION ALL SELECT 'customers',
                 count(*) - count(DISTINCT customer_id),
                 count(*)
          FROM raw_customers
          WHERE _loaded_at = (SELECT max(_loaded_at) FROM raw_customers)
UNION ALL SELECT 'products',
                 count(*) - count(DISTINCT product_id),
                 count(*)
          FROM raw_products
          WHERE _loaded_at = (SELECT max(_loaded_at) FROM raw_products)
UNION ALL SELECT 'stores',
                 count(*) - count(DISTINCT store_id),
                 count(*)
          FROM raw_stores
          WHERE _loaded_at = (SELECT max(_loaded_at) FROM raw_stores);
