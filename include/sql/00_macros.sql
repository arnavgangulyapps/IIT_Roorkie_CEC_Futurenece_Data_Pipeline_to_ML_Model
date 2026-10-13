-- include/sql/00_macros.sql
-- Shared DuckDB macros for the ELT pipeline.
--
-- The masking macros mirror include/nammamart_dq.py exactly. If the ETL and
-- ELT masks differ, the two pipelines' top-customer tables stop agreeing and
-- the Part B "mart totals must match ETL" check fails for a cosmetic reason.

-- pr***@example.com — keeps the domain so deliverability stays auditable.
CREATE OR REPLACE MACRO mask_email(e) AS
    CASE
        WHEN e IS NULL                    THEN NULL
        WHEN position('@' IN e) = 0       THEN '***'
        ELSE substr(split_part(e, '@', 1), 1, 2) || '***@' || split_part(e, '@', 2)
    END;

-- 98******21 — first two and last two digits of whatever digits are present.
CREATE OR REPLACE MACRO mask_phone(p) AS
    CASE
        WHEN p IS NULL THEN NULL
        WHEN length(regexp_replace(p, '[^0-9]', '', 'g')) < 4
            THEN repeat('*', greatest(length(regexp_replace(p, '[^0-9]', '', 'g')), 1))
        ELSE substr(regexp_replace(p, '[^0-9]', '', 'g'), 1, 2)
             || repeat('*', length(regexp_replace(p, '[^0-9]', '', 'g')) - 4)
             || right(regexp_replace(p, '[^0-9]', '', 'g'), 2)
    END;

-- 'Priya Hegde' -> 'Priya H.'  A single-token name is returned unchanged.
CREATE OR REPLACE MACRO mask_name(n) AS
    CASE
        WHEN n IS NULL              THEN NULL
        WHEN position(' ' IN trim(n)) = 0 THEN trim(n)
        ELSE split_part(trim(n), ' ', 1) || ' '
             || substr(regexp_extract(trim(n), '([^ ]+)$', 1), 1, 1) || '.'
    END;

-- Canonical payment mode. Returns NULL for anything unmappable, so a validity
-- check can catch it rather than it silently becoming a new category.
CREATE OR REPLACE MACRO canon_payment_mode(m) AS
    CASE upper(trim(m))
        WHEN 'UPI'              THEN 'UPI'
        WHEN 'CARD'             THEN 'Card'
        WHEN 'CASH ON DELIVERY' THEN 'Cash on Delivery'
        WHEN 'COD'              THEN 'Cash on Delivery'
        WHEN 'CASHH'            THEN 'Cash on Delivery'
        WHEN 'CASH'             THEN 'Cash on Delivery'
        WHEN 'WALLET'           THEN 'Wallet'
        ELSE NULL
    END;

-- Title-cased loyalty tier: 'BRONZE', 'bronze' and 'Bronze' are one tier.
CREATE OR REPLACE MACRO canon_tier(t) AS
    CASE WHEN t IS NULL THEN NULL
         ELSE upper(substr(trim(t), 1, 1)) || lower(substr(trim(t), 2))
    END;

-- True when a text column is absent for practical purposes.
CREATE OR REPLACE MACRO is_blank(v) AS (v IS NULL OR trim(v) = '');
