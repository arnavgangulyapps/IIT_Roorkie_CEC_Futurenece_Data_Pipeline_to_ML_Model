-- include/sql/stg_customers.sql
-- Staging layer for the CRM export. This is the PII boundary: email, phone and
-- full_name are masked here and the unmasked values never leave `raw_customers`.
-- Everything downstream of this file reads masked columns only, which is what
-- makes "no unmasked PII in any reporting table" (criterion 8) a property of
-- the pipeline shape rather than a rule someone has to remember.

CREATE OR REPLACE TABLE stg_customers AS
WITH ranked AS (
    SELECT *, row_number() OVER (
               PARTITION BY customer_id
               ORDER BY _loaded_at DESC, _file_seq DESC, _row_num DESC) AS _rn,
           count(*) OVER (PARTITION BY customer_id) AS _copies
    FROM raw_customers
),
typed AS (
    SELECT
        nullif(trim(customer_id), '')              AS customer_id,
        mask_name(full_name)                       AS customer_name,
        mask_email(email)                          AS email_masked,
        mask_phone(phone)                          AS phone_masked,
        trim(city)                                 AS city,
        trim(area)                                 AS area,
        pincode,
        TRY_CAST(signup_date AS DATE)              AS signup_date,
        canon_tier(loyalty_tier)                   AS loyalty_tier,
        upper(trim(is_active)) = 'Y'               AS is_active,
        -- raw values used by the format checks, then discarded: they are never
        -- selected into the output column list below
        email AS raw_email, phone AS raw_phone, city AS raw_city,
        _copies, _source_file, _run_id, _loaded_at
    FROM ranked WHERE _rn = 1
),
checked AS (
    SELECT *,
        concat_ws('; ',
            CASE WHEN customer_id IS NULL THEN 'Critical:customer_id_present' END
        ) AS critical_reason,
        concat_ws('; ',
            CASE WHEN _copies > 1
                 THEN 'Warning:customer_id_had_duplicates' END,
            CASE WHEN signup_date > CAST('@now_ts@' AS DATE)
                 THEN '@master_sev@:signup_date_not_future' END,
            CASE WHEN is_blank(raw_city)
                 THEN 'Warning:city_present' END,
            CASE WHEN NOT regexp_matches(coalesce(raw_email, ''),
                                         '^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$')
                 THEN 'Warning:email_format' END,
            CASE WHEN NOT regexp_matches(coalesce(raw_phone, ''), '^[0-9]{10}$')
                 THEN 'Warning:phone_10_digits' END,
            CASE WHEN NOT regexp_matches(coalesce(pincode, ''), '^[0-9]{6}$')
                 THEN 'Warning:pincode_6_digits' END,
            CASE WHEN loyalty_tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum')
                 THEN 'Warning:loyalty_tier_known' END
        ) AS other_reason
    FROM typed
)
SELECT customer_id, customer_name, email_masked, phone_masked, city, area,
       pincode, signup_date, loyalty_tier, is_active,
       CASE WHEN critical_reason <> ''           THEN 'QUARANTINED'
            WHEN other_reason LIKE '%Critical:%' THEN 'QUARANTINED'
            WHEN other_reason <> ''              THEN 'WARNING'
            ELSE 'PASS' END                      AS dq_status,
       nullif(concat_ws('; ', nullif(critical_reason, ''),
                              nullif(other_reason, '')), '') AS failure_reason,
       _source_file, _run_id, _loaded_at
FROM checked;
