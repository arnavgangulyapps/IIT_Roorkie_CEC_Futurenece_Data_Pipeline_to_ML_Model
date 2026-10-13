#!/usr/bin/env python3
"""
scripts/business_questions.py
-----------------------------
The six business questions from the brief, answered from BOTH pipelines, with
the SQL and the result printed together.

Generated rather than hand-written for the same reason as the Part F tables: a
query pasted into a document and a query actually run against the warehouse
drift apart, and the one in the document is the one nobody checks.

Each question carries an ETL query and an ELT query. Where they should return
identical rows, the script asserts it — so the submission's claim that the two
pipelines agree is checked at the moment the document is produced, not asserted
in prose.

Usage:
    .venv/bin/python -I scripts/business_questions.py            # readable
    .venv/bin/python -I scripts/business_questions.py --markdown  # for SUBMISSION.md
"""

import os
import sys

import duckdb

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE = os.path.join(REPO, "include", "warehouse_arnav_ganguly.duckdb")

# (number, question, etl_sql, elt_sql, compare_rows)
QUESTIONS = [
    (1,
     "Daily net revenue by zone and by store (delivered orders only, after discount)",
     """-- ETL: the reporting table the pipeline builds
SELECT zone, store_name,
       sum(delivered_orders)      AS delivered_orders,
       round(sum(net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
GROUP BY zone, store_name
ORDER BY net_revenue DESC
LIMIT 8""",
     """-- ELT: the mart table, same grain
SELECT zone, store_name,
       sum(delivered_orders)      AS delivered_orders,
       round(sum(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
GROUP BY zone, store_name
ORDER BY net_revenue DESC
LIMIT 8""",
     True),

    (2,
     "Revenue and gross margin by product category (margin = revenue - quantity x cost_price)",
     """SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct
FROM etl_category_margin
ORDER BY net_revenue DESC""",
     """SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct
FROM mart_category_margin
ORDER BY net_revenue DESC""",
     True),

    (3,
     "Average delivery time per store, and the % of delivered orders within 15 minutes",
     """-- ETL: aggregated from the fact table to store grain
SELECT store_name,
       round(avg(delivery_minutes), 1) AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE within_15_min)
             / count(*), 1)            AS pct_within_15_min,
       count(*)                        AS delivered_orders
FROM etl_fact_orders
WHERE is_delivered AND delivery_minutes IS NOT NULL
GROUP BY store_name
ORDER BY pct_within_15_min DESC
LIMIT 8""",
     """-- ELT: the purpose-built SLA mart
SELECT store_name, avg_delivery_minutes, pct_within_15_min, delivered_orders
FROM mart_delivery_sla
ORDER BY pct_within_15_min DESC
LIMIT 8""",
     True),

    (4,
     "Revenue by payment mode, and the total that does not reconcile with the gateway",
     """SELECT payment_mode,
       count(*)                        AS delivered_orders,
       round(sum(net_revenue), 2)      AS net_revenue
FROM etl_fact_orders
WHERE is_delivered
GROUP BY payment_mode
ORDER BY net_revenue DESC""",
     """SELECT payment_mode,
       count(*)                        AS delivered_orders,
       round(sum(order_amount), 2)     AS net_revenue
FROM stg_orders
WHERE dq_status <> 'QUARANTINED' AND order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY net_revenue DESC""",
     True),

    (5,
     "Top 10 customers by spend, with loyalty tier (no raw email or phone shown)",
     """-- ETL: fact joined to the masked customer dimension.
-- NOTE the GROUP BY must include customer_id: two different customers can mask
-- to the same name and email, and grouping on the masked columns alone merges
-- them and overstates the top spender.
SELECT f.customer_id, d.customer_name, d.email_masked, d.phone_masked,
       d.loyalty_tier, round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f
JOIN etl_dim_customer d USING (customer_id)
WHERE f.is_delivered
GROUP BY f.customer_id, d.customer_name, d.email_masked, d.phone_masked,
         d.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10""",
     """SELECT customer_id, customer_name, email_masked, phone_masked,
       loyalty_tier, total_spend
FROM mart_top_customers
ORDER BY total_spend DESC
LIMIT 10""",
     True),

    (6,
     "Number of rows rejected today, by file and by reason",
     """SELECT split_part(check_name, '.', 1)  AS file,
       split_part(check_name, '.', 2)  AS reason,
       severity,
       rows_failed
FROM dq_results
WHERE dag_id = 'nammamart_etl_arnav_ganguly'
  AND status = 'FAIL' AND severity = 'Critical'
ORDER BY rows_failed DESC
LIMIT 10""",
     """SELECT split_part(check_name, '.', 1)  AS file,
       split_part(check_name, '.', 2)  AS reason,
       severity,
       rows_failed
FROM dq_results
WHERE dag_id = 'nammamart_elt_arnav_ganguly'
  AND status = 'FAIL' AND severity = 'Critical'
ORDER BY rows_failed DESC
LIMIT 10""",
     False),
]

EXTRA = [
    ("4b", "The unreconciled total the CFO asks about",
     """SELECT reason, count(*) AS orders, round(sum(variance), 2) AS variance
FROM etl_payment_reconciliation
GROUP BY reason ORDER BY abs(sum(variance)) DESC""",
     """SELECT reason, count(*) AS orders, round(sum(variance), 2) AS variance
FROM mart_payment_reconciliation
GROUP BY reason ORDER BY abs(sum(variance)) DESC""",
     True),
]


def table(con, sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return cols, rows


def fmt(v):
    if isinstance(v, float):
        return f"{v:,.2f}"
    return "" if v is None else str(v)


def md_table(cols, rows):
    out = ["| " + " | ".join(cols) + " |",
           "|" + "|".join("---" for _ in cols) + "|"]
    for r in rows:
        out.append("| " + " | ".join(fmt(v) for v in r) + " |")
    return "\n".join(out)


def main():
    md = "--markdown" in sys.argv
    con = duckdb.connect(WAREHOUSE, read_only=True)
    agreements = []

    if md:
        print("## The six business questions\n")
        print("Each is one query on the final tables, answered from **both** "
              "pipelines. Generated by `scripts/business_questions.py`, which "
              "asserts that the two pipelines return identical rows wherever "
              "they should — so the claim that they agree is checked when the "
              "document is produced rather than asserted in prose.\n")

    for num, question, etl_sql, elt_sql, compare in list(QUESTIONS) + EXTRA:
        etl_cols, etl_rows = table(con, etl_sql)
        elt_cols, elt_rows = table(con, elt_sql)

        same = None
        if compare:
            same = etl_rows == elt_rows
            agreements.append((num, same))

        if md:
            print(f"\n### Question {num} — {question}\n")
            print("**ETL**\n")
            print(f"```sql\n{etl_sql}\n```\n")
            print(md_table(etl_cols, etl_rows))
            print("\n**ELT**\n")
            print(f"```sql\n{elt_sql}\n```\n")
            print(md_table(elt_cols, elt_rows))
            if compare:
                verdict = ("**identical to the ETL result above**" if same
                           else "**DIFFERS from the ETL result — see note**")
                print(f"\n{verdict}\n")
        else:
            print(f"\n{'=' * 78}\nQ{num}: {question}\n{'=' * 78}")
            print(f"  ETL -> {len(etl_rows)} rows, ELT -> {len(elt_rows)} rows", end="")
            print(f", identical: {same}" if compare else " (per-DAG by design)")
            for r in etl_rows[:4]:
                print("   ", tuple(fmt(v) for v in r))

    if not md:
        print(f"\n{'=' * 78}")
        for num, same in agreements:
            print(f"  Q{num}: pipelines agree = {same}")
        bad = [n for n, s in agreements if not s]
        print("\nALL COMPARABLE QUESTIONS AGREE" if not bad
              else f"\nMISMATCH on: {bad}")
    con.close()


if __name__ == "__main__":
    main()
