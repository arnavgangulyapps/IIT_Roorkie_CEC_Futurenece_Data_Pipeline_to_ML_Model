"""
include/profile_nammamart.py
----------------------------
Part 0 — discovery and data profiling for the NammaMart assignment.

Reads every CSV as all-VARCHAR so nothing is silently coerced or dropped:
a type-sniffing reader turns 'Pending??' and '-3' into NULL and the evidence
disappears before you ever see it.

Run (no Airflow, no Docker needed):
    .venv/bin/python -I include/profile_nammamart.py
    .venv/bin/python -I include/profile_nammamart.py --markdown > profile.md
"""

import sys
import duckdb

# The pristine dataset as delivered, not include/data/nammamart — that is the
# mutable landing zone the pipelines read and the Part F experiments rename
# files in. Part 0 profiles the source exports, so it must not depend on
# whatever state an experiment left the landing zone in.
DATA_DIR = "assignment/nammamart_dataset"

FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "orders_inc": "orders_2026-10-01_increment.csv",
}

# Expected row counts from the client brief. If a read drifts from these,
# the CSV or the reader changed and every number below is suspect.
EXPECTED_ROWS = {
    "orders": 2412, "payments": 2228, "customers": 405,
    "products": 152, "stores": 15, "orders_inc": 160,
}

ALLOWED_STATUS = "('DELIVERED','CANCELLED','RETURNED')"
ALLOWED_PAY_MODE = "('UPI','Card','Cash on Delivery','Wallet')"
RUN_TS = "'2026-10-13'"  # assignment "today"; the DAGs use the real run date

# name, file, column, dimension, severity, proposed rule, SQL returning bad rows
CHECKS = [
    # ---- orders -----------------------------------------------------------
    ("orders_customer_id_null", "orders", "customer_id", "Completeness", "Critical",
     "Reject: an order with no customer cannot be attributed or reconciled",
     "SELECT order_id FROM orders WHERE customer_id IS NULL OR trim(customer_id)=''"),
    ("orders_order_ts_null", "orders", "order_ts", "Completeness", "Critical",
     "Reject: without a timestamp the row cannot be assigned to a report day",
     "SELECT order_id FROM orders WHERE order_ts IS NULL OR trim(order_ts)=''"),
    ("orders_delivered_no_delivery_minutes", "orders", "delivery_minutes", "Completeness", "Critical",
     "Reject: a DELIVERED order must carry a delivery time (SLA KPI depends on it)",
     """SELECT order_id FROM orders WHERE upper(trim(order_status))='DELIVERED'
        AND (delivery_minutes IS NULL OR trim(delivery_minutes)='')"""),
    ("orders_order_id_duplicate", "orders", "order_id", "Uniqueness", "Critical",
     "Keep the last row per order_id; duplicates double-count revenue",
     """SELECT order_id FROM orders WHERE order_id IN
        (SELECT order_id FROM orders GROUP BY order_id HAVING count(*)>1)"""),
    ("orders_status_not_allowed", "orders", "order_status", "Validity", "Critical",
     f"Reject: order_status must be one of {ALLOWED_STATUS} after upper-casing",
     f"SELECT order_id FROM orders WHERE upper(trim(order_status)) NOT IN {ALLOWED_STATUS}"),
    ("orders_payment_mode_not_allowed", "orders", "payment_mode", "Validity", "Warning",
     f"Standardise case/whitespace to {ALLOWED_PAY_MODE}; reject only unmappable values",
     f"SELECT order_id FROM orders WHERE trim(payment_mode) NOT IN {ALLOWED_PAY_MODE}"),
    ("orders_order_ts_unparseable", "orders", "order_ts", "Validity", "Critical",
     "Reject: order_ts must parse as a timestamp",
     """SELECT order_id FROM orders WHERE trim(coalesce(order_ts,''))<>''
        AND TRY_CAST(order_ts AS TIMESTAMP) IS NULL"""),
    ("orders_discount_pct_out_of_range", "orders", "discount_pct", "Validity", "Critical",
     "Reject: discount_pct must be between 0 and 100",
     """SELECT order_id FROM orders
        WHERE TRY_CAST(discount_pct AS DOUBLE) NOT BETWEEN 0 AND 100"""),
    ("orders_quantity_not_positive", "orders", "quantity", "Accuracy", "Critical",
     "Reject: quantity must be > 0 (ask client whether negatives are returns)",
     "SELECT order_id FROM orders WHERE TRY_CAST(quantity AS INT) <= 0"),
    ("orders_amount_formula_mismatch", "orders", "order_amount", "Accuracy", "Critical",
     "Reject: order_amount must equal round(qty*unit_price*(1-discount/100), 2)",
     """SELECT order_id FROM orders
        WHERE TRY_CAST(order_amount AS DOUBLE) IS NOT NULL
          AND TRY_CAST(quantity AS INT) IS NOT NULL
          AND TRY_CAST(unit_price AS DOUBLE) IS NOT NULL
          AND TRY_CAST(discount_pct AS DOUBLE) IS NOT NULL
          AND abs(TRY_CAST(order_amount AS DOUBLE) - round(
                TRY_CAST(quantity AS INT) * TRY_CAST(unit_price AS DOUBLE)
                * (1 - TRY_CAST(discount_pct AS DOUBLE)/100), 2)) > 0.01"""),
    ("orders_delivery_minutes_implausible", "orders", "delivery_minutes", "Accuracy", "Critical",
     "Reject: delivery_minutes >= 120 is an hours/minutes unit error, not a slow delivery",
     "SELECT order_id FROM orders WHERE TRY_CAST(delivery_minutes AS DOUBLE) >= 120"),
    ("orders_unit_price_not_positive", "orders", "unit_price", "Accuracy", "Critical",
     "Reject: unit_price must be > 0",
     "SELECT order_id FROM orders WHERE TRY_CAST(unit_price AS DOUBLE) <= 0"),
    ("orders_customer_id_orphan", "orders", "customer_id", "Consistency", "Critical",
     "Reject: customer_id must exist in customers.csv",
     """SELECT order_id FROM orders o WHERE trim(coalesce(o.customer_id,''))<>''
        AND NOT EXISTS (SELECT 1 FROM customers c WHERE c.customer_id=o.customer_id)"""),
    ("orders_product_id_orphan", "orders", "product_id", "Consistency", "Critical",
     "Reject: product_id must exist in products.csv (no margin without cost_price)",
     """SELECT order_id FROM orders o WHERE trim(coalesce(o.product_id,''))<>''
        AND NOT EXISTS (SELECT 1 FROM products p WHERE p.product_id=o.product_id)"""),
    ("orders_store_id_orphan", "orders", "store_id", "Consistency", "Critical",
     "Reject: store_id must exist in stores.csv (no zone without it)",
     """SELECT order_id FROM orders o WHERE trim(coalesce(o.store_id,''))<>''
        AND NOT EXISTS (SELECT 1 FROM stores s WHERE s.store_id=o.store_id)"""),
    ("orders_order_ts_future", "orders", "order_ts", "Timeliness", "Critical",
     "Reject: order_ts must not be in the future at run time",
     f"SELECT order_id FROM orders WHERE TRY_CAST(order_ts AS TIMESTAMP) > CAST({RUN_TS} AS TIMESTAMP)"),

    # ---- payments ---------------------------------------------------------
    ("payments_gateway_ref_null", "payments", "gateway_ref", "Completeness", "Warning",
     "Load but flag: no gateway_ref means finance cannot trace it at the gateway",
     "SELECT payment_id FROM payments WHERE gateway_ref IS NULL OR trim(gateway_ref)=''"),
    ("payments_payment_id_duplicate", "payments", "payment_id", "Uniqueness", "Critical",
     "Keep one row per payment_id; duplicates inflate collected cash",
     """SELECT payment_id FROM payments WHERE payment_id IN
        (SELECT payment_id FROM payments GROUP BY payment_id HAVING count(*)>1)"""),
    ("payments_order_id_duplicate", "payments", "order_id", "Uniqueness", "Warning",
     "Investigate: >1 payment per order is a retry or a double charge",
     """SELECT payment_id FROM payments WHERE order_id IN
        (SELECT order_id FROM payments GROUP BY order_id HAVING count(*)>1)"""),
    ("payments_mode_not_allowed", "payments", "payment_mode", "Validity", "Warning",
     f"Standardise case/whitespace to {ALLOWED_PAY_MODE}",
     f"SELECT payment_id FROM payments WHERE trim(payment_mode) NOT IN {ALLOWED_PAY_MODE}"),
    ("payments_amount_not_positive", "payments", "amount_paid", "Accuracy", "Critical",
     "Reject: amount_paid must be > 0",
     "SELECT payment_id FROM payments WHERE TRY_CAST(amount_paid AS DOUBLE) <= 0"),
    ("payments_order_id_orphan", "payments", "order_id", "Consistency", "Critical",
     "Reject: a payment must reference a known order",
     """SELECT payment_id FROM payments p
        WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.order_id=p.order_id)"""),
    ("payments_success_amount_mismatch", "payments", "amount_paid", "Consistency", "Critical",
     "Route to reconciliation table: a SUCCESS payment must equal its order_amount",
     """SELECT p.payment_id FROM payments p JOIN orders o ON o.order_id=p.order_id
        WHERE upper(trim(p.payment_status))='SUCCESS'
          AND abs(TRY_CAST(p.amount_paid AS DOUBLE)
                  - TRY_CAST(o.order_amount AS DOUBLE)) > 0.01"""),
    ("payments_mode_disagrees_with_order", "payments", "payment_mode", "Consistency", "Warning",
     "Flag: payment_mode on the payment should match the order",
     """SELECT p.payment_id FROM payments p JOIN orders o ON o.order_id=p.order_id
        WHERE upper(replace(trim(p.payment_mode),' ','')) <> upper(replace(trim(o.payment_mode),' ',''))"""),
    ("payments_ts_future", "payments", "payment_ts", "Timeliness", "Critical",
     "Reject: payment_ts must not be in the future",
     f"SELECT payment_id FROM payments WHERE TRY_CAST(payment_ts AS TIMESTAMP) > CAST({RUN_TS} AS TIMESTAMP)"),

    # ---- customers --------------------------------------------------------
    ("customers_city_null", "customers", "city", "Completeness", "Warning",
     "Load but flag: city is blank; default to Bengaluru only with client sign-off",
     "SELECT customer_id FROM customers WHERE city IS NULL OR trim(city)=''"),
    ("customers_customer_id_duplicate", "customers", "customer_id", "Uniqueness", "Critical",
     "Dedupe to one row per customer_id (latest signup_date wins)",
     """SELECT customer_id FROM customers WHERE customer_id IN
        (SELECT customer_id FROM customers GROUP BY customer_id HAVING count(*)>1)"""),
    ("customers_email_invalid", "customers", "email", "Validity", "Warning",
     "Load but flag: email must look like local@domain.tld",
     """SELECT customer_id FROM customers
        WHERE NOT regexp_matches(coalesce(email,''), '^[^@\\s]+@[^@\\s]+\\.[A-Za-z]{2,}$')"""),
    ("customers_phone_not_10_digits", "customers", "phone", "Validity", "Warning",
     "Load but flag: Indian mobile must be exactly 10 digits",
     """SELECT customer_id FROM customers
        WHERE NOT regexp_matches(coalesce(phone,''), '^[0-9]{10}$')"""),
    ("customers_pincode_not_6_digits", "customers", "pincode", "Validity", "Warning",
     "Load but flag: Indian pincode must be exactly 6 digits",
     """SELECT customer_id FROM customers
        WHERE NOT regexp_matches(coalesce(pincode,''), '^[0-9]{6}$')"""),
    ("customers_loyalty_tier_casing", "customers", "loyalty_tier", "Consistency", "Warning",
     "Standardise to title case; do not reject",
     """SELECT customer_id FROM customers
        WHERE loyalty_tier NOT IN ('Bronze','Silver','Gold','Platinum')"""),
    ("customers_signup_date_future", "customers", "signup_date", "Timeliness", "Critical",
     "Reject: signup_date must not be in the future",
     f"SELECT customer_id FROM customers WHERE TRY_CAST(signup_date AS DATE) > CAST({RUN_TS} AS DATE)"),

    # ---- products ---------------------------------------------------------
    ("products_category_null", "products", "category", "Completeness", "Critical",
     "Reject: category drives the margin-by-category report",
     "SELECT product_id FROM products WHERE category IS NULL OR trim(category)=''"),
    ("products_product_id_duplicate", "products", "product_id", "Uniqueness", "Critical",
     "Dedupe to one row per product_id",
     """SELECT product_id FROM products WHERE product_id IN
        (SELECT product_id FROM products GROUP BY product_id HAVING count(*)>1)"""),
    ("products_mrp_not_positive", "products", "mrp", "Accuracy", "Critical",
     "Reject: mrp must be > 0",
     "SELECT product_id FROM products WHERE TRY_CAST(mrp AS DOUBLE) <= 0"),
    ("products_cost_exceeds_mrp", "products", "cost_price", "Accuracy", "Critical",
     "Reject: cost_price must be <= mrp or margin goes negative on a healthy product",
     """SELECT product_id FROM products
        WHERE TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE)"""),
    ("products_launch_date_future", "products", "launch_date", "Timeliness", "Warning",
     "Flag: a future launch_date means the product should not have orders yet",
     f"SELECT product_id FROM products WHERE TRY_CAST(launch_date AS DATE) > CAST({RUN_TS} AS DATE)"),

    # ---- stores -----------------------------------------------------------
    ("stores_zone_null", "stores", "zone", "Completeness", "Critical",
     "Reject: zone drives the revenue-by-zone report the COO reads",
     "SELECT store_id FROM stores WHERE zone IS NULL OR trim(zone)=''"),
    ("stores_store_id_duplicate", "stores", "store_id", "Uniqueness", "Critical",
     "Dedupe to one row per store_id",
     """SELECT store_id FROM stores WHERE store_id IN
        (SELECT store_id FROM stores GROUP BY store_id HAVING count(*)>1)"""),
    ("stores_pincode_not_6_digits", "stores", "pincode", "Validity", "Warning",
     "Flag: pincode must be exactly 6 digits",
     """SELECT store_id FROM stores
        WHERE NOT regexp_matches(coalesce(pincode,''), '^[0-9]{6}$')"""),
    ("stores_capacity_not_positive", "stores", "capacity_orders_per_day", "Accuracy", "Critical",
     "Reject: capacity must be > 0",
     "SELECT store_id FROM stores WHERE TRY_CAST(capacity_orders_per_day AS INT) <= 0"),

    # ---- increment file ---------------------------------------------------
    ("orders_inc_status_not_allowed", "orders_inc", "order_status", "Validity", "Critical",
     f"Reject: order_status must be one of {ALLOWED_STATUS}",
     f"SELECT order_id FROM orders_inc WHERE upper(trim(order_status)) NOT IN {ALLOWED_STATUS}"),
    ("orders_inc_corrections_to_september", "orders_inc", "order_id", "Consistency", "Warning",
     "MERGE on order_id: these are restatements of existing orders, not new ones",
     """SELECT i.order_id FROM orders_inc i
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.order_id=i.order_id)"""),
    ("orders_inc_order_id_duplicate", "orders_inc", "order_id", "Uniqueness", "Critical",
     "Keep the last row per order_id within the increment batch",
     """SELECT order_id FROM orders_inc WHERE order_id IN
        (SELECT order_id FROM orders_inc GROUP BY order_id HAVING count(*)>1)"""),
]


def connect():
    con = duckdb.connect()
    for view, fname in FILES.items():
        con.execute(
            f"CREATE VIEW {view} AS SELECT * FROM "
            f"read_csv('{DATA_DIR}/{fname}', all_varchar=true, header=true)"
        )
        n = con.execute(f"SELECT count(*) FROM {view}").fetchone()[0]
        assert n == EXPECTED_ROWS[view], f"{fname}: read {n} rows, brief says {EXPECTED_ROWS[view]}"
    return con


def run_checks(con):
    rows = []
    for name, f, col, dim, sev, rule, sql in CHECKS:
        keys = [r[0] for r in con.execute(sql).fetchall()]
        rows.append({
            "check": name, "file": FILES[f], "column": col, "dimension": dim,
            "severity": sev, "rows_checked": con.execute(
                f"SELECT count(*) FROM {f}").fetchone()[0],
            "rows_failed": len(keys),
            "examples": ", ".join(sorted(set(keys))[:3]),
            "rule": rule,
        })
    return rows


def summarize(con):
    """Min/max/nulls/cardinality for every column of every file (Part 0 item 1).

    Two passes on purpose. The all-VARCHAR view gives true null counts and
    distinct counts for the data as it actually arrived. A second, type-sniffed
    read gives meaningful min/max for numeric and date columns — on the VARCHAR
    view those would be lexicographic, which is correct for ISO dates and wrong
    for numbers ('9' > '100').
    """
    for view, fname in FILES.items():
        print(f"\n{'=' * 100}\n{fname}\n{'=' * 100}")
        typed = f"read_csv('{DATA_DIR}/{fname}', header=true)"
        rows = con.execute(
            f"SELECT column_name, column_type, min, max, approx_unique, "
            f"null_percentage FROM (SUMMARIZE SELECT * FROM {typed})").fetchall()
        print(f"{'column':26s} {'sniffed type':12s} {'min':>22s} {'max':>22s} "
              f"{'uniq':>7s} {'null%':>7s}")
        print("-" * 100)
        for name, ctype, lo, hi, uniq, nullpct in rows:
            lo = "" if lo is None else str(lo)[:22]
            hi = "" if hi is None else str(hi)[:22]
            flag = "  <--" if nullpct and float(str(nullpct).rstrip('%')) > 0 else ""
            print(f"{name:26s} {str(ctype)[:12]:12s} {lo:>22s} {hi:>22s} "
                  f"{uniq:>7} {str(nullpct):>7}{flag}")


def main():
    con = connect()
    if "--summarize" in sys.argv:
        summarize(con)
        return
    rows = run_checks(con)
    md = "--markdown" in sys.argv

    if md:
        print("## Part 0 — Data Issues Log\n")
        print("| File | Row key (examples) | Column | Problem | Dimension | Severity | Rows failed | Proposed rule |")
        print("|---|---|---|---|---|---|---|---|")
        for r in rows:
            if r["rows_failed"] == 0:
                continue
            print(f"| `{r['file']}` | {r['examples'] or '—'} | `{r['column']}` | "
                  f"{r['check']} | {r['dimension']} | {r['severity']} | "
                  f"{r['rows_failed']} | {r['rule']} |")
    else:
        hdr = f"{'check':42s} {'dim':14s} {'sev':9s} {'failed':>7s}  examples"
        print(hdr)
        print("-" * len(hdr))
        for r in rows:
            mark = " " if r["rows_failed"] == 0 else "!"
            print(f"{mark}{r['check']:41s} {r['dimension']:14s} {r['severity']:9s} "
                  f"{r['rows_failed']:7d}  {r['examples']}")

    failed = [r for r in rows if r["rows_failed"]]
    files_hit = {r["file"] for r in failed}
    crit = sum(r["rows_failed"] for r in failed if r["severity"] == "Critical")
    print(f"\n{len(failed)}/{len(rows)} checks found issues, across {len(files_hit)} files. "
          f"{crit} Critical row-failures total.", file=sys.stderr)


if __name__ == "__main__":
    main()
