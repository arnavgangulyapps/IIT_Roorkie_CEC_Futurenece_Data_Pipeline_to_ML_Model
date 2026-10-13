"""
dags/nammamart_etl_arnav_ganguly.py
-----------------------------------
NammaMart ETL pipeline — Extract -> Validate -> Transform -> Load.

The finance-first pipeline: a row that fails a Critical check is quarantined
with its reason and never reaches a reporting table (acceptance criterion 2).

Idempotency (criterion 5) and the incremental feed (criterion 6) share one
mechanism: the fact load deletes the batch's own `order_id`s and re-inserts
them. Re-running restates the same rows; the increment file's 10 status
corrections overwrite their September originals instead of duplicating them.

The increment feed is picked up by globbing `orders_*_increment.csv` in the
data directory, the way a real landing zone works — rather than a DAG param a
production pipeline would not have. To run the happy-path baseline without it
(Part F experiment 1), move the file out of the data directory and trigger.
"""

import sys

sys.path.insert(0, "/usr/local/airflow/include")

import glob
import os
from datetime import timedelta

import duckdb
import pandas as pd
import pendulum
from airflow.decorators import dag, task

from nammamart_dq import (
    AMOUNT_TOLERANCE,
    CRITICAL,
    POLICY_VAR,
    LOYALTY_TIERS,
    MAX_DELIVERY_MINUTES,
    ORDER_STATUSES,
    PAYMENT_MODES,
    SLA_MINUTES,
    WARNING,
    mask_email,
    mask_name,
    mask_phone,
    master_content_severity,
    quarantine_to_csv,
    read_policy,
    run_checks,
    standardise_payment_mode,
    standardise_status,
    standardise_tier,
    write_dq_results,
    write_pipeline_run,
)

OWNER = "arnav_ganguly"
AIRFLOW_HOME = "/usr/local/airflow"
DATA_DIR = f"{AIRFLOW_HOME}/include/data/nammamart"
WAREHOUSE = f"{AIRFLOW_HOME}/include/warehouse_{OWNER}.duckdb"
QUARANTINE_DIR = f"{AIRFLOW_HOME}/include/quarantine/{OWNER}"
STAGING_DIR = f"{AIRFLOW_HOME}/include/staging/{OWNER}"

DAG_ID = f"nammamart_etl_{OWNER}"

default_args = {
    "owner": OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=20),
}


# ── small helpers ─────────────────────────────────────────────────────────────

def _num(s: pd.Series) -> pd.Series:
    """Numeric or NaN — the pandas equivalent of TRY_CAST(... AS DOUBLE)."""
    return pd.to_numeric(s, errors="coerce")


def _ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", format="mixed")


def _blank(s: pd.Series) -> pd.Series:
    """True where the value is null or whitespace-only."""
    return s.isna() | (s.astype("string").str.strip() == "")


def _read_csv_as_text(path: str) -> pd.DataFrame:
    """Everything as text, on purpose. A type-sniffing read turns 'Pending??'
    into NaN and '-3' into a valid int, destroying the evidence before any
    check can see it."""
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])


def _staging_path(run_id: str, dataset: str) -> str:
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in str(run_id))
    directory = os.path.join(STAGING_DIR, safe)
    os.makedirs(directory, exist_ok=True)
    return os.path.join(directory, f"{dataset}.parquet")


@dag(
    dag_id=DAG_ID,
    # 9:00 AM IST, Monday to Saturday. Interpreted in the DAG's timezone,
    # which the tz-aware start_date below pins to Asia/Kolkata.
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    # The client wants this morning's number, not a backfill of every morning
    # since the start_date. Catchup would fire ~12 runs on first unpause and
    # each would load the same September CSVs.
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fde", "nammamart", "etl", OWNER],
    doc_md=__doc__,
)
def nammamart_etl():

    # ── EXTRACT — five parallel tasks, one per source file ────────────────────

    def _extract(dataset: str, filename: str):
        @task(task_id=f"extract_{dataset}")
        def _inner(**context) -> dict:
            path = f"{DATA_DIR}/{filename}"
            if not os.path.exists(path):
                # Fail loudly and name the file. A missing master silently
                # treated as empty would quarantine every order as an orphan.
                raise FileNotFoundError(f"source file missing: {path}")
            df = _read_csv_as_text(path)
            out = _staging_path(context["run_id"], dataset)
            df.to_parquet(out, index=False)
            print(f"[extract] {dataset}: {len(df)} rows from {filename}")
            return {"dataset": dataset, "path": out, "rows": len(df),
                    "source_file": filename}
        return _inner()

    @task
    def extract_orders(**context) -> dict:
        """orders.csv plus any increment files that have landed."""
        base = f"{DATA_DIR}/orders.csv"
        if not os.path.exists(base):
            raise FileNotFoundError(f"source file missing: {base}")

        frames, sources = [], []
        for path in [base] + sorted(glob.glob(f"{DATA_DIR}/orders_*_increment.csv")):
            part = _read_csv_as_text(path)
            part["_source_file"] = os.path.basename(path)
            frames.append(part)
            sources.append(f"{os.path.basename(path)}({len(part)})")

        df = pd.concat(frames, ignore_index=True)
        out = _staging_path(context["run_id"], "orders")
        df.to_parquet(out, index=False)
        print(f"[extract] orders: {len(df)} rows from {', '.join(sources)}")
        return {"dataset": "orders", "path": out, "rows": len(df),
                "source_file": ",".join(s.split("(")[0] for s in sources)}

    # ── VALIDATE — one task, because referential integrity is cross-file ──────

    # pool="duckdb_writer" on every task that opens the warehouse.
    #
    # The ELT needed this because its five raw loads fan out (see that DAG).
    # The ETL needs it for a different and less obvious reason: BOTH DAGs are
    # scheduled at `0 9 * * 1-6`, so at 09:00 IST they run concurrently and
    # both write the same single-writer DuckDB file. max_active_runs=1 is
    # per-DAG and does nothing across DAGs.
    #
    # Airflow pools ARE global, so putting both DAGs' writers in one 1-slot
    # pool serialises warehouse access across the pair. Without it the 9 AM
    # collision is a coin flip — the runs I triggered together happened to
    # succeed, which is exactly how this kind of bug reaches production.
    #
    # The extracts stay out of the pool: they write parquet, not the warehouse,
    # so they keep running five-wide and the DAG keeps its parallel extract.
    @task(pool="duckdb_writer")
    def validate(orders_in: dict, payments_in: dict, customers_in: dict,
                 products_in: dict, stores_in: dict, **context) -> dict:
        """Apply every quality rule, split passed/quarantined, write the
        quarantine files and the dq_results scorecard.

        This is one task rather than five because half the rules are
        consistency rules that need another file to evaluate: an order's
        store_id can only be judged against the stores master.
        """
        run_id = context["run_id"]
        policy = read_policy()
        master_sev = master_content_severity(policy)
        print(f"[validate] master-data DQ policy: {policy} "
              f"(content defects are {master_sev}) — set the Airflow Variable "
              f"{POLICY_VAR} to switch")
        now = pendulum.now("Asia/Kolkata")
        now_naive = pd.Timestamp(now.naive())

        orders = pd.read_parquet(orders_in["path"])
        payments = pd.read_parquet(payments_in["path"])
        customers = pd.read_parquet(customers_in["path"])
        products = pd.read_parquet(products_in["path"])
        stores = pd.read_parquet(stores_in["path"])

        # Masters are validated first; orders are then checked against the
        # rows that survived, so an order is never attached to a bad master row.
        #
        # SEVERITY POLICY ON MASTER DATA — a deliberate deviation from the
        # severities the brief *suggests*, justified by measurement:
        #
        # Rejecting a master row does not drop one row, it drops every order
        # that points at it. Measured end to end by running both pipelines
        # under both policies (scripts/policy_comparison.py):
        #
        #                          lenient        strict         delta
        #     ETL fact rows          2,310         1,897          -413
        #     delivered orders       2,088         1,718          -370
        #     net revenue     1,135,628.80    931,157.95  -204,470.85
        #     gross margin      260,999.43    227,955.56   -33,043.87
        #     zones in report            5             4            -1
        #     categories                 7             6            -1
        #
        #     24 defective master rows cost Rs 204,470.85 — 18.0% of revenue.
        #
        # (An earlier per-defect estimate said Rs 248,156. That was too high:
        # summing revenue per defect double-counts orders that point at more
        # than one bad master row. The measured figure is authoritative.)
        #
        # So: only defects that make a master row unusable *as a dimension key*
        # (null or duplicate key) are Critical. Content defects are Warning and
        # repaired to a visible sentinel in transform(). Criterion 2 says no bad
        # *order* may reach finance; an order is not bad because its store's
        # zone cell is empty, and silently deleting a fifth of the revenue is a
        # worse quality failure than showing zone='UNKNOWN' and flagging it.
        store_checks = [
            ("store_id_present", "Completeness", CRITICAL,
             lambda d: _blank(d["store_id"])),
            ("zone_present", "Completeness", master_sev, lambda d: _blank(d["zone"])),
            ("store_id_unique", "Uniqueness", CRITICAL,
             lambda d: d["store_id"].duplicated(keep="last")),
            ("pincode_6_digits", "Validity", WARNING,
             lambda d: ~d["pincode"].astype("string").str.fullmatch(r"\d{6}").fillna(False)),
            ("capacity_positive", "Accuracy", master_sev,
             lambda d: ~(_num(d["capacity_orders_per_day"]) > 0)),
            ("open_date_not_future", "Timeliness", CRITICAL,
             lambda d: _ts(d["open_date"]) > now_naive),
            ("zone_in_known_set", "Consistency", WARNING,
             lambda d: ~d["zone"].astype("string").str.strip().isin(
                 ["North", "South", "East", "West"])),
        ]
        stores_ok, stores_bad, dq = run_checks(
            stores, store_checks, dataset="stores", run_id=run_id, dag_id=DAG_ID)

        product_checks = [
            ("product_id_present", "Completeness", CRITICAL,
             lambda d: _blank(d["product_id"])),
            # Warning, not Critical: see the severity policy above. A blank
            # category costs the margin report 48 orders if rejected; it is
            # repaired to 'UNCATEGORISED' and flagged instead.
            ("category_present", "Completeness", master_sev,
             lambda d: _blank(d["category"])),
            ("product_id_unique", "Uniqueness", CRITICAL,
             lambda d: d["product_id"].duplicated(keep="last")),
            ("unit_in_allowed_set", "Validity", WARNING,
             lambda d: ~d["unit"].astype("string").str.strip().isin(["kg", "pack", "unit"])),
            ("mrp_positive", "Accuracy", master_sev, lambda d: ~(_num(d["mrp"]) > 0)),
            ("cost_not_above_mrp", "Accuracy", master_sev,
             lambda d: _num(d["cost_price"]) > _num(d["mrp"])),
            ("cost_price_present", "Consistency", WARNING,
             lambda d: _num(d["cost_price"]).isna()),
            ("launch_date_not_future", "Timeliness", WARNING,
             lambda d: _ts(d["launch_date"]) > now_naive),
        ]
        products_ok, products_bad, rows = run_checks(
            products, product_checks, dataset="products", run_id=run_id, dag_id=DAG_ID)
        dq += rows

        customer_checks = [
            ("customer_id_present", "Completeness", CRITICAL,
             lambda d: _blank(d["customer_id"])),
            ("city_present", "Completeness", WARNING, lambda d: _blank(d["city"])),
            ("customer_id_unique", "Uniqueness", CRITICAL,
             lambda d: d["customer_id"].duplicated(keep="last")),
            ("email_format", "Validity", WARNING,
             lambda d: ~d["email"].astype("string").str.fullmatch(
                 r"[^@\s]+@[^@\s]+\.[A-Za-z]{2,}").fillna(False)),
            ("phone_10_digits", "Validity", WARNING,
             lambda d: ~d["phone"].astype("string").str.fullmatch(r"\d{10}").fillna(False)),
            ("pincode_6_digits", "Validity", WARNING,
             lambda d: ~d["pincode"].astype("string").str.fullmatch(r"\d{6}").fillna(False)),
            ("loyalty_tier_known", "Consistency", WARNING,
             lambda d: ~standardise_tier(d["loyalty_tier"]).isin(LOYALTY_TIERS)),
            # Warning: a future signup_date is a CRM defect, but the customer
            # has 18 real orders worth Rs 10,782. Dropping the customer to
            # punish the date drops the revenue with it.
            ("signup_date_not_future", "Timeliness", master_sev,
             lambda d: _ts(d["signup_date"]) > now_naive),
        ]
        customers_ok, customers_bad, rows = run_checks(
            customers, customer_checks, dataset="customers", run_id=run_id, dag_id=DAG_ID)
        dq += rows

        good_stores = set(stores_ok["store_id"])
        good_products = set(products_ok["product_id"])
        good_customers = set(customers_ok["customer_id"])

        delivered = standardise_status(orders["order_status"]) == "DELIVERED"
        order_checks = [
            ("customer_id_present", "Completeness", CRITICAL,
             lambda d: _blank(d["customer_id"])),
            ("order_ts_present", "Completeness", CRITICAL,
             lambda d: _blank(d["order_ts"])),
            # Only DELIVERED needs a delivery time; a blank on CANCELLED is correct
            # and quarantining it would throw away 156 perfectly good rows.
            ("delivered_has_delivery_minutes", "Completeness", CRITICAL,
             lambda d: delivered.reindex(d.index, fill_value=False)
                       & _blank(d["delivery_minutes"])),
            ("order_id_unique", "Uniqueness", CRITICAL,
             lambda d: d["order_id"].duplicated(keep="last")),
            ("status_in_allowed_set", "Validity", CRITICAL,
             lambda d: ~standardise_status(d["order_status"]).isin(ORDER_STATUSES)),
            ("payment_mode_mappable", "Validity", WARNING,
             lambda d: standardise_payment_mode(d["payment_mode"]).isna()),
            ("order_ts_parses", "Validity", CRITICAL,
             lambda d: _ts(d["order_ts"]).isna() & ~_blank(d["order_ts"])),
            ("discount_pct_0_to_100", "Validity", CRITICAL,
             lambda d: ~_num(d["discount_pct"]).between(0, 100)),
            ("quantity_positive", "Accuracy", CRITICAL,
             lambda d: ~(_num(d["quantity"]) > 0)),
            ("unit_price_positive", "Accuracy", CRITICAL,
             lambda d: ~(_num(d["unit_price"]) > 0)),
            ("amount_matches_formula", "Accuracy", CRITICAL,
             lambda d: (_num(d["order_amount"]) - (
                 _num(d["quantity"]) * _num(d["unit_price"])
                 * (1 - _num(d["discount_pct"]) / 100)).round(2)
             ).abs() > AMOUNT_TOLERANCE),
            ("delivery_minutes_plausible", "Accuracy", CRITICAL,
             lambda d: _num(d["delivery_minutes"]) >= MAX_DELIVERY_MINUTES),
            ("customer_id_known", "Consistency", CRITICAL,
             lambda d: ~_blank(d["customer_id"]) & ~d["customer_id"].isin(good_customers)),
            ("product_id_known", "Consistency", CRITICAL,
             lambda d: ~_blank(d["product_id"]) & ~d["product_id"].isin(good_products)),
            ("store_id_known", "Consistency", CRITICAL,
             lambda d: ~_blank(d["store_id"]) & ~d["store_id"].isin(good_stores)),
            ("order_ts_not_future", "Timeliness", CRITICAL,
             lambda d: _ts(d["order_ts"]) > now_naive),
        ]
        orders_ok, orders_bad, rows = run_checks(
            orders, order_checks, dataset="orders", run_id=run_id, dag_id=DAG_ID)
        dq += rows

        good_orders = set(orders_ok["order_id"])
        order_amount_by_id = dict(zip(orders_ok["order_id"], _num(orders_ok["order_amount"])))

        def _success_mismatch(d: pd.DataFrame) -> pd.Series:
            expected = d["order_id"].map(order_amount_by_id)
            is_success = d["payment_status"].astype("string").str.strip().str.upper() == "SUCCESS"
            return is_success & expected.notna() & (
                (_num(d["amount_paid"]) - expected).abs() > AMOUNT_TOLERANCE)

        payment_checks = [
            ("gateway_ref_present", "Completeness", WARNING,
             lambda d: _blank(d["gateway_ref"])),
            ("payment_id_unique", "Uniqueness", CRITICAL,
             lambda d: d["payment_id"].duplicated(keep="last")),
            ("one_payment_per_order", "Uniqueness", WARNING,
             lambda d: d["order_id"].duplicated(keep=False)),
            ("payment_mode_mappable", "Validity", WARNING,
             lambda d: standardise_payment_mode(d["payment_mode"]).isna()),
            ("payment_status_known", "Validity", CRITICAL,
             lambda d: ~d["payment_status"].astype("string").str.strip().str.upper()
                       .isin(["SUCCESS", "FAILED", "REFUNDED"])),
            ("amount_paid_positive", "Accuracy", CRITICAL,
             lambda d: ~(_num(d["amount_paid"]) > 0)),
            # Warning, not Critical, and deliberately so: a payment whose order
            # was rejected is money that arrived with nothing to match it. That
            # is precisely the finance gap the client hired us to close, so it
            # must survive into etl_payment_reconciliation as a named exception.
            # Quarantining it would hide real cash — the Excel process's exact
            # failure mode.
            ("order_id_known", "Consistency", WARNING,
             lambda d: ~d["order_id"].isin(good_orders)),
            # Flagged, not quarantined: the mismatch IS the finance finding and
            # must survive into etl_payment_reconciliation (criterion 7).
            ("success_amount_matches_order", "Consistency", WARNING, _success_mismatch),
            ("payment_ts_not_future", "Timeliness", CRITICAL,
             lambda d: _ts(d["payment_ts"]) > now_naive),
        ]
        payments_ok, payments_bad, rows = run_checks(
            payments, payment_checks, dataset="payments", run_id=run_id, dag_id=DAG_ID)
        dq += rows

        # Volume and freshness checks on the batch as a whole (Part F 7 and 9).
        latest = _ts(orders_ok["order_ts"]).max()
        staleness_days = (now_naive - latest).days if pd.notna(latest) else None
        batch_dq = [
            ("orders", "batch_volume_not_collapsed", "Volume", WARNING,
             len(orders) < 500),
            ("orders", "batch_freshness_within_1_day", "Timeliness", WARNING,
             staleness_days is None or staleness_days > 1),
        ]
        # Distribution check (Part F experiment 8). Every other rule here is
        # row-level, and a pricing error is not wrong on any single row: a
        # consistent 100x on one store satisfies the amount formula, keeps
        # prices positive and leaves quantity alone. Measured: it inflates
        # reported revenue from Rs 1.22M to Rs 9.59M with zero check failures.
        #
        # So this compares stores against each other rather than against a
        # threshold — a cross-sectional check needs no history, which matters
        # because on day one there is none. Severity Warning on purpose: a
        # genuine promotion can move a store's average, and a Critical rule
        # here would quarantine a real day's revenue to catch a maybe.
        delivered_ok = orders_ok[
            standardise_status(orders_ok["order_status"]) == "DELIVERED"].copy()
        drifted = []
        stores_checked = 0
        if len(delivered_ok) > 20:
            delivered_ok["_amt"] = _num(delivered_ok["order_amount"])
            store_mean = delivered_ok.groupby("store_id")["_amt"].mean().dropna()
            stores_checked = len(store_mean)
            typical = store_mean.median()
            if typical and typical > 0:
                ratio = store_mean / typical
                drifted = sorted(store_mean.index[(ratio > 5) | (ratio < 0.2)])
                if drifted:
                    print(f"[dq] WARNING orders.store_value_distribution: "
                          f"{len(drifted)} store(s) far from the median store's "
                          f"average order value ({typical:,.2f}): "
                          + ", ".join(f"{s}={store_mean[s]:,.2f}" for s in drifted))
        dq.append({
            "run_id": run_id, "dag_id": DAG_ID,
            "check_name": "orders.store_value_distribution",
            "dimension": "Distribution", "severity": WARNING,
            # stores compared, not orders — the ELT reports the same unit
            "rows_checked": stores_checked, "rows_failed": len(drifted),
            "status": "FAIL" if drifted else "PASS",
            "checked_at": now.to_iso8601_string(),
        })

        for dataset, name, dimension, severity, failed in batch_dq:
            if failed:
                print(f"[dq] WARNING {dataset}.{name}: "
                      f"rows={len(orders)} latest_order_ts={latest} "
                      f"staleness_days={staleness_days}")
            dq.append({
                "run_id": run_id, "dag_id": DAG_ID,
                "check_name": f"{dataset}.{name}", "dimension": dimension,
                "severity": severity, "rows_checked": len(orders),
                "rows_failed": int(bool(failed)),
                "status": "FAIL" if failed else "PASS",
                "checked_at": now.to_iso8601_string(),
            })

        # ── quarantine, with reasons ─────────────────────────────────────────
        quarantined = {}
        for dataset, bad in [("orders", orders_bad), ("payments", payments_bad),
                             ("customers", customers_bad), ("products", products_bad),
                             ("stores", stores_bad)]:
            path = quarantine_to_csv(bad, QUARANTINE_DIR, dataset, run_id)
            quarantined[dataset] = len(bad)
            if path:
                print(f"[quarantine] {dataset}: {len(bad)} rows -> {path}")

        con = duckdb.connect(WAREHOUSE)
        try:
            n = write_dq_results(con, dq)
        finally:
            con.close()
        print(f"[dq] {n} check results written; "
              f"{sum(r['rows_failed'] > 0 for r in dq)} checks failing")

        paths = {}
        for dataset, frame in [("orders", orders_ok), ("payments", payments_ok),
                               ("customers", customers_ok), ("products", products_ok),
                               ("stores", stores_ok)]:
            out = _staging_path(run_id, f"{dataset}_clean")
            frame.to_parquet(out, index=False)
            paths[dataset] = out
            print(f"[validate] {dataset}: {len(frame)} passed, "
                  f"{quarantined[dataset]} quarantined")

        return {
            "paths": paths,
            "rows_extracted": sum(len(f) for f in
                                  [orders, payments, customers, products, stores]),
            "rows_quarantined": sum(quarantined.values()),
            "quarantined_by_dataset": quarantined,
        }

    # ── TRANSFORM ────────────────────────────────────────────────────────────

    @task(pool="duckdb_writer")
    def transform(validated: dict, **context) -> dict:
        run_id = context["run_id"]
        p = validated["paths"]
        orders = pd.read_parquet(p["orders"])
        customers = pd.read_parquet(p["customers"])
        products = pd.read_parquet(p["products"])
        stores = pd.read_parquet(p["stores"])

        # ── dimensions: standardised, deduplicated, PII masked ───────────────
        dim_customer = pd.DataFrame({
            "customer_id": customers["customer_id"],
            "customer_name": customers["full_name"].map(mask_name),
            "email_masked": customers["email"].map(mask_email),
            "phone_masked": customers["phone"].map(mask_phone),
            "city": customers["city"].astype("string").str.strip(),
            "area": customers["area"].astype("string").str.strip(),
            "pincode": customers["pincode"],
            "signup_date": _ts(customers["signup_date"]).dt.date,
            "loyalty_tier": standardise_tier(customers["loyalty_tier"]),
            "is_active": customers["is_active"].astype("string").str.strip().str.upper() == "Y",
        }).drop_duplicates(subset="customer_id", keep="last")

        dim_product = pd.DataFrame({
            "product_id": products["product_id"],
            "product_name": products["product_name"].astype("string").str.strip(),
            # Repaired, not dropped. A blank category becomes visible in the
            # margin report rather than deleting the product's 48 orders.
            "category": products["category"].astype("string").str.strip()
                        .replace("", pd.NA).fillna("UNCATEGORISED"),
            "sub_category": products["sub_category"].astype("string").str.strip(),
            "brand": products["brand"].astype("string").str.strip(),
            "mrp": _num(products["mrp"]),
            "cost_price": _num(products["cost_price"]),
            "unit": products["unit"].astype("string").str.strip(),
            "is_perishable": products["is_perishable"].astype("string").str.strip().str.upper()
                             .isin(["TRUE", "Y", "YES", "1"]),
            "launch_date": _ts(products["launch_date"]).dt.date,
        }).drop_duplicates(subset="product_id", keep="last")

        dim_store = pd.DataFrame({
            "store_id": stores["store_id"],
            "store_name": stores["store_name"].astype("string").str.strip(),
            # 'UNKNOWN' keeps S07's 176 orders and Rs 93,674 in the COO's zone
            # report, visibly unattributed, instead of silently deleting them.
            "zone": stores["zone"].astype("string").str.strip()
                    .replace("", pd.NA).fillna("UNKNOWN"),
            "area": stores["area"].astype("string").str.strip(),
            "pincode": stores["pincode"],
            "open_date": _ts(stores["open_date"]).dt.date,
            "manager_name": stores["manager_name"].map(mask_name),
            "capacity_orders_per_day": _num(stores["capacity_orders_per_day"]).astype("Int64"),
        }).drop_duplicates(subset="store_id", keep="last")

        # ── fact: join to the three dimensions and derive the metrics ────────
        o = orders.drop_duplicates(subset="order_id", keep="last").copy()
        o["order_ts"] = _ts(o["order_ts"])
        o["order_date"] = o["order_ts"].dt.date
        o["order_status"] = standardise_status(o["order_status"])
        o["payment_mode"] = standardise_payment_mode(o["payment_mode"])
        o["quantity"] = _num(o["quantity"]).astype("Int64")
        o["unit_price"] = _num(o["unit_price"])
        o["discount_pct"] = _num(o["discount_pct"])
        o["order_amount"] = _num(o["order_amount"]).round(2)
        o["delivery_minutes"] = _num(o["delivery_minutes"])

        fact = (o.merge(dim_product[["product_id", "category", "sub_category", "cost_price"]],
                        on="product_id", how="left")
                 .merge(dim_store[["store_id", "store_name", "zone"]],
                        on="store_id", how="left")
                 .merge(dim_customer[["customer_id", "loyalty_tier"]],
                        on="customer_id", how="left"))

        is_delivered = fact["order_status"] == "DELIVERED"
        # Only DELIVERED counts as revenue; CANCELLED and RETURNED contribute 0.
        fact["net_revenue"] = (fact["order_amount"].where(is_delivered, 0.0)).round(2)
        fact["gross_margin"] = (
            fact["net_revenue"] - (fact["quantity"] * fact["cost_price"]).where(is_delivered, 0.0)
        ).round(2)
        fact["within_15_min"] = is_delivered & (fact["delivery_minutes"] <= SLA_MINUTES)
        fact["is_delivered"] = is_delivered
        fact["_run_id"] = run_id
        fact["_loaded_at"] = pendulum.now("UTC").to_iso8601_string()

        fact = fact[[
            "order_id", "order_date", "order_ts", "customer_id", "store_id", "store_name",
            "zone", "product_id", "category", "sub_category", "loyalty_tier",
            "quantity", "unit_price", "discount_pct", "cost_price", "order_amount",
            "net_revenue", "gross_margin", "payment_mode", "order_status", "channel",
            "delivery_minutes", "within_15_min", "is_delivered",
            # carried through so Part E.3 can trace any mart number back to the
            # exact file the row arrived in
            "_source_file", "_run_id", "_loaded_at",
        ]]

        out = {}
        for name, frame in [("fact_orders", fact), ("dim_customer", dim_customer),
                            ("dim_product", dim_product), ("dim_store", dim_store)]:
            path = _staging_path(run_id, name)
            frame.to_parquet(path, index=False)
            out[name] = path
            print(f"[transform] {name}: {len(frame)} rows")

        print(f"[transform] net revenue in batch: "
              f"Rs {fact['net_revenue'].sum():,.2f} over {int(is_delivered.sum())} delivered orders")
        return {"paths": out, "rows_loaded": len(fact),
                "payments_path": p["payments"]}

    # ── LOAD — idempotent star schema ────────────────────────────────────────

    @task(pool="duckdb_writer")
    def load(transformed: dict) -> dict:
        paths = transformed["paths"]
        con = duckdb.connect(WAREHOUSE)
        try:
            # Dimensions are small masters: replacing them wholesale is both
            # simplest and trivially idempotent.
            for table, path in [("etl_dim_customer", paths["dim_customer"]),
                                ("etl_dim_product", paths["dim_product"]),
                                ("etl_dim_store", paths["dim_store"])]:
                con.execute(
                    f"CREATE OR REPLACE TABLE {table} AS "
                    f"SELECT * FROM read_parquet('{path}')")
                n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                print(f"[load] {table}: {n} rows (replaced)")

            # The fact table is an upsert keyed on order_id. This is what makes
            # a re-run restate instead of double-count (criterion 5) and the
            # increment file's corrections overwrite instead of duplicate
            # (criterion 6) — one mechanism, both requirements.
            con.execute(
                "CREATE TABLE IF NOT EXISTS etl_fact_orders AS "
                f"SELECT * FROM read_parquet('{paths['fact_orders']}') WHERE 1=0")
            con.execute(
                f"CREATE OR REPLACE TEMP VIEW batch AS "
                f"SELECT * FROM read_parquet('{paths['fact_orders']}')")
            before = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
            updated = con.execute(
                "SELECT count(*) FROM etl_fact_orders f "
                "WHERE f.order_id IN (SELECT order_id FROM batch)").fetchone()[0]
            con.execute(
                "DELETE FROM etl_fact_orders "
                "WHERE order_id IN (SELECT order_id FROM batch)")
            con.execute("INSERT INTO etl_fact_orders SELECT * FROM batch")
            after = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]

            print(f"[load] etl_fact_orders: {before} -> {after} rows "
                  f"({updated} restated, {after - before} new)")
            revenue = con.execute(
                "SELECT round(sum(net_revenue), 2) FROM etl_fact_orders").fetchone()[0]
            print(f"[load] total net revenue in fact: Rs {revenue:,.2f}")
        finally:
            con.close()

        return {"rows_loaded": after, "rows_restated": updated,
                "payments_path": transformed["payments_path"]}

    # ── RECONCILE — orders vs the payment gateway (criterion 7) ──────────────

    @task(pool="duckdb_writer")
    def reconcile(loaded: dict) -> int:
        con = duckdb.connect(WAREHOUSE)
        try:
            con.execute(
                "CREATE OR REPLACE TEMP VIEW pay AS SELECT * FROM read_parquet('"
                + loaded["payments_path"] + "')")
            con.execute("""
                CREATE OR REPLACE TABLE etl_payment_reconciliation AS
                WITH p AS (
                    SELECT order_id, payment_id, gateway_ref,
                           upper(trim(payment_status)) AS payment_status,
                           TRY_CAST(amount_paid AS DOUBLE) AS amount_paid
                    FROM pay
                ),
                paid AS (
                    SELECT order_id,
                           sum(CASE WHEN payment_status='SUCCESS' THEN amount_paid ELSE 0 END)
                             AS amount_collected,
                           count(*) AS payment_rows,
                           max(payment_id) AS sample_payment_id,
                           bool_or(gateway_ref IS NULL OR trim(gateway_ref)='') AS missing_gateway_ref
                    FROM p GROUP BY order_id
                )
                SELECT
                    coalesce(f.order_id, paid.order_id)      AS order_id,
                    f.order_date,
                    f.store_id,
                    f.order_status,
                    f.order_amount                           AS order_amount,
                    paid.amount_collected,
                    round(coalesce(f.order_amount,0) - coalesce(paid.amount_collected,0), 2)
                                                             AS variance,
                    paid.sample_payment_id,
                    CASE
                      WHEN f.order_id IS NULL
                        THEN 'payment with no matching order'
                      WHEN paid.order_id IS NULL AND f.order_status='CANCELLED'
                        THEN 'cancelled order, no payment expected'
                      WHEN paid.order_id IS NULL
                        THEN 'delivered order with no payment recorded'
                      WHEN f.order_status='RETURNED' AND paid.amount_collected > 0
                        THEN 'returned order still shows collected cash'
                      WHEN abs(coalesce(f.order_amount,0) - coalesce(paid.amount_collected,0)) > 0.01
                        THEN 'amount collected does not equal order amount'
                      ELSE 'reconciled'
                    END                                      AS reason
                FROM etl_fact_orders f
                FULL OUTER JOIN paid ON paid.order_id = f.order_id
            """)
            # Keep only the exceptions: one row per mismatch, as the client asked.
            con.execute("""
                DELETE FROM etl_payment_reconciliation
                WHERE reason IN ('reconciled', 'cancelled order, no payment expected')
            """)
            n = con.execute("SELECT count(*) FROM etl_payment_reconciliation").fetchone()[0]
            breakdown = con.execute("""
                SELECT reason, count(*) AS orders, round(sum(variance),2) AS total_variance
                FROM etl_payment_reconciliation GROUP BY reason ORDER BY orders DESC
            """).fetchdf()
            print(f"[reconcile] {n} mismatching orders\n{breakdown.to_string(index=False)}")
        finally:
            con.close()
        return n

    # ── REPORT ───────────────────────────────────────────────────────────────

    @task(pool="duckdb_writer")
    def report(_reconciled: int) -> dict:
        con = duckdb.connect(WAREHOUSE)
        try:
            con.execute("""
                CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                SELECT order_date, zone, store_id, store_name,
                       count(*) FILTER (WHERE is_delivered)        AS delivered_orders,
                       round(sum(net_revenue), 2)                  AS net_revenue,
                       round(avg(delivery_minutes) FILTER (WHERE is_delivered), 1)
                                                                   AS avg_delivery_minutes,
                       round(100.0 * count(*) FILTER (WHERE within_15_min)
                             / nullif(count(*) FILTER (WHERE is_delivered), 0), 1)
                                                                   AS pct_within_15_min
                FROM etl_fact_orders
                GROUP BY order_date, zone, store_id, store_name
                ORDER BY order_date, zone, store_id
            """)
            con.execute("""
                CREATE OR REPLACE TABLE etl_category_margin AS
                SELECT category,
                       count(*) FILTER (WHERE is_delivered) AS delivered_orders,
                       round(sum(net_revenue), 2)           AS net_revenue,
                       round(sum(gross_margin), 2)          AS gross_margin,
                       round(100.0 * sum(gross_margin) / nullif(sum(net_revenue), 0), 1)
                                                            AS margin_pct
                FROM etl_fact_orders
                GROUP BY category ORDER BY net_revenue DESC
            """)
            zone = con.execute("""
                SELECT zone, round(sum(net_revenue),2) AS net_revenue,
                       sum(delivered_orders) AS delivered_orders
                FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC
            """).fetchdf()
            margin = con.execute("SELECT * FROM etl_category_margin").fetchdf()
            print(f"\n[report] Net revenue by zone:\n{zone.to_string(index=False)}")
            print(f"\n[report] Margin by category:\n{margin.to_string(index=False)}")
            totals = {"net_revenue": float(zone["net_revenue"].sum()),
                      "delivered_orders": int(zone["delivered_orders"].sum()),
                      "zones": int(len(zone))}
        finally:
            con.close()
        return totals

    # ── RUN AUDIT — always runs, even after a failure (Part F) ───────────────

    # retries=0: this task fails as a FUNCTION of the run's outcome, so a
    # retry can never succeed. Observed after adding the raise below — it
    # retried to try_number 3, burning two minutes and delaying the only
    # failure signal the client gets by two minutes on a 09:00 SLA.
    @task(trigger_rule="all_done", retries=0, pool="duckdb_writer")
    def run_audit(validated: dict, loaded: dict, reported: dict | None = None,
                  **context) -> None:
        dag_run = context["dag_run"]
        # Airflow 3's runtime DagRun is a plain dataclass with no database
        # access — there is no get_task_instances() to ask for sibling states.
        # Run health is inferred from whether each upstream task published its
        # XCom: with trigger_rule="all_done" this task runs even when an
        # upstream one fails, and a failed task's argument arrives as None.
        stages = [("validate", validated), ("load", loaded), ("report", reported)]
        missing = [name for name, value in stages if not value]
        status = "FAILED" if missing else "SUCCESS"
        if missing:
            print(f"[audit] stages that produced no result: {missing}")

        con = duckdb.connect(WAREHOUSE)
        try:
            write_pipeline_run(
                con,
                run_id=context["run_id"],
                dag_id=DAG_ID,
                start_time=str(dag_run.start_date),
                end_time=pendulum.now("UTC").to_iso8601_string(),
                rows_extracted=(validated or {}).get("rows_extracted", 0),
                rows_loaded=(loaded or {}).get("rows_loaded", 0),
                rows_quarantined=(validated or {}).get("rows_quarantined", 0),
                status=status,
            )
            history = con.execute("""
                SELECT run_id, rows_extracted, rows_loaded, rows_quarantined, status
                FROM pipeline_runs WHERE dag_id = ?
                ORDER BY start_time DESC LIMIT 5
            """, [DAG_ID]).fetchdf()
        finally:
            con.close()
        print(f"[audit] status={status}\n{history.to_string(index=False)}")

        # The audit row is written FIRST, then this task fails if the run did.
        #
        # Two measured defects, one fix:
        #
        # 1. False green. run_audit is the only leaf task and carries
        #    trigger_rule="all_done", so it succeeded even when every other
        #    task failed — and Airflow derives dag-run state from its leaves.
        #    Experiments 4, 6 and 7 all produced run_state=success having
        #    loaded nothing. Criterion 9 says anyone can see whether today's
        #    run worked; a green run that loaded nothing is worse than a red
        #    one, because nobody goes looking.
        #
        # 2. Stale audit after recovery. Because run_audit had *succeeded*,
        #    `tasks clear --only-failed --downstream` skipped it, so after a
        #    successful recovery pipeline_runs still said status=FAILED,
        #    rows_loaded=0 while the fact table held 2310 rows. Now that this
        #    task fails with the run, a recovery clear includes it and
        #    write_pipeline_run restates the row.
        #
        # Ordering matters: write, then raise. An audit task that raises before
        # recording leaves no evidence of the failure it is reporting.
        if status == "FAILED":
            from airflow.exceptions import AirflowException
            raise AirflowException(
                f"pipeline run recorded as FAILED (stages with no result: "
                f"{missing}); failing run_audit so the dag run is not "
                f"reported green")

    # ── WIRING ───────────────────────────────────────────────────────────────
    #
    #   extract_orders    ─┐
    #   extract_payments  ─┤
    #   extract_customers ─┼─► validate ─► transform ─► load ─► reconcile ─► report
    #   extract_products  ─┤                                        │
    #   extract_stores    ─┘                                        └─► run_audit
    #                                                            (trigger_rule=all_done)

    orders_in = extract_orders()
    payments_in = _extract("payments", "payments.csv")
    customers_in = _extract("customers", "customers.csv")
    products_in = _extract("products", "products.csv")
    stores_in = _extract("stores", "stores.csv")

    validated = validate(orders_in, payments_in, customers_in, products_in, stores_in)
    transformed = transform(validated)
    loaded = load(transformed)
    reconciled = reconcile(loaded)
    reported = report(reconciled)

    # `reported` is passed as an argument rather than wired with `>>` so the
    # whole body is a pure data-dependency graph. That keeps the dependency
    # explicit in the UI and lets tests drive the body outside Airflow.
    # trigger_rule="all_done" still makes this run after an upstream failure.
    run_audit(validated, loaded, reported)


nammamart_etl()
