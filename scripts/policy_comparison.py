#!/usr/bin/env python3
"""
scripts/policy_comparison.py
----------------------------
Run both pipelines under BOTH master-data severity policies and measure the
difference. This is the evidence behind the Part G recommendation.

The question: the brief *suggests* Critical severity for master-data content
defects (products.category not null, cost_price <= mrp, keys exist in masters).
Applied literally, rejecting a defective master row also rejects every order
that points at it. How much revenue does that cost?

I estimated Rs 248,156 from the source CSVs early on. This script replaces the
estimate with a measured pipeline output, because a recommendation to a CTO
should quote what the pipeline did, not what a spreadsheet predicted.

    lenient — master rows are Critical only on KEY defects (null/duplicate key).
              Content defects are Warning and repaired to a visible sentinel
              (zone='UNKNOWN', category='UNCATEGORISED').
    strict  — the brief's suggested severities applied literally. Master content
              defects are Critical, and their orders cascade into quarantine.

Usage:
    .venv/bin/python -I scripts/policy_comparison.py
"""

import json
import os
import shutil
import subprocess
import sys
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRISTINE = os.path.join(REPO, "assignment", "nammamart_dataset")
DATA = os.path.join(REPO, "include", "data", "nammamart")
WAREHOUSE = os.path.join(REPO, "include", "warehouse_arnav_ganguly.duckdb")
QUARANTINE = os.path.join(REPO, "include", "quarantine", "arnav_ganguly")
OUT = os.path.join(REPO, "include", "policy_comparison.json")

ASTRO = os.path.expanduser("~/.local/bin/astro")
ETL = "nammamart_etl_arnav_ganguly"
ELT = "nammamart_elt_arnav_ganguly"
POLICY_VAR = "nammamart_master_dq_policy"

BASE_FILES = ["orders.csv", "payments.csv", "customers.csv",
              "products.csv", "stores.csv"]


def sh(cmd, timeout=900):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout, cwd=REPO)


def psql(sql):
    pg = sh("docker ps -q --filter 'name=etl-elt-dag.*postgres'").stdout.strip()
    out = sh(f"docker exec {pg} psql -U postgres -d postgres -At -F'|' -c \"{sql}\"")
    return [l.split("|") for l in out.stdout.strip().splitlines() if l]


def latest_run(dag_id):
    rows = psql(f"SELECT run_id, state FROM dag_run WHERE dag_id='{dag_id}' "
                "ORDER BY start_date DESC NULLS LAST LIMIT 1")
    return (rows[0][0], rows[0][1]) if rows else (None, None)


def trigger_and_wait(dag_id, timeout=900):
    before, _ = latest_run(dag_id)
    sh(f"{ASTRO} dev run dags trigger {dag_id}")
    deadline = time.time() + timeout
    while time.time() < deadline:
        run_id, state = latest_run(dag_id)
        if run_id and run_id != before and state not in ("queued", "running", None):
            return run_id, state
        time.sleep(5)
    return None, "timeout"


def reset():
    for suffix in ("", ".wal"):
        if os.path.exists(WAREHOUSE + suffix):
            os.remove(WAREHOUSE + suffix)
    if os.path.isdir(QUARANTINE):
        for name in os.listdir(QUARANTINE):
            os.remove(os.path.join(QUARANTINE, name))
    os.makedirs(DATA, exist_ok=True)
    for name in os.listdir(DATA):
        os.remove(os.path.join(DATA, name))
    for name in BASE_FILES:
        shutil.copy(os.path.join(PRISTINE, name), DATA)


def measure():
    import duckdb
    con = duckdb.connect(WAREHOUSE, read_only=True)

    def one(sql, default=0):
        try:
            v = con.execute(sql).fetchone()[0]
            return v if v is not None else default
        except Exception:
            return default

    out = {
        "etl_fact_rows": one("SELECT count(*) FROM etl_fact_orders"),
        "etl_delivered": one("SELECT count(*) FROM etl_fact_orders WHERE is_delivered"),
        "etl_revenue": one("SELECT round(sum(net_revenue),2) FROM etl_fact_orders"),
        "etl_margin": one("SELECT round(sum(gross_margin),2) FROM etl_fact_orders"),
        "elt_stg_rows": one("SELECT count(*) FROM stg_orders"),
        "elt_quarantined": one(
            "SELECT count(*) FROM stg_orders WHERE dq_status='QUARANTINED'"),
        "elt_revenue": one("SELECT round(sum(net_revenue),2) FROM mart_daily_store_kpis"),
        "dim_store_rows": one("SELECT count(*) FROM etl_dim_store"),
        "dim_product_rows": one("SELECT count(*) FROM etl_dim_product"),
        "dim_customer_rows": one("SELECT count(*) FROM etl_dim_customer"),
        "zones_reported": one("SELECT count(DISTINCT zone) FROM mart_daily_store_kpis"),
        "categories_reported": one("SELECT count(*) FROM mart_category_margin"),
        "stores_in_sla": one("SELECT count(*) FROM mart_delivery_sla"),
        "critical_checks_failing": one(
            "SELECT count(*) FROM dq_results WHERE severity='Critical' AND status='FAIL'"),
        "critical_rows_failed": one(
            "SELECT sum(rows_failed) FROM dq_results WHERE severity='Critical'"),
        "warning_rows_failed": one(
            "SELECT sum(rows_failed) FROM dq_results WHERE severity='Warning'"),
    }
    # which zones/categories actually survive into the report
    try:
        out["zone_list"] = sorted(
            r[0] for r in con.execute(
                "SELECT DISTINCT zone FROM mart_daily_store_kpis").fetchall()
            if r[0])
        out["category_list"] = sorted(
            r[0] for r in con.execute(
                "SELECT category FROM mart_category_margin").fetchall() if r[0])
    except Exception:
        out["zone_list"], out["category_list"] = [], []
    con.close()
    return out


def quarantine_counts():
    if not os.path.isdir(QUARANTINE):
        return {}
    out = {}
    for name in sorted(os.listdir(QUARANTINE)):
        with open(os.path.join(QUARANTINE, name)) as fh:
            out[name.split("_failed_")[0]] = sum(1 for _ in fh) - 1
    return out


def run_policy(policy):
    print(f"\n{'=' * 78}\nPOLICY: {policy}\n{'=' * 78}")
    sh(f"{ASTRO} dev run variables set {POLICY_VAR} {policy}")
    got = sh(f"{ASTRO} dev run variables get {POLICY_VAR}").stdout
    if policy not in got:
        print(f"  WARNING: could not confirm the Variable was set; got {got[-120:]!r}")
    reset()
    states = {}
    for dag_id in (ETL, ELT):
        print(f"  running {dag_id} ...", flush=True)
        run_id, state = trigger_and_wait(dag_id)
        states[dag_id] = state
        print(f"    {state}")
    m = measure()
    m["run_states"] = states
    m["quarantine_files"] = quarantine_counts()
    print(f"  ETL fact {m['etl_fact_rows']} rows / {m['etl_delivered']} delivered "
          f"/ Rs {m['etl_revenue']:,.2f}")
    print(f"  ELT staged {m['elt_stg_rows']} / {m['elt_quarantined']} quarantined "
          f"/ Rs {m['elt_revenue']:,.2f}")
    print(f"  dims: store {m['dim_store_rows']}, product {m['dim_product_rows']}, "
          f"customer {m['dim_customer_rows']}")
    print(f"  zones reported: {m['zone_list']}")
    print(f"  quarantine files: {m['quarantine_files']}")
    return m


def main():
    results = {}
    for policy in ("lenient", "strict"):
        results[policy] = run_policy(policy)

    # leave the environment on the recommended default
    sh(f"{ASTRO} dev run variables set {POLICY_VAR} lenient")

    a, b = results["lenient"], results["strict"]
    print(f"\n{'=' * 78}\nCOMPARISON\n{'=' * 78}")
    rows = [
        ("ETL fact rows", "etl_fact_rows", "{:,.0f}"),
        ("delivered orders", "etl_delivered", "{:,.0f}"),
        ("net revenue (Rs)", "etl_revenue", "{:,.2f}"),
        ("gross margin (Rs)", "etl_margin", "{:,.2f}"),
        ("ELT staged rows", "elt_stg_rows", "{:,.0f}"),
        ("ELT quarantined", "elt_quarantined", "{:,.0f}"),
        ("dim_store rows", "dim_store_rows", "{:,.0f}"),
        ("dim_product rows", "dim_product_rows", "{:,.0f}"),
        ("dim_customer rows", "dim_customer_rows", "{:,.0f}"),
        ("zones in report", "zones_reported", "{:,.0f}"),
        ("categories in report", "categories_reported", "{:,.0f}"),
        ("stores in SLA table", "stores_in_sla", "{:,.0f}"),
        ("Critical rows failed", "critical_rows_failed", "{:,.0f}"),
        ("Warning rows failed", "warning_rows_failed", "{:,.0f}"),
    ]
    print(f"{'metric':24s} {'lenient':>18s} {'strict':>18s} {'delta':>18s}")
    print("-" * 82)
    for label, key, fmt in rows:
        va, vb = a.get(key) or 0, b.get(key) or 0
        delta = vb - va
        print(f"{label:24s} {fmt.format(va):>18s} {fmt.format(vb):>18s} "
              f"{fmt.format(delta):>18s}")

    lost = (a.get("etl_revenue") or 0) - (b.get("etl_revenue") or 0)
    pct = 100 * lost / (a.get("etl_revenue") or 1)
    print(f"\nStrict policy removes Rs {lost:,.2f} of reported revenue "
          f"({pct:.1f}% of the lenient figure)")
    print(f"lenient zones:    {a.get('zone_list')}")
    print(f"strict zones:     {b.get('zone_list')}")
    print(f"lenient categories: {a.get('category_list')}")
    print(f"strict categories:  {b.get('category_list')}")

    results["delta"] = {"revenue_lost": round(lost, 2), "pct_lost": round(pct, 2)}
    with open(OUT, "w") as fh:
        json.dump(results, fh, indent=2, default=str)
    print(f"\nwritten to {os.path.relpath(OUT, REPO)}")


if __name__ == "__main__":
    main()
