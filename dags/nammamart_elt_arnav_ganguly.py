"""
dags/nammamart_elt_arnav_ganguly.py
-----------------------------------
NammaMart ELT pipeline — Extract -> Load raw -> Transform in the warehouse.

The analytics-and-compliance pipeline: every source row is kept exactly as
received, and the quality rules are applied in SQL on the way to the mart
rather than at the door (acceptance criterion 3).

Three layers in DuckDB:
    raw_*  — exact copy of each CSV, every column text, plus _loaded_at,
             _source_file, _run_id, _file_seq, _row_num. Append-only.
    stg_*  — SQL only: cast, standardise, dedupe, mask PII, apply the quality
             rules, add dq_status and failure_reason.
    mart_* — SQL only: the five business-ready tables.

Every transformation is a .sql file in include/sql/, run by one task per table.
No pandas touches the data: the raw load is a DuckDB `read_csv(all_varchar)`
INSERT, so there is no pandas type inference anywhere in this pipeline.

Raw tables grow on every run, by design — that is what "keep every record for
audit" means. Idempotency lives in staging instead: stg_orders upserts on
order_id with the most recently loaded copy winning, so mart totals do not move
when the DAG is re-run (criterion 5), and the increment file's 10 corrections
overwrite their originals while its 150 new orders are inserted (criterion 6).
"""

import sys

sys.path.insert(0, "/usr/local/airflow/include")

import glob
import os
from datetime import timedelta

import duckdb
import pendulum
from airflow.decorators import dag, task

from nammamart_dq import (
    POLICY_VAR,
    PIPELINE_RUNS_DDL,
    master_content_severity,
    read_policy,
    write_pipeline_run,
)

OWNER = "arnav_ganguly"
AIRFLOW_HOME = "/usr/local/airflow"
DATA_DIR = f"{AIRFLOW_HOME}/include/data/nammamart"
SQL_DIR = f"{AIRFLOW_HOME}/include/sql"
WAREHOUSE = f"{AIRFLOW_HOME}/include/warehouse_{OWNER}.duckdb"

DAG_ID = f"nammamart_elt_{OWNER}"

# dataset -> the file(s) that feed its raw table. Orders takes a glob so any
# increment file that has landed is appended to raw alongside the base export.
SOURCES = {
    "orders": ["orders.csv", "orders_*_increment.csv"],
    "payments": ["payments.csv"],
    "customers": ["customers.csv"],
    "products": ["products.csv"],
    "stores": ["stores.csv"],
}

default_args = {
    "owner": OWNER,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=20),
}


def _sql(name: str, **params) -> str:
    """Read a .sql file and substitute @placeholder@ tokens.

    @-delimited rather than Python {} formatting because SQL is full of braces
    in struct and lambda syntax, and str.format would choke on them.
    """
    with open(os.path.join(SQL_DIR, f"{name}.sql")) as fh:
        text = fh.read()
    for key, value in params.items():
        text = text.replace(f"@{key}@", str(value))
    leftover = [tok for tok in ("@run_id@", "@dag_id@", "@now_ts@", "@master_sev@")
                if tok in text]
    if leftover:
        raise ValueError(f"{name}.sql has unsubstituted placeholders: {leftover}")
    return text


def _run_sql(con: duckdb.DuckDBPyConnection, name: str, **params) -> None:
    """Run every statement in a .sql file."""
    con.execute(_sql(name, **params))


def _connect() -> duckdb.DuckDBPyConnection:
    """Open the warehouse with the shared macros already defined."""
    con = duckdb.connect(WAREHOUSE)
    con.execute(_sql("00_macros"))
    return con


def _context_params(context: dict) -> dict:
    policy = read_policy()
    return {
        "run_id": context["run_id"],
        "dag_id": DAG_ID,
        "now_ts": pendulum.now("Asia/Kolkata").format("YYYY-MM-DD HH:mm:ss"),
        "master_sev": master_content_severity(policy),
    }


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",                                   # 9:00 AM IST, Mon-Sat
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fde", "nammamart", "elt", OWNER],
    doc_md=__doc__,
)
def nammamart_elt():

    # ── EXTRACT + LOAD RAW — five parallel tasks, no cleaning ────────────────

    def _load_raw(dataset: str, patterns: list[str]):
        # pool="duckdb_writer" (1 slot) is load-bearing, not tuning.
        #
        # These five tasks have no dependencies on each other, so Airflow runs
        # them concurrently — and all five write the same DuckDB file. DuckDB is
        # a single-writer embedded database, so under the real scheduler the
        # losers get `IOException: Could not set lock on file`. Measured before
        # this pool existed: load_raw_products succeeded on try 1, customers on
        # try 2, payments on try 3, while orders and stores exhausted all three
        # tries and failed — 2 failed, 10 upstream_failed, nothing loaded.
        #
        # `astro dev run dags test` hides this completely, because it executes
        # every task sequentially in one process.
        #
        # The pool serialises the writers while keeping one task per table, so
        # the DAG still documents its sources. Retries alone would not fix it:
        # they turn a hard failure into a flaky one.
        #
        # Create it once with:
        #   astro dev run pools set duckdb_writer 1 "single-writer warehouse"
        #
        # The real fix at scale is a warehouse with MVCC (Postgres, BigQuery,
        # Snowflake), where this pool becomes unnecessary — see Part G.
        @task(task_id=f"load_raw_{dataset}", pool="duckdb_writer")
        def _inner(**context) -> dict:
            table = f"raw_{dataset}"
            paths = []
            for pattern in patterns:
                if "*" in pattern:
                    paths.extend(sorted(glob.glob(f"{DATA_DIR}/{pattern}")))
                else:
                    path = f"{DATA_DIR}/{pattern}"
                    if not os.path.exists(path):
                        raise FileNotFoundError(f"source file missing: {path}")
                    paths.append(path)
            if not paths:
                raise FileNotFoundError(
                    f"no source files matched {patterns} in {DATA_DIR}")

            loaded_at = pendulum.now("UTC").format("YYYY-MM-DD HH:mm:ss.SSSSSS")
            con = _connect()
            try:
                total = 0
                for seq, path in enumerate(paths):
                    # all_varchar: the raw layer stores exactly what the file
                    # said. No type inference, so 'Pending??' survives as text
                    # instead of becoming NULL before anyone can audit it.
                    src = (
                        f"SELECT *, "
                        f"  CAST('{loaded_at}' AS TIMESTAMP) AS _loaded_at, "
                        f"  '{os.path.basename(path)}'        AS _source_file, "
                        f"  '{context['run_id']}'             AS _run_id, "
                        f"  {seq}                             AS _file_seq, "
                        f"  row_number() OVER ()              AS _row_num "
                        f"FROM read_csv('{path}', all_varchar=true, header=true)"
                    )
                    con.execute(
                        f"CREATE TABLE IF NOT EXISTS {table} AS {src} LIMIT 0")
                    # Idempotent *within* a run, append-only *across* runs.
                    # Clearing this run's own rows for this file first means a
                    # retry — or any re-execution of the task inside the same
                    # dag run — restates its batch instead of doubling it.
                    # Without this, an observed double execution put 4824 rows
                    # in raw_orders from one 2412-row file. Earlier runs are
                    # untouched, so the audit history is still complete.
                    con.execute(
                        f"DELETE FROM {table} WHERE _run_id = ? AND _source_file = ?",
                        [context["run_id"], os.path.basename(path)])
                    con.execute(f"INSERT INTO {table} {src}")
                    n = con.execute(
                        "SELECT count(*) FROM read_csv("
                        f"'{path}', all_varchar=true, header=true)").fetchone()[0]
                    total += n
                    print(f"[raw] {table} <- {os.path.basename(path)}: {n} rows")

                grand = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                runs = con.execute(
                    f"SELECT count(DISTINCT _run_id) FROM {table}").fetchone()[0]
                print(f"[raw] {table} now holds {grand} rows across {runs} run(s) "
                      f"— append-only by design")
            finally:
                con.close()
            return {"dataset": dataset, "rows": total, "files": len(paths)}
        return _inner()

    # ── STAGING — SQL only, one task per table ───────────────────────────────

    @task(pool="duckdb_writer")
    def stage_masters(raw_customers: dict, raw_products: dict,
                      raw_stores: dict, **context) -> dict:
        """Masters first: stg_orders checks its foreign keys against them."""
        params = _context_params(context)
        print(f"[staging] master-data DQ policy severity: {params['master_sev']} "
              f"(Airflow Variable {POLICY_VAR})")
        con = _connect()
        try:
            counts = {}
            for name in ("stg_customers", "stg_products", "stg_stores"):
                _run_sql(con, name, **params)
                counts[name] = con.execute(
                    f"SELECT count(*) FROM {name}").fetchone()[0]
                breakdown = con.execute(
                    f"SELECT dq_status, count(*) FROM {name} "
                    "GROUP BY 1 ORDER BY 2 DESC").fetchall()
                print(f"[staging] {name}: {counts[name]} rows {breakdown}")
        finally:
            con.close()
        return counts

    @task(pool="duckdb_writer")
    def stage_orders(masters: dict, raw_orders: dict, **context) -> dict:
        """The upsert. This is criteria 5 and 6."""
        params = _context_params(context)
        con = _connect()
        try:
            before = con.execute(
                "SELECT count(*) FROM stg_orders"
            ).fetchone()[0] if con.execute(
                "SELECT count(*) FROM duckdb_tables() "
                "WHERE table_name='stg_orders'").fetchone()[0] else 0

            _run_sql(con, "stg_orders", **params)

            after = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
            quarantined = con.execute(
                "SELECT count(*) FROM stg_orders WHERE dq_status='QUARANTINED'"
            ).fetchone()[0]
            superseded = con.execute(
                "SELECT count(*) FROM stg_orders_quarantine "
                "WHERE failure_reason LIKE '%order_id_unique%'").fetchone()[0]
            raw_rows = con.execute("SELECT count(*) FROM raw_orders").fetchone()[0]
            print(f"[staging] stg_orders: {before} -> {after} rows "
                  f"(+{after - before}) from {raw_rows} raw rows; "
                  f"{quarantined} quarantined, {superseded} superseded duplicates")
            revenue = con.execute(
                "SELECT round(sum(order_amount),2) FROM stg_orders "
                "WHERE dq_status<>'QUARANTINED' AND order_status='DELIVERED'"
            ).fetchone()[0]
            print(f"[staging] net revenue available to the mart: Rs {revenue:,.2f}")
        finally:
            con.close()
        return {"stg_orders": after, "quarantined": quarantined,
                "superseded": superseded}

    @task(pool="duckdb_writer")
    def stage_payments(orders: dict, raw_payments: dict, **context) -> dict:
        """After orders, because its consistency checks join to stg_orders."""
        params = _context_params(context)
        con = _connect()
        try:
            _run_sql(con, "stg_payments", **params)
            # Counted from raw, because staging keeps only the surviving copy.
            _run_sql(con, "stg_dedup_stats", **params)
            dedup = con.execute(
                "SELECT dataset, superseded FROM stg_dedup_stats "
                "WHERE superseded > 0 ORDER BY superseded DESC").fetchall()
            print(f"[staging] superseded duplicate copies by dataset: {dedup}")
            n = con.execute("SELECT count(*) FROM stg_payments").fetchone()[0]
            breakdown = con.execute(
                "SELECT dq_status, count(*) FROM stg_payments "
                "GROUP BY 1 ORDER BY 2 DESC").fetchall()
            print(f"[staging] stg_payments: {n} rows {breakdown}")
        finally:
            con.close()
        return {"stg_payments": n}

    # ── DATA QUALITY SCORECARD ───────────────────────────────────────────────

    @task(pool="duckdb_writer")
    def record_dq_results(payments: dict, **context) -> int:
        params = _context_params(context)
        con = _connect()
        try:
            _run_sql(con, "dq_results", **params)
            unmapped = con.execute(
                "SELECT check_name FROM dq_results "
                "WHERE dag_id = ? AND run_id = ? AND dimension = 'Unmapped'",
                [DAG_ID, context["run_id"]]).fetchall()
            if unmapped:
                # The staging SQL recorded a check the registry does not declare.
                # Failing here is the point: a silent drift between the two lists
                # would quietly stop a dimension being reported on.
                raise ValueError(
                    "checks missing from the dq_results registry: "
                    f"{[u[0] for u in unmapped]}")

            summary = con.execute("""
                SELECT dimension,
                       count(*)                             AS checks,
                       count(*) FILTER (WHERE status='FAIL') AS failing,
                       sum(rows_failed)                     AS rows_failed
                FROM dq_results WHERE dag_id = ? AND run_id = ?
                GROUP BY dimension ORDER BY dimension
            """, [DAG_ID, context["run_id"]]).fetchdf()
            total = con.execute(
                "SELECT count(*) FROM dq_results WHERE dag_id = ? AND run_id = ?",
                [DAG_ID, context["run_id"]]).fetchone()[0]
            print(f"[dq] {total} checks recorded\n{summary.to_string(index=False)}")
        finally:
            con.close()
        return total

    # ── MART — SQL only, one task per table ──────────────────────────────────

    def _mart(name: str):
        @task(task_id=f"build_{name}", pool="duckdb_writer")
        def _inner(_upstream, **context) -> dict:
            params = _context_params(context)
            con = _connect()
            try:
                _run_sql(con, name, **params)
                n = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
                print(f"[mart] {name}: {n} rows")
            finally:
                con.close()
            return {"table": name, "rows": n}
        return _inner

    # ── REPORT ───────────────────────────────────────────────────────────────

    @task(pool="duckdb_writer")
    def report(_marts: list) -> dict:
        con = _connect()
        try:
            zone = con.execute("""
                SELECT zone, sum(delivered_orders) AS delivered_orders,
                       round(sum(net_revenue),2)   AS net_revenue
                FROM mart_daily_store_kpis GROUP BY zone ORDER BY net_revenue DESC
            """).fetchdf()
            margin = con.execute(
                "SELECT category, delivered_orders, net_revenue, gross_margin, "
                "margin_pct FROM mart_category_margin").fetchdf()
            recon = con.execute("""
                SELECT reason, count(*) AS orders, round(sum(variance),2) AS variance
                FROM mart_payment_reconciliation GROUP BY reason ORDER BY orders DESC
            """).fetchdf()
            rejected = con.execute("""
                SELECT split_part(check_name,'.',1) AS dataset, severity,
                       count(*) AS checks_failing, sum(rows_failed) AS rows_failed
                FROM dq_results WHERE status='FAIL'
                GROUP BY 1,2 ORDER BY rows_failed DESC
            """).fetchdf()
            totals = con.execute("""
                SELECT round(sum(net_revenue),2) AS net_revenue,
                       sum(delivered_orders)     AS delivered_orders
                FROM mart_daily_store_kpis
            """).fetchone()

            print(f"\n[report] Net revenue by zone:\n{zone.to_string(index=False)}")
            print(f"\n[report] Margin by category:\n{margin.to_string(index=False)}")
            print(f"\n[report] Reconciliation:\n{recon.to_string(index=False)}")
            print(f"\n[report] Rejections by file:\n{rejected.to_string(index=False)}")
            print(f"\n[report] MART TOTAL net revenue Rs {totals[0]:,.2f} "
                  f"over {totals[1]} delivered orders "
                  f"— compare with the ETL's figure for the same files")
        finally:
            con.close()
        return {"net_revenue": totals[0], "delivered_orders": totals[1]}

    # ── RUN AUDIT ────────────────────────────────────────────────────────────

    # retries=0: this task fails as a FUNCTION of the run's outcome, so a
    # retry can never succeed. Observed after adding the raise below — it
    # retried to try_number 3, burning two minutes and delaying the only
    # failure signal the client gets by two minutes on a 09:00 SLA.
    @task(trigger_rule="all_done", pool="duckdb_writer", retries=0)
    def run_audit(raws: list, orders: dict, totals: dict, **context) -> None:
        dag_run = context["dag_run"]
        # See the ETL DAG's run_audit: Airflow 3's runtime DagRun has no ORM
        # access, so run health is inferred from the upstream XComs. A failed
        # upstream task leaves its argument as None under trigger_rule="all_done".
        stages = [("load_raw", raws and all(raws)), ("stage_orders", orders),
                  ("report", totals)]
        missing = [name for name, value in stages if not value]
        status = "FAILED" if missing else "SUCCESS"
        if missing:
            print(f"[audit] stages that produced no result: {missing}")

        con = duckdb.connect(WAREHOUSE)
        try:
            con.execute(PIPELINE_RUNS_DDL)
            write_pipeline_run(
                con,
                run_id=context["run_id"],
                dag_id=DAG_ID,
                start_time=str(dag_run.start_date),
                end_time=pendulum.now("UTC").to_iso8601_string(),
                rows_extracted=sum((r or {}).get("rows", 0) for r in (raws or [])),
                rows_loaded=(orders or {}).get("stg_orders", 0),
                rows_quarantined=((orders or {}).get("quarantined", 0)
                                  + (orders or {}).get("superseded", 0)),
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

        # Write the audit row first, then fail with the run. See the ETL DAG's
        # run_audit for the two measured defects this fixes: the false-green
        # dag run (experiments 4, 6, 7) and the stale audit row left behind by
        # `tasks clear --only-failed` after a recovery (experiment 10).
        if status == "FAILED":
            from airflow.exceptions import AirflowException
            raise AirflowException(
                f"pipeline run recorded as FAILED (stages with no result: "
                f"{missing}); failing run_audit so the dag run is not "
                f"reported green")

    # ── WIRING ───────────────────────────────────────────────────────────────
    #
    #  load_raw_customers ─┐
    #  load_raw_products   ─┼─► stage_masters ─┐
    #  load_raw_stores     ─┘                  │
    #  load_raw_orders   ───────────────────────┼─► stage_orders ─► stage_payments
    #  load_raw_payments ───────────────────────┘                        │
    #                                                      record_dq_results
    #                                                                   │
    #            ┌──────────────────────────────────────────────────────┤
    #            ▼                                                      │
    #  build_mart_daily_store_kpis / mart_category_margin /             │
    #  mart_delivery_sla / mart_payment_reconciliation /                │
    #  mart_top_customers  ──► report ──► run_audit (all_done)          │

    raw = {name: _load_raw(name, patterns) for name, patterns in SOURCES.items()}

    masters = stage_masters(raw["customers"], raw["products"], raw["stores"])
    orders = stage_orders(masters, raw["orders"])
    payments = stage_payments(orders, raw["payments"])
    dq = record_dq_results(payments)

    marts = [
        _mart("mart_daily_store_kpis")(dq),
        _mart("mart_category_margin")(dq),
        _mart("mart_delivery_sla")(dq),
        _mart("mart_payment_reconciliation")(dq),
        _mart("mart_top_customers")(dq),
    ]

    totals = report(marts)
    run_audit(list(raw.values()), orders, totals)


nammamart_elt()
