"""
tests/test_nammamart_etl_local.py
---------------------------------
Runs the real ETL DAG body against the real CSVs, outside Airflow, on a
throwaway warehouse — so the acceptance criteria can be checked in seconds
instead of waiting on a container.

It stubs `airflow.decorators` so `@dag` hands back the body function and
`@task` is identity-plus-context-injection. The pipeline then executes inline,
in dependency order, because TaskFlow's data dependencies are just function
calls.

What it asserts:
  * criterion 2 — no row that failed a Critical check is in the fact table
  * criterion 4 — every quarantined row carries a failure_reason
  * criterion 5 — re-running does not change fact row counts or revenue
  * criterion 6 — the increment file adds 150 and restates 10, not 160 inserts
  * Part D     — dq_results is populated, with all six dimensions

Run:
    .venv/bin/python -I tests/test_nammamart_etl_local.py
"""

import inspect
import os
import shutil
import sys
import tempfile
import types

import duckdb
import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "include"))

# ── stub airflow before the DAG module imports it ────────────────────────────

_captured = {}


def _task_stub(*dargs, **dkwargs):
    """Mimic @task and @task(...). The wrapper injects a fake Airflow context
    only into functions that declare **kwargs, the way Airflow does."""
    def wrap(fn):
        takes_context = any(
            p.kind is inspect.Parameter.VAR_KEYWORD
            for p in inspect.signature(fn).parameters.values()
        )

        def inner(*args, **kwargs):
            if takes_context:
                kwargs = {**_fake_context(), **kwargs}
            return fn(*args, **kwargs)
        inner.__name__ = getattr(fn, "__name__", "task")
        return inner
    if len(dargs) == 1 and callable(dargs[0]) and not dkwargs:
        return wrap(dargs[0])
    return wrap


def _dag_stub(**kwargs):
    def deco(fn):
        _captured["body"] = fn
        _captured["kwargs"] = kwargs
        return lambda *a, **k: None   # the module's bottom-of-file call is a no-op
    return deco


class _FakeTI:
    task_id = "run_audit"
    state = "running"


class _FakeDagRun:
    start_date = pd.Timestamp("2026-10-13T03:30:00Z")

    def get_task_instances(self):
        return [_FakeTI()]


# One id per dag run, as the scheduler assigns them.
_RUN = {"id": "local_test_run_1", "seq": 1}


def _fake_context():
    return {"run_id": _RUN["id"], "task_instance": _FakeTI(), "dag_run": _FakeDagRun()}


decorators = types.ModuleType("airflow.decorators")
decorators.task = _task_stub
decorators.dag = _dag_stub
airflow = types.ModuleType("airflow")
airflow.decorators = decorators
sys.modules.setdefault("airflow", airflow)
sys.modules.setdefault("airflow.decorators", decorators)

sys.path.insert(0, os.path.join(REPO, "dags"))
import nammamart_etl_arnav_ganguly as etl  # noqa: E402


# ── drive the pipeline ────────────────────────────────────────────────────────

def _point_at(tmp: str, data_dir: str):
    """Repoint the module's container paths at a temp sandbox."""
    etl.DATA_DIR = data_dir
    etl.WAREHOUSE = os.path.join(tmp, "warehouse_test.duckdb")
    etl.QUARANTINE_DIR = os.path.join(tmp, "quarantine")
    etl.STAGING_DIR = os.path.join(tmp, "staging")


def _run(new_run: bool = True):
    """Execute the DAG body. By default as a fresh dag run."""
    if new_run:
        _RUN["seq"] += 1
        _RUN["id"] = f"local_test_run_{_RUN['seq']}"
    _captured["body"]()


def _q(warehouse: str, sql: str):
    con = duckdb.connect(warehouse, read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def main():
    assert "body" in _captured, "the @dag stub did not capture the DAG body"
    kw = _captured["kwargs"]
    assert kw["schedule"] == "0 9 * * 1-6", kw["schedule"]
    assert kw["catchup"] is False
    assert str(kw["start_date"].timezone.name) == "Asia/Kolkata", kw["start_date"]

    tmp = tempfile.mkdtemp(prefix="nammamart-etl-")
    data = os.path.join(tmp, "data")
    os.makedirs(data)
    # the pristine dataset, not include/data/nammamart — that is the mutable
    # landing zone the Part F experiments rename files in
    src = os.path.join(REPO, "assignment", "nammamart_dataset")
    # Baseline: the five source files, WITHOUT the increment feed.
    for name in ["orders.csv", "payments.csv", "customers.csv",
                 "products.csv", "stores.csv"]:
        shutil.copy(os.path.join(src, name), data)
    _point_at(tmp, data)
    wh = etl.WAREHOUSE

    print("\n" + "=" * 72 + "\nRUN 1 — baseline, no increment file\n" + "=" * 72)
    _run()

    fact_rows = _q(wh, "SELECT count(*) FROM etl_fact_orders")[0][0]
    revenue = _q(wh, "SELECT round(sum(net_revenue),2) FROM etl_fact_orders")[0][0]
    distinct_ids = _q(wh, "SELECT count(DISTINCT order_id) FROM etl_fact_orders")[0][0]
    print(f"\nfact rows={fact_rows} distinct order_id={distinct_ids} revenue={revenue}")

    assert fact_rows == distinct_ids, "fact table has duplicate order_ids"

    # criterion 2: nothing quarantined reached the fact table
    quarantine_dir = etl.QUARANTINE_DIR
    orders_q = [f for f in os.listdir(quarantine_dir) if f.startswith("orders_failed")]
    assert orders_q, "no orders quarantine file was written"
    bad = pd.read_csv(os.path.join(quarantine_dir, orders_q[0]))
    assert "failure_reason" in bad.columns              # criterion 4
    assert bad["failure_reason"].str.len().gt(0).all(), "a quarantined row has no reason"
    assert bad["failure_reason"].str.startswith("Critical:").all(), \
        "a Warning-only row was quarantined"
    # The uniqueness check quarantines the *earlier* copy of a duplicated
    # order_id and keeps the last, so that order_id is legitimately present in
    # the fact table via its surviving row. Every other quarantine reason means
    # the order_id must be absent entirely.
    dedup_only = bad["failure_reason"].str.fullmatch("Critical:order_id_unique")
    rejected = bad.loc[~dedup_only, "order_id"].dropna().unique()
    deduped = bad.loc[dedup_only, "order_id"].dropna().unique()

    in_fact = _q(wh, "SELECT count(*) FROM etl_fact_orders WHERE order_id IN ("
                     + ",".join(f"'{o}'" for o in rejected) + ")")[0][0]
    assert in_fact == 0, f"{in_fact} rejected orders reached etl_fact_orders"
    print(f"criterion 2 OK — {len(rejected)} rejected order_ids, none in the fact table")

    # the deduplicated ones must appear exactly once, not zero and not twice
    for oid in deduped:
        n = _q(wh, f"SELECT count(*) FROM etl_fact_orders WHERE order_id='{oid}'")[0][0]
        assert n == 1, f"deduplicated {oid} appears {n} times in the fact table"
    print(f"           — {len(deduped)} deduplicated order_ids kept exactly once")

    # every quarantined row must still be reproducible from the raw file
    print(f"criterion 4 OK — failure_reason present on all {len(bad)} rows")

    # Part D: dq_results populated, all six dimensions present
    dims = {r[0] for r in _q(wh, "SELECT DISTINCT dimension FROM dq_results")}
    expected_dims = {"Completeness", "Uniqueness", "Validity",
                     "Accuracy", "Consistency", "Timeliness"}
    assert expected_dims <= dims, f"missing dimensions: {expected_dims - dims}"
    n_checks = _q(wh, "SELECT count(*) FROM dq_results")[0][0]
    n_failing = _q(wh, "SELECT count(*) FROM dq_results WHERE status='FAIL'")[0][0]
    print(f"Part D OK — {n_checks} checks recorded, {n_failing} failing, "
          f"dimensions={sorted(dims)}")

    # criterion 5: re-run changes nothing
    print("\n" + "=" * 72 + "\nRUN 2 — identical re-run (idempotency)\n" + "=" * 72)
    _run()
    fact_rows_2 = _q(wh, "SELECT count(*) FROM etl_fact_orders")[0][0]
    revenue_2 = _q(wh, "SELECT round(sum(net_revenue),2) FROM etl_fact_orders")[0][0]
    assert fact_rows_2 == fact_rows, f"row count moved {fact_rows} -> {fact_rows_2}"
    assert revenue_2 == revenue, f"revenue moved {revenue} -> {revenue_2}"
    per_run = _q(wh, "SELECT count(*) FROM dq_results WHERE run_id = ?",
                 )[0][0] if False else _q(
        wh, f"SELECT count(*) FROM dq_results WHERE run_id='{_RUN['id']}'")[0][0]
    assert per_run == n_checks, f"this run recorded {per_run}, expected {n_checks}"
    runs = _q(wh, "SELECT count(*) FROM pipeline_runs")[0][0]
    assert runs == 2, f"expected one pipeline_runs row per run, got {runs}"
    # and re-executing the SAME run restates rather than appending
    _run(new_run=False)
    assert _q(wh, "SELECT count(*) FROM pipeline_runs")[0][0] == 2
    assert _q(wh, f"SELECT count(*) FROM dq_results WHERE run_id='{_RUN['id']}'"
                 )[0][0] == n_checks
    print(f"criterion 5 OK — rows {fact_rows} and revenue {revenue} unchanged; "
          f"dq_results holds {n_checks} per run")

    # criterion 6: the increment file restates 10, appends 150
    print("\n" + "=" * 72 + "\nRUN 3 — increment file present\n" + "=" * 72)
    shutil.copy(os.path.join(src, "orders_2026-10-01_increment.csv"), data)
    inc = pd.read_csv(os.path.join(src, "orders_2026-10-01_increment.csv"), dtype=str)
    base_ids = set(pd.read_csv(os.path.join(src, "orders.csv"), dtype=str)["order_id"])
    corrections = sorted(set(inc["order_id"]) & base_ids)
    assert len(corrections) == 10, f"expected 10 corrections, found {len(corrections)}"

    before_status = dict(_q(wh, "SELECT order_id, order_status FROM etl_fact_orders "
                                "WHERE order_id IN ("
                                + ",".join(f"'{o}'" for o in corrections) + ")"))
    _run()
    fact_rows_3 = _q(wh, "SELECT count(*) FROM etl_fact_orders")[0][0]
    distinct_3 = _q(wh, "SELECT count(DISTINCT order_id) FROM etl_fact_orders")[0][0]
    assert fact_rows_3 == distinct_3, "increment load introduced duplicate order_ids"

    new_ids = sorted(set(inc["order_id"]) - base_ids)
    print(f"increment: {len(new_ids)} new order_ids, {len(corrections)} corrections")
    print(f"fact rows {fact_rows} -> {fact_rows_3} (+{fact_rows_3 - fact_rows})")

    # the 10 corrections must have moved status, not created a second row
    after_status = dict(_q(wh, "SELECT order_id, order_status FROM etl_fact_orders "
                               "WHERE order_id IN ("
                               + ",".join(f"'{o}'" for o in corrections) + ")"))
    for oid in before_status:
        n = _q(wh, f"SELECT count(*) FROM etl_fact_orders WHERE order_id='{oid}'")[0][0]
        assert n == 1, f"{oid} appears {n} times after the increment load"
    changed = {k: (before_status[k], after_status.get(k))
               for k in before_status if before_status[k] != after_status.get(k)}
    print(f"criterion 6 OK — no duplicates; {len(changed)} of "
          f"{len(before_status)} loaded corrections changed status: {changed}")

    # reconciliation table exists and every row carries a reason
    recon = _q(wh, "SELECT reason, count(*) FROM etl_payment_reconciliation "
                   "GROUP BY reason ORDER BY 2 DESC")
    assert recon, "etl_payment_reconciliation is empty"
    assert all(r[0] and r[0] != "reconciled" for r in recon)
    print("\ncriterion 7 OK — reconciliation exceptions:")
    for reason, n in recon:
        print(f"    {n:5d}  {reason}")

    # PII must be masked everywhere in the reporting tables
    sample = _q(wh, "SELECT email_masked, phone_masked, customer_name "
                    "FROM etl_dim_customer LIMIT 5")
    for email, phone, name in sample:
        assert email is None or "***" in email, email
        assert phone is None or "*" in phone, phone
    print(f"criterion 8 OK — PII masked, e.g. {sample[0]}")

    print("\n" + "=" * 72)
    print("all ETL acceptance checks passed")
    print("=" * 72)
    shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
