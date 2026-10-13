"""
tests/test_nammamart_elt_local.py
---------------------------------
Runs the real ELT DAG body against the real CSVs, outside Airflow, on a
throwaway warehouse — then runs the ETL DAG over the same files and asserts
the two agree.

What it asserts:
  * criterion 3 — raw tables hold every source row, uncleaned, and GROW on
                  re-run (that is correct for an audit layer)
  * criterion 4 — rejected rows are set aside with a reason
  * criterion 5 — re-running does not move the mart totals
  * criterion 6 — the increment file restates 10 and inserts 150 in staging
  * criterion 8 — no unmasked email or phone anywhere outside raw_customers
  * Part B      — mart net revenue equals the ETL fact table's net revenue
  * Part D      — dq_results holds all six dimensions, with PASS rows too

Run:
    .venv/bin/python -I tests/test_nammamart_elt_local.py
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

_captured = {}
# Each Airflow dag run gets its own run_id. The harness must model that:
# re-running with the SAME id means "re-execute this run", which the raw
# loader deliberately restates rather than appends.
_RUN = {"id": "elt_local_run_1", "seq": 1}


class _FakeTI:
    task_id = "run_audit"
    state = "running"


class _FakeDagRun:
    start_date = pd.Timestamp("2026-10-13T03:30:00Z")

    def get_task_instances(self):
        return [_FakeTI()]


def _fake_context():
    return {"run_id": _RUN["id"], "task_instance": _FakeTI(), "dag_run": _FakeDagRun()}


def _new_run():
    """Advance to a fresh dag run, as the scheduler would."""
    _RUN["seq"] += 1
    _RUN["id"] = f"elt_local_run_{_RUN['seq']}"
    return _RUN["id"]


def _task_stub(*dargs, **dkwargs):
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
        _captured.setdefault("bodies", {})[kwargs["dag_id"]] = fn
        _captured.setdefault("kwargs", {})[kwargs["dag_id"]] = kwargs
        return lambda *a, **k: None
    return deco


decorators = types.ModuleType("airflow.decorators")
decorators.task = _task_stub
decorators.dag = _dag_stub
airflow = types.ModuleType("airflow")
airflow.decorators = decorators
sys.modules.setdefault("airflow", airflow)
sys.modules.setdefault("airflow.decorators", decorators)

sys.path.insert(0, os.path.join(REPO, "dags"))
import nammamart_elt_arnav_ganguly as elt   # noqa: E402
import nammamart_etl_arnav_ganguly as etl   # noqa: E402

ELT_DAG = "nammamart_elt_arnav_ganguly"
ETL_DAG = "nammamart_etl_arnav_ganguly"


def _q(warehouse, sql, params=None):
    con = duckdb.connect(warehouse, read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


def _one(warehouse, sql):
    return _q(warehouse, sql)[0][0]


def main():
    bodies = _captured["bodies"]
    assert ELT_DAG in bodies and ETL_DAG in bodies, sorted(bodies)

    kw = _captured["kwargs"][ELT_DAG]
    assert kw["schedule"] == "0 9 * * 1-6", kw["schedule"]
    assert kw["catchup"] is False
    assert kw["start_date"].timezone.name == "Asia/Kolkata"
    print(f"Part C OK — ELT schedule {kw['schedule']} "
          f"tz={kw['start_date'].timezone.name} catchup={kw['catchup']}")

    tmp = tempfile.mkdtemp(prefix="nammamart-elt-")
    data = os.path.join(tmp, "data")
    os.makedirs(data)
    # the pristine dataset, not include/data/nammamart — that is the mutable
    # landing zone the Part F experiments rename files in
    src = os.path.join(REPO, "assignment", "nammamart_dataset")
    base_files = ["orders.csv", "payments.csv", "customers.csv",
                  "products.csv", "stores.csv"]
    for name in base_files:
        shutil.copy(os.path.join(src, name), data)

    wh = os.path.join(tmp, "warehouse_test.duckdb")
    elt.DATA_DIR = data
    elt.SQL_DIR = os.path.join(REPO, "include", "sql")
    elt.WAREHOUSE = wh

    print("\n" + "=" * 72 + "\nELT RUN 1 — baseline\n" + "=" * 72)
    bodies[ELT_DAG]()

    # criterion 3: raw holds every row of every file, uncleaned
    raw_counts = {t: _one(wh, f"SELECT count(*) FROM raw_{t}")
                  for t in ["orders", "payments", "customers", "products", "stores"]}
    expected = {"orders": 2412, "payments": 2228, "customers": 405,
                "products": 152, "stores": 15}
    assert raw_counts == expected, f"{raw_counts} != {expected}"
    # the dirty values must still be there, as text
    assert _one(wh, "SELECT count(*) FROM raw_orders WHERE order_status='Pending??'") == 3
    assert _one(wh, "SELECT count(*) FROM raw_orders WHERE quantity='-3'") == 2
    print(f"criterion 3 OK — raw holds {raw_counts}, 'Pending??' and '-3' intact")

    for col in ["_loaded_at", "_source_file", "_run_id"]:
        assert _one(wh, "SELECT count(*) FROM duckdb_columns() "
                        f"WHERE table_name='raw_orders' AND column_name='{col}'") == 1
    print("           — _loaded_at / _source_file / _run_id present on raw")

    mart_revenue = _one(wh, "SELECT round(sum(net_revenue),2) FROM mart_daily_store_kpis")
    stg_orders = _one(wh, "SELECT count(*) FROM stg_orders")
    quarantined = _one(wh, "SELECT count(*) FROM stg_orders WHERE dq_status='QUARANTINED'")
    print(f"\nstg_orders={stg_orders} quarantined={quarantined} "
          f"mart_revenue={mart_revenue}")

    # criterion 4
    sidelined = _one(wh, "SELECT count(*) FROM stg_orders_quarantine")
    no_reason = _one(wh, "SELECT count(*) FROM stg_orders_quarantine "
                         "WHERE failure_reason IS NULL OR failure_reason=''")
    assert sidelined > 0 and no_reason == 0
    print(f"criterion 4 OK — {sidelined} rows set aside, all with a reason")

    # Part D: all six dimensions, and PASS rows recorded as well as FAIL
    dims = {r[0] for r in _q(wh, "SELECT DISTINCT dimension FROM dq_results "
                                 "WHERE dag_id=?", [ELT_DAG])}
    need = {"Completeness", "Uniqueness", "Validity", "Accuracy",
            "Consistency", "Timeliness"}
    assert need <= dims, f"missing {need - dims}"
    assert "Unmapped" not in dims, "a staging check is missing from the registry"
    n_pass = _one(wh, f"SELECT count(*) FROM dq_results "
                      f"WHERE dag_id='{ELT_DAG}' AND status='PASS'")
    n_fail = _one(wh, f"SELECT count(*) FROM dq_results "
                      f"WHERE dag_id='{ELT_DAG}' AND status='FAIL'")
    assert n_pass > 0 and n_fail > 0
    print(f"Part D OK — {n_pass} PASS + {n_fail} FAIL across {sorted(dims)}")

    # criterion 8: no unmasked PII outside raw
    leaks = _one(wh, "SELECT count(*) FROM stg_customers "
                     "WHERE email_masked NOT LIKE '%***%'")
    assert leaks == 0, f"{leaks} unmasked emails in stg_customers"
    assert _one(wh, "SELECT count(*) FROM duckdb_columns() WHERE table_name='stg_customers' "
                    "AND column_name IN ('email','phone','full_name')") == 0
    top = _q(wh, "SELECT customer_name, email_masked, phone_masked "
                 "FROM mart_top_customers LIMIT 3")
    for name, email, phone in top:
        assert "***" in email and "*" in phone, (name, email, phone)
    print(f"criterion 8 OK — masked in staging and mart, e.g. {top[0]}")

    # criterion 5: raw grows, mart does not move
    print("\n" + "=" * 72 + "\nELT RUN 2 — identical re-run\n" + "=" * 72)
    _new_run()
    bodies[ELT_DAG]()
    raw_after = _one(wh, "SELECT count(*) FROM raw_orders")
    stg_after = _one(wh, "SELECT count(*) FROM stg_orders")
    mart_after = _one(wh, "SELECT round(sum(net_revenue),2) FROM mart_daily_store_kpis")
    assert raw_after == 2 * 2412, f"raw_orders should append: {raw_after}"
    assert stg_after == stg_orders, f"staging moved {stg_orders} -> {stg_after}"
    assert mart_after == mart_revenue, f"mart moved {mart_revenue} -> {mart_after}"
    per_run = _one(wh, f"SELECT count(*) FROM dq_results "
                       f"WHERE dag_id='{ELT_DAG}' AND run_id='{_RUN['id']}'")
    assert per_run == n_pass + n_fail, \
        f"this run recorded {per_run} checks, expected {n_pass + n_fail}"
    print(f"criterion 5 OK — raw_orders grew 2412 -> {raw_after} (audit layer, "
          f"correct); staging stayed {stg_after}; mart stayed Rs {mart_after:,.2f}")

    # criterion 6: increment
    print("\n" + "=" * 72 + "\nELT RUN 3 — increment file\n" + "=" * 72)
    shutil.copy(os.path.join(src, "orders_2026-10-01_increment.csv"), data)
    inc = pd.read_csv(os.path.join(src, "orders_2026-10-01_increment.csv"), dtype=str)
    base_ids = set(pd.read_csv(os.path.join(src, "orders.csv"), dtype=str)["order_id"])
    corrections = sorted(set(inc["order_id"]) & base_ids)
    new_ids = sorted(set(inc["order_id"]) - base_ids)
    assert (len(corrections), len(new_ids)) == (10, 150)

    before = dict(_q(wh, "SELECT order_id, order_status FROM stg_orders WHERE order_id IN ("
                         + ",".join(f"'{o}'" for o in corrections) + ")"))
    _new_run()
    bodies[ELT_DAG]()
    after = dict(_q(wh, "SELECT order_id, order_status FROM stg_orders WHERE order_id IN ("
                        + ",".join(f"'{o}'" for o in corrections) + ")"))
    stg_inc = _one(wh, "SELECT count(*) FROM stg_orders")
    for oid in corrections:
        n = _one(wh, f"SELECT count(*) FROM stg_orders WHERE order_id='{oid}'")
        assert n == 1, f"{oid} appears {n} times — the upsert duplicated it"
    flipped = {k: (before[k], after[k]) for k in before if before[k] != after[k]}
    assert stg_inc == stg_orders + 150, \
        f"expected +150 staged rows, got {stg_inc - stg_orders}"
    assert len(flipped) == 10, f"expected 10 status corrections, got {len(flipped)}"
    print(f"criterion 6 OK — staging {stg_orders} -> {stg_inc} (+150 inserted), "
          f"{len(flipped)} restated in place, 0 duplicated")
    print(f"           e.g. {list(flipped.items())[0]}")

    # ── Part B: the two pipelines must agree ─────────────────────────────────
    print("\n" + "=" * 72 + "\nETL over the same files, for comparison\n" + "=" * 72)
    etl_tmp = tempfile.mkdtemp(prefix="nammamart-etl-cmp-")
    etl.DATA_DIR = data
    etl.WAREHOUSE = os.path.join(etl_tmp, "warehouse_etl.duckdb")
    etl.QUARANTINE_DIR = os.path.join(etl_tmp, "quarantine")
    etl.STAGING_DIR = os.path.join(etl_tmp, "staging")
    _new_run()
    bodies[ETL_DAG]()

    etl_wh = etl.WAREHOUSE
    etl_rev = _one(etl_wh, "SELECT round(sum(net_revenue),2) FROM etl_fact_orders")
    etl_rows = _one(etl_wh, "SELECT count(*) FROM etl_fact_orders")
    etl_delivered = _one(etl_wh, "SELECT count(*) FROM etl_fact_orders WHERE is_delivered")

    elt_rev = _one(wh, "SELECT round(sum(net_revenue),2) FROM mart_daily_store_kpis")
    elt_rows = _one(wh, "SELECT count(*) FROM stg_orders WHERE dq_status<>'QUARANTINED'")
    elt_delivered = _one(wh, "SELECT sum(delivered_orders) FROM mart_daily_store_kpis")

    print(f"\n{'':22s} {'ETL':>16s} {'ELT':>16s}")
    print(f"{'rows in fact/mart':22s} {etl_rows:>16d} {elt_rows:>16d}")
    print(f"{'delivered orders':22s} {etl_delivered:>16d} {elt_delivered:>16d}")
    print(f"{'net revenue':22s} {etl_rev:>16,.2f} {elt_rev:>16,.2f}")

    assert etl_rows == elt_rows, f"row counts differ: ETL {etl_rows} vs ELT {elt_rows}"
    assert etl_delivered == elt_delivered, "delivered order counts differ"
    assert abs(etl_rev - elt_rev) < 0.01, \
        f"net revenue differs: ETL {etl_rev} vs ELT {elt_rev}"
    print("\nPart B OK — the two pipelines agree to the paisa")

    # category margin should agree too
    etl_cat = dict(_q(etl_wh, "SELECT category, net_revenue FROM etl_category_margin"))
    elt_cat = dict(_q(wh, "SELECT category, net_revenue FROM mart_category_margin"))
    assert etl_cat.keys() == elt_cat.keys(), (sorted(etl_cat), sorted(elt_cat))
    for cat in etl_cat:
        assert abs(etl_cat[cat] - elt_cat[cat]) < 0.01, \
            f"{cat}: ETL {etl_cat[cat]} vs ELT {elt_cat[cat]}"
    print(f"           — all {len(etl_cat)} category margins agree")

    print("\n" + "=" * 72)
    print("all ELT acceptance checks passed")
    print("=" * 72)
    shutil.rmtree(tmp, ignore_errors=True)
    shutil.rmtree(etl_tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
