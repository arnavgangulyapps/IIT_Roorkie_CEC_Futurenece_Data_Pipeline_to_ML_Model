#!/usr/bin/env python3
"""
scripts/part_f_experiments.py
-----------------------------
Part F — the 10 observability experiments, run against both DAGs in real
Airflow, with the data directory mutated and restored around each one.

Scripted rather than done by hand for three reasons: 20 runs is too many to do
reliably by clicking, every experiment needs its source files put back
afterwards (the brief says "restore the original file after each one"), and an
experiment nobody can re-run is not evidence.

Observations come from three places, none of them a screenshot:
  * the Airflow metadata database  — task states, try_number, durations,
                                     and which tasks genuinely overlapped
  * the DuckDB warehouse           — row counts, revenue, dq_results
  * the task logs                  — the error text for schema failures

Usage:
    .venv/bin/python -I scripts/part_f_experiments.py              # all
    .venv/bin/python -I scripts/part_f_experiments.py 1 2 3        # a subset
    .venv/bin/python -I scripts/part_f_experiments.py --list
"""

import csv
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timedelta

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PRISTINE = os.path.join(REPO, "assignment", "nammamart_dataset")
DATA = os.path.join(REPO, "include", "data", "nammamart")
WAREHOUSE = os.path.join(REPO, "include", "warehouse_arnav_ganguly.duckdb")
QUARANTINE = os.path.join(REPO, "include", "quarantine", "arnav_ganguly")
RESULTS = os.path.join(REPO, "include", "part_f_results.json")

ASTRO = os.path.expanduser("~/.local/bin/astro")
ETL = "nammamart_etl_arnav_ganguly"
ELT = "nammamart_elt_arnav_ganguly"

BASE_FILES = ["orders.csv", "payments.csv", "customers.csv",
              "products.csv", "stores.csv"]
INCREMENT = "orders_2026-10-01_increment.csv"


# ── plumbing ──────────────────────────────────────────────────────────────────

def sh(cmd, timeout=900):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                          timeout=timeout, cwd=REPO)


def psql(sql):
    """Query the Airflow metadata database."""
    pg = sh("docker ps -q --filter 'name=etl-elt-dag.*postgres'").stdout.strip()
    if not pg:
        raise RuntimeError("Airflow postgres container not found — is astro running?")
    out = sh(f"docker exec {pg} psql -U postgres -d postgres -At -F'|' -c \"{sql}\"")
    rows = [line.split("|") for line in out.stdout.strip().splitlines() if line]
    return rows


def duck(sql, default=None):
    """Query the warehouse. Returns None if the table or file is absent."""
    import duckdb
    if not os.path.exists(WAREHOUSE):
        return default
    con = duckdb.connect(WAREHOUSE, read_only=True)
    try:
        return con.execute(sql).fetchall()
    except Exception:
        return default
    finally:
        con.close()


def one(sql, default=None):
    rows = duck(sql)
    return rows[0][0] if rows else default


def restore_data(include_increment=False):
    """Put the landing zone back to pristine."""
    os.makedirs(DATA, exist_ok=True)
    for name in os.listdir(DATA):
        os.remove(os.path.join(DATA, name))
    for name in BASE_FILES:
        shutil.copy(os.path.join(PRISTINE, name), DATA)
    if include_increment:
        shutil.copy(os.path.join(PRISTINE, INCREMENT), DATA)


def fresh_warehouse():
    for suffix in ("", ".wal"):
        path = WAREHOUSE + suffix
        if os.path.exists(path):
            os.remove(path)
    if os.path.isdir(QUARANTINE):
        for name in os.listdir(QUARANTINE):
            os.remove(os.path.join(QUARANTINE, name))


def run_dag(dag_id):
    """Run one DAG to completion via the SCHEDULER, returning (ok, log).

    `dags test` executes every task sequentially inside one process, which
    hides two things Part F is specifically about: which tasks genuinely run
    in parallel, and how retries are spaced in wall-clock time. So experiments
    go through `dags trigger` and the real scheduler instead.
    """
    before, _ = latest_run(dag_id)
    run_id, state = trigger_and_wait(dag_id, before)
    log = ""
    if run_id:
        # task logs live under logs/dag_id=.../run_id=...; read what we can
        res = sh(f"{ASTRO} dev run tasks states-for-dag-run {dag_id} '{run_id}'")
        log = res.stdout + res.stderr
    return state == "success", log


def latest_run(dag_id):
    """Most recently STARTED run. NULLS LAST matters: Postgres orders NULLs
    first on DESC, so a queued run that has not started yet would otherwise
    outrank the run we are measuring."""
    rows = psql(
        f"SELECT run_id, state FROM dag_run WHERE dag_id='{dag_id}' "
        "ORDER BY start_date DESC NULLS LAST LIMIT 1")
    return (rows[0][0], rows[0][1]) if rows else (None, None)


def run_state(dag_id, run_id):
    rows = psql(f"SELECT state FROM dag_run WHERE dag_id='{dag_id}' "
                f"AND run_id='{run_id}'")
    return rows[0][0] if rows else None


def wait_for_run(dag_id, run_id, timeout=900):
    """Wait for one specific run to reach a terminal state."""
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = run_state(dag_id, run_id)
        if st not in ("queued", "running", None):
            return st
        time.sleep(5)
    return "timeout"


def task_states(dag_id, run_id):
    rows = psql(
        "SELECT task_id, state, try_number, coalesce(duration,0), "
        "coalesce(start_date::text,''), coalesce(end_date::text,'') "
        f"FROM task_instance WHERE dag_id='{dag_id}' AND run_id='{run_id}'")
    return [{"task_id": r[0], "state": r[1], "try": int(r[2]),
             "secs": round(float(r[3]), 1), "start": r[4], "end": r[5]}
            for r in rows]


def summarise(tasks):
    out = {}
    for t in tasks:
        out[t["state"] or "none"] = out.get(t["state"] or "none", 0) + 1
    return out


def parallel_groups(tasks):
    """Task ids whose execution windows actually overlapped."""
    def ts(s):
        return datetime.fromisoformat(s) if s else None
    spans = [(t["task_id"], ts(t["start"]), ts(t["end"]))
             for t in tasks if t["start"] and t["end"]]
    groups = []
    for tid, s, e in spans:
        placed = False
        for g in groups:
            if any(s < ge and gs < e for _, gs, ge in g):
                g.append((tid, s, e))
                placed = True
                break
        if not placed:
            groups.append([(tid, s, e)])
    return [sorted(t[0] for t in g) for g in groups if len(g) > 1]


def dq_failing(dag_id):
    rows = duck(
        "SELECT check_name, severity, rows_failed FROM dq_results "
        f"WHERE dag_id='{dag_id}' AND status='FAIL' ORDER BY rows_failed DESC")
    return [(r[0], r[1], r[2]) for r in (rows or [])]


def warehouse_snapshot():
    return {
        "etl_fact_rows": one("SELECT count(*) FROM etl_fact_orders", 0),
        "etl_revenue": one("SELECT round(sum(net_revenue),2) FROM etl_fact_orders", 0),
        "elt_stg_rows": one("SELECT count(*) FROM stg_orders", 0),
        "elt_mart_revenue": one(
            "SELECT round(sum(net_revenue),2) FROM mart_daily_store_kpis", 0),
        "raw_orders": one("SELECT count(*) FROM raw_orders", 0),
        "elt_quarantined": one(
            "SELECT count(*) FROM stg_orders WHERE dq_status='QUARANTINED'", 0),
    }


def quarantine_counts():
    if not os.path.isdir(QUARANTINE):
        return {}
    out = {}
    for name in sorted(os.listdir(QUARANTINE)):
        dataset = name.split("_failed_")[0]
        with open(os.path.join(QUARANTINE, name)) as fh:
            out[dataset] = sum(1 for _ in fh) - 1
    return out


def grep_log(text, pattern, limit=4):
    return [m.strip() for m in re.findall(pattern, text)][:limit]


# ── data mutations ────────────────────────────────────────────────────────────

def mutate_add_bad_rows():
    """Experiment 5: five new bad rows appended to the working orders.csv."""
    path = os.path.join(DATA, "orders.csv")
    with open(path) as fh:
        header = next(csv.reader(fh))
    bad = [
        # order_id, order_ts, customer_id, store_id, product_id, quantity,
        # unit_price, discount_pct, order_amount, payment_mode, order_status,
        # channel, delivery_minutes
        ["ORD900001", "2026-09-20 10:00:00", "", "S01", "P0001", "2",
         "100.0", "0", "200.0", "UPI", "DELIVERED", "App", "12"],
        ["ORD900002", "2026-09-20 10:05:00", "C00005", "S01", "P0001", "-4",
         "100.0", "0", "-400.0", "UPI", "DELIVERED", "App", "12"],
        ["ORD900003", "2026-09-20 10:10:00", "C00005", "S99", "P0001", "1",
         "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "12"],
        ["ORD900004", "2026-09-20 10:15:00", "C00005", "S01", "P0001", "1",
         "100.0", "0", "100.0", "UPI", "SHIPPED", "App", "12"],
        ["ORD900005", "2027-09-20 10:20:00", "C00005", "S01", "P0001", "1",
         "100.0", "0", "100.0", "UPI", "DELIVERED", "App", "12"],
    ]
    with open(path, "a", newline="") as fh:
        csv.writer(fh).writerows(bad)
    return {"expected_quarantine_ids": [r[0] for r in bad], "header": header}


def mutate_remove_products():
    """Experiment 4: rename products.csv so the source is missing.

    Renamed rather than deleted, which is what the brief asks and is also how
    this failure really happens: an upstream export writes
    products.csv.tmp and never completes the move.
    """
    src = os.path.join(DATA, "products.csv")
    dst = os.path.join(DATA, "products.csv.renamed")
    os.rename(src, dst)
    return {"renamed_to": os.path.basename(dst)}


def mutate_rename_column():
    """Experiment 6: order_amount renamed to total."""
    path = os.path.join(DATA, "orders.csv")
    with open(path) as fh:
        rows = list(csv.reader(fh))
    rows[0] = ["total" if c == "order_amount" else c for c in rows[0]]
    with open(path, "w", newline="") as fh:
        csv.writer(fh).writerows(rows)


def mutate_truncate_orders(keep=50):
    """Experiment 7: only 50 orders."""
    path = os.path.join(DATA, "orders.csv")
    with open(path) as fh:
        rows = list(csv.reader(fh))
    with open(path, "w", newline="") as fh:
        csv.writer(fh).writerows(rows[:keep + 1])


def mutate_price_drift(store="S01", factor=100, consistent=False):
    """Experiment 8: unit_price x100 for one store.

    `consistent=False` is the brief's literal instruction and leaves
    order_amount untouched, so the row stops satisfying the amount formula.
    `consistent=True` scales order_amount too — a drift that passes every
    row-level check, which is the harder and more realistic case.
    """
    path = os.path.join(DATA, "orders.csv")
    with open(path) as fh:
        rows = list(csv.reader(fh))
    head, idx = rows[0], {c: i for i, c in enumerate(rows[0])}
    touched = 0
    for r in rows[1:]:
        if r[idx["store_id"]] == store:
            try:
                r[idx["unit_price"]] = str(float(r[idx["unit_price"]]) * factor)
                if consistent:
                    r[idx["order_amount"]] = str(
                        round(float(r[idx["order_amount"]]) * factor, 2))
                touched += 1
            except ValueError:
                pass
    with open(path, "w", newline="") as fh:
        csv.writer(fh).writerows(rows)
    return touched


def mutate_stale(days=30):
    """Experiment 9: shift every order_ts back so the newest is ~30 days old."""
    path = os.path.join(DATA, "orders.csv")
    with open(path) as fh:
        rows = list(csv.reader(fh))
    idx = {c: i for i, c in enumerate(rows[0])}
    parsed = []
    for r in rows[1:]:
        try:
            parsed.append(datetime.fromisoformat(r[idx["order_ts"]]))
        except ValueError:
            parsed.append(None)
    newest = max(p for p in parsed if p)
    target = datetime(2026, 10, 13) - timedelta(days=days)
    shift = newest - target
    for r, p in zip(rows[1:], parsed):
        if p:
            r[idx["order_ts"]] = (p - shift).strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "w", newline="") as fh:
        csv.writer(fh).writerows(rows)
    return shift.days


# ── the experiments ───────────────────────────────────────────────────────────

def experiment(n, name, setup, expected, both=True, fresh=True,
               restore_increment=False):
    """Run one experiment against both DAGs and collect observations."""
    print(f"\n{'=' * 78}\nEXPERIMENT {n} — {name}\n{'=' * 78}")
    if fresh:
        fresh_warehouse()
    restore_data(include_increment=restore_increment)
    extra = setup() if setup else None

    record = {"n": n, "name": name, "expected": expected, "setup": extra,
              "dags": {}}
    for dag_id in ([ETL, ELT] if both else [ETL]):
        print(f"  running {dag_id} ...", flush=True)
        ok, log = run_dag(dag_id)
        run_id, run_state = latest_run(dag_id)
        tasks = task_states(dag_id, run_id) if run_id else []
        entry = {
            "cli_ok": ok,
            "run_id": run_id,
            "run_state": run_state,
            "states": summarise(tasks),
            "max_try": max([t["try"] for t in tasks], default=0),
            "total_secs": round(sum(t["secs"] for t in tasks), 1),
            "slowest": sorted(tasks, key=lambda t: -t["secs"])[:3],
            "parallel": parallel_groups(tasks),
            "errors": grep_log(log, r"(?:Error|Exception|error)[^\"\n]{0,160}"),
            "pipeline_log": grep_log(
                log, r"\[(?:dq|validate|quarantine|load|staging|mart|report|audit)\][^\"\n]{0,150}",
                limit=40),
        }
        record["dags"][dag_id] = entry
        print(f"    run_state={run_state} states={entry['states']} "
              f"max_try={entry['max_try']} total={entry['total_secs']}s")

    record["warehouse"] = warehouse_snapshot()
    record["quarantine_files"] = quarantine_counts()
    record["dq_fail_etl"] = dq_failing(ETL)[:8]
    record["dq_fail_elt"] = dq_failing(ELT)[:8]
    print(f"  warehouse: {record['warehouse']}")
    print(f"  quarantine: {record['quarantine_files']}")
    return record


def trigger_and_wait(dag_id, previous_run_id, timeout=900):
    """Trigger a real run (scheduler-executed, unlike `dags test`) and wait."""
    import time
    sh(f"{ASTRO} dev run dags trigger {dag_id}")
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = psql(
            f"SELECT run_id, state FROM dag_run WHERE dag_id='{dag_id}' "
            "ORDER BY start_date DESC LIMIT 1")
        if rows and rows[0][0] != previous_run_id:
            if rows[0][1] not in ("queued", "running"):
                return rows[0][0], rows[0][1]
        time.sleep(5)
    return None, "timeout"


def experiment_10():
    """Recovery: fail, fix the cause, Clear the failed task, observe.

    Uses `dags trigger` rather than `dags test` because Clear only means
    something for a run the scheduler owns.
    """
    n, name = 10, "Recovery via Clear"
    expected = ("After fixing the cause and clearing only the failed task with "
                "--only-failed --downstream, the previously successful tasks "
                "keep their original try_number and start time; only the "
                "failed task and its downstream re-run.")
    print(f"\n{'=' * 78}\nEXPERIMENT {n} — {name}\n{'=' * 78}")

    fresh_warehouse()
    restore_data()
    before_run, _ = latest_run(ETL)

    # ── break it ──────────────────────────────────────────────────────────
    info = mutate_remove_products()
    print(f"  renamed products.csv -> {info['renamed_to']}; triggering a real run")
    run_id, state = trigger_and_wait(ETL, before_run)
    failed = task_states(ETL, run_id) if run_id else []
    print(f"    run_state={state} states={summarise(failed)}")

    snapshot = {t["task_id"]: (t["state"], t["try"], t["start"]) for t in failed}

    # ── fix the cause, then Clear only what failed ─────────────────────────
    restore_data()
    print("  products.csv restored; clearing failed tasks + downstream")
    clear = sh(f"{ASTRO} dev run tasks clear {ETL} -f -d -y")

    # Clear sets the affected tasks to state NULL and the run back to queued.
    # Wait for THIS run (not "the latest") to finish re-executing.
    final_state = wait_for_run(ETL, run_id)
    final = task_states(ETL, run_id)

    after = {t["task_id"]: (t["state"], t["try"], t["start"]) for t in final}
    rerun, untouched = [], []
    for task_id, (st, tr, start) in after.items():
        was = snapshot.get(task_id)
        if was and (tr > was[1] or start != was[2]):
            rerun.append(task_id)
        elif was:
            untouched.append(task_id)

    print(f"    re-ran ({len(rerun)}): {sorted(rerun)}")
    print(f"    untouched ({len(untouched)}): {sorted(untouched)}")
    print(f"    final run_state={final_state}")

    record = {
        "n": n, "name": name, "expected": expected,
        "setup": info,
        "failed_run": {"run_id": run_id, "state": state,
                       "states": summarise(failed)},
        "clear_cmd": f"airflow tasks clear {ETL} --only-failed --downstream --yes",
        "clear_out": (clear.stdout + clear.stderr)[-400:],
        "after_states": summarise(final),
        "re_ran": sorted(rerun),
        "untouched": sorted(untouched),
        "final_state": final_state,
        "warehouse": warehouse_snapshot(),
    }
    restore_data()
    return record


EXPERIMENTS = {
    1: ("Happy path",
        None,
        "Both DAGs green. 5 extracts / 5 raw loads run in parallel. "
        "2310 ETL fact rows, 2400 ELT staged, Rs 1,135,628.80 both."),
    2: ("Re-run (idempotency)",
        None,
        "Fact row counts and revenue unchanged. ELT raw tables grow "
        "(append-only audit layer); staging and mart do not."),
    3: ("Incremental load",
        None,
        "150 new orders inserted, 10 September orders restated to RETURNED "
        "with no duplicates. September revenue falls Rs 4,193.70."),
    4: ("Missing source file",
        mutate_remove_products,
        "extract_products / load_raw_products retries twice at 1-minute "
        "intervals then fails; downstream goes upstream_failed; run_audit "
        "still writes a FAILED row via trigger_rule=all_done."),
    5: ("New bad data",
        mutate_add_bad_rows,
        "All 5 planted rows quarantined with the right reason, and each "
        "reason's count in dq_results increases by one."),
    6: ("Schema change",
        mutate_rename_column,
        "Fails loudly on the missing order_amount column rather than loading "
        "nulls silently."),
    7: ("Volume anomaly",
        lambda: mutate_truncate_orders(50),
        "The batch_volume_not_collapsed check warns (ETL). The ELT has no "
        "equivalent, which is a gap to fix."),
    8: ("Distribution drift",
        lambda: mutate_price_drift("S01", 100, consistent=False),
        "The amount_matches_formula check catches every drifted row because "
        "the brief's mutation makes order_amount inconsistent. S01 revenue "
        "collapses rather than inflating."),
    11: ("Distribution drift, CONSISTENT (8b)",
        lambda: mutate_price_drift("S01", 100, consistent=True),
        "The realistic version of experiment 8: unit_price AND order_amount "
        "both scaled 100x, so the amount formula still holds and every "
        "row-level check passes. Before the distribution check existed this "
        "inflated revenue to Rs 9.59M silently. Now store_value_distribution "
        "should flag S01."),
    9: ("Freshness",
        lambda: mutate_stale(30),
        "The batch_freshness_within_1_day check warns with "
        "staleness_days ~= 30."),
}


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if "--list" in sys.argv:
        for n, (name, _, exp) in EXPERIMENTS.items():
            print(f"  {n}. {name}\n     expected: {exp}")
        return
    wanted = [int(a) for a in args] if args else sorted(EXPERIMENTS) + [10]

    results = {}
    if os.path.exists(RESULTS):
        with open(RESULTS) as fh:
            results = json.load(fh)

    for n in wanted:
        if n == 10:
            rec = experiment_10()
            results["10"] = rec
            with open(RESULTS, "w") as fh:
                json.dump(results, fh, indent=2, default=str)
            print(f"  -> written to {os.path.relpath(RESULTS, REPO)}")
            continue
        name, setup, expected = EXPERIMENTS[n]
        # 2 and 3 build on the previous run's warehouse on purpose
        fresh = n not in (2, 3)
        rec = experiment(n, name, setup, expected, fresh=fresh,
                         restore_increment=(n == 3))
        results[str(n)] = rec
        with open(RESULTS, "w") as fh:
            json.dump(results, fh, indent=2, default=str)
        print(f"  -> written to {os.path.relpath(RESULTS, REPO)}")

    restore_data()
    print(f"\nlanding zone restored to pristine. "
          f"{len(wanted)} experiment(s) recorded.")


if __name__ == "__main__":
    main()
