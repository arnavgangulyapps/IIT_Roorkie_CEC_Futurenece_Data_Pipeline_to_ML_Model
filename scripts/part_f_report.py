#!/usr/bin/env python3
"""
scripts/part_f_report.py
------------------------
Turn include/part_f_results.json into the Part F markdown for SUBMISSION.md.

Generated rather than hand-written so the numbers in the submission are the
numbers the experiments actually produced. Transcribing 10 experiments x 2 DAGs
by hand is how a report ends up disagreeing with its own evidence.

Usage:
    .venv/bin/python -I scripts/part_f_report.py            # markdown to stdout
    .venv/bin/python -I scripts/part_f_report.py --summary   # one line each
"""

import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS = os.path.join(REPO, "include", "part_f_results.json")

ETL = "nammamart_etl_arnav_ganguly"
ELT = "nammamart_elt_arnav_ganguly"


def states_str(d):
    """success: 11  ->  '11 success'; ordered worst-first so failures lead."""
    order = ["failed", "upstream_failed", "up_for_retry", "skipped", "success"]
    parts = []
    for key in order:
        if d.get(key):
            parts.append(f"{d[key]} {key}")
    for key, n in d.items():
        if key not in order and n:
            parts.append(f"{n} {key}")
    return ", ".join(parts) or "—"


def wh(w):
    if not w:
        return "—"
    return (f"ETL fact {w.get('etl_fact_rows')} / Rs {w.get('etl_revenue'):,.2f}"
            f" · ELT staged {w.get('elt_stg_rows')} / Rs {w.get('elt_mart_revenue'):,.2f}"
            f" · raw_orders {w.get('raw_orders')}")


def main():
    with open(RESULTS) as fh:
        data = json.load(fh)

    if "--summary" in sys.argv:
        for key in sorted(data, key=int):
            r = data[key]
            if key == "10":
                print(f"{key:>3}. {r['name']:24s} final={r.get('final_state')} "
                      f"re-ran={len(r.get('re_ran', []))} "
                      f"untouched={len(r.get('untouched', []))}")
                continue
            legs = " | ".join(
                f"{'ETL' if d == ETL else 'ELT'}={e['run_state']}"
                f"({states_str(e['states'])}, try{e['max_try']})"
                for d, e in r.get("dags", {}).items())
            print(f"{key:>3}. {r['name']:24s} {legs}")
        return

    print("## Experiment results\n")
    print("| # | Experiment | DAG | Run state | Task states | Max try | Wall |")
    print("|---|---|---|---|---|---|---|")
    for key in sorted(data, key=int):
        r = data[key]
        if key == "10":
            continue
        for dag_id, e in r.get("dags", {}).items():
            label = "ETL" if dag_id == ETL else "ELT"
            print(f"| {key} | {r['name']} | {label} | `{e['run_state']}` | "
                  f"{states_str(e['states'])} | {e['max_try']} | "
                  f"{e['total_secs']}s |")

    print("\n## Warehouse state after each experiment\n")
    print("| # | Experiment | Warehouse |")
    print("|---|---|---|")
    for key in sorted(data, key=int):
        r = data[key]
        if "warehouse" in r:
            print(f"| {key} | {r['name']} | {wh(r['warehouse'])} |")

    print("\n## Quarantine counts per experiment\n")
    print("| # | Experiment | orders | payments | customers | products |")
    print("|---|---|---|---|---|---|")
    for key in sorted(data, key=int):
        r = data[key]
        q = r.get("quarantine_files") or {}
        if q:
            print(f"| {key} | {r['name']} | {q.get('orders','—')} | "
                  f"{q.get('payments','—')} | {q.get('customers','—')} | "
                  f"{q.get('products','—')} |")

    if "10" in data:
        r = data["10"]
        print("\n## Experiment 10 — recovery via Clear\n")
        print(f"- failed run: `{r['failed_run']['run_id']}` → "
              f"{states_str(r['failed_run']['states'])}")
        print(f"- command: `{r['clear_cmd']}`")
        print(f"- re-ran ({len(r['re_ran'])}): {', '.join(f'`{t}`' for t in r['re_ran'])}")
        print(f"- untouched ({len(r['untouched'])}): "
              f"{', '.join(f'`{t}`' for t in r['untouched'])}")
        print(f"- final run state: `{r['final_state']}`")

    print("\n## Observed errors, by experiment\n")
    for key in sorted(data, key=int):
        r = data[key]
        for dag_id, e in r.get("dags", {}).items():
            errs = [x for x in (e.get("errors") or []) if x.strip()]
            if errs:
                label = "ETL" if dag_id == ETL else "ELT"
                print(f"- **{key} / {label}**: `{errs[0][:150]}`")


if __name__ == "__main__":
    main()
