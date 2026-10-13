# Screenshots

Evidence for the submission checklist. Captured from the running project on
2026-10-13, against the canonical state: the five base source files, no
increment, under the recommended **lenient** master-data policy — ETL fact 2310
rows, ELT 2400 staged, **Rs 1,135,628.80** from both pipelines.

| # | File | What it shows | Checklist item |
|---|---|---|---|
| 01 | `01-etl-graph-green-run.jpg` | `nammamart_etl_arnav_ganguly` Graph view, all **11 tasks green**, run Success, 26s. The five `extract_*` tasks are visibly parallel | both DAGs in Graph view (green run) |
| 02 | `02-elt-graph-green-run.jpg` | `nammamart_elt_arnav_ganguly` Graph view, all **16 tasks green**, run Success, 25s. raw → staging → dq → 5 marts → report → audit | both DAGs in Graph view (green run) |
| 03 | `03-schedule-next-run-IST.jpg` | DAGs list in **Asia/Kolkata**: schedule `0 9 * * 1-6`, **Next Run 2026-10-14 09:00:00** on both | the schedule showing 9:00 AM IST |
| 04 | `04-schedule-next-run-UTC.jpg` | The same page in **UTC**: same cron, **Next Run 03:30:00**. The pair is the evidence for the Part C answer about the 5h30m offset | Part C |
| 05 | `05-duckdb-writer-pool.jpg` | Admin → Pools: `duckdb_writer` with **1 slot**. Five of the defects in Part F trace to this one line | supporting |
| 06 | `06-failed-run-schema-change.jpg` | A **failed** run (experiment 6, schema change): `validate` and `run_audit` red, 4 tasks `upstream_failed`, 5 extracts green, dag run **Failed**. Shows the false-green defect fixed | Part F |
| 07 | `07-dq-results-and-pipeline-runs.jpg` | `dq_results` grouped by pipeline and dimension — **all 8 dimensions on both pipelines** — plus the failing checks and the `pipeline_runs` audit rows | query results from `dq_results` and `pipeline_runs` |
| 08 | `08-payment-reconciliation.jpg` | `mart_payment_reconciliation`: **231 exceptions** in 3 classes (122 / 80 / 29) with variances, plus the largest individual mismatches | the payment reconciliation table |
| 09 | `09-quarantine-file-with-reasons.jpg` | A quarantine file, **102 rejected orders**, each with the rule it broke in `failure_reason`. The bad values are readable — `S99`, `Pending??`, quantity 0, a 480-minute delivery — and one row carries **two** reasons | one quarantine file |

## How these were produced

**01–06** are direct captures of the Airflow UI at `http://localhost:<port>`
(Astro assigns the port; `docker ps | grep api-server` gives the current one).

**07–09** are **real query output from the warehouse**, rendered to a page for
legibility rather than captured from a SQL GUI. The SQL that produced each table
is printed above it in the image, so every number is reproducible by running
that query against `include/warehouse_arnav_ganguly.duckdb`. Nothing in them is
transcribed or retyped — they are generated directly from the database and the
quarantine CSV on disk.

The same numbers can be regenerated from the command line at any time:

```bash
.venv/bin/python -I scripts/business_questions.py      # the six business questions
.venv/bin/python -I scripts/part_f_report.py           # the Part F results tables
.venv/bin/python -I include/profile_nammamart.py       # the Part 0 data issues log
```
