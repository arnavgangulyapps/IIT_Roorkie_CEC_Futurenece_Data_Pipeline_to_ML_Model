"""
include/nammamart_dq.py
-----------------------
Shared helpers for the NammaMart ETL and ELT pipelines (Arnav Ganguly).

Holds the things both pipelines must agree on, so the two cannot drift apart:
  * the canonical allowed-value lists and the standardisation maps
  * PII masking
  * the check runner that splits a frame into passed/quarantined and emits
    `dq_results` rows
  * the `dq_results` / `pipeline_runs` writers

Severity contract (Part D):
  Critical -> the row is quarantined and never reaches a reporting table
  Warning  -> the row loads, but the failure is recorded in `dq_results`
              and named in the row's `dq_warnings` column

A row that fails several checks keeps every reason, semicolon-separated. Losing
the second reason is how a quarantine file stops being an audit trail.
"""

import os
import re

import duckdb
import pandas as pd

CRITICAL = "Critical"
WARNING = "Warning"

ORDER_STATUSES = ("DELIVERED", "CANCELLED", "RETURNED")
PAYMENT_MODES = ("UPI", "Card", "Cash on Delivery", "Wallet")
LOYALTY_TIERS = ("Bronze", "Silver", "Gold", "Platinum")

# Observed variants -> canonical. Keys are upper-cased and whitespace-stripped
# before lookup, so 'upi', 'UPI ' and 'UPI' all collapse to one entry.
PAYMENT_MODE_MAP = {
    "UPI": "UPI",
    "CARD": "Card",
    "CASH ON DELIVERY": "Cash on Delivery",
    "COD": "Cash on Delivery",
    "CASHH": "Cash on Delivery",
    "CASH": "Cash on Delivery",
    "WALLET": "Wallet",
}

MAX_DELIVERY_MINUTES = 120  # above this it is an hours/minutes unit error
SLA_MINUTES = 15            # the "% delivered within 15 min" KPI
AMOUNT_TOLERANCE = 0.01     # rupee-paise rounding

# ── master-data severity policy ───────────────────────────────────────────────
#
# Rejecting a master row does not drop one row, it drops every fact that points
# at it. Measured by running both pipelines under both policies
# (scripts/policy_comparison.py): 24 defective master rows cost 413 fact rows
# and Rs 204,470.85 — 18.0% of September revenue — plus a whole zone and a
# whole product category disappearing from the reports.
#
#   "lenient" (default) — a master row is Critical only on key defects (null or
#       duplicate key). Content defects (blank zone, blank category, cost > mrp,
#       mrp <= 0, capacity <= 0, future signup) are Warning and repaired to a
#       visible sentinel. The order survives; the defect is flagged.
#   "strict" — the severities the brief suggests, applied literally. Master
#       content defects are Critical and their orders cascade into quarantine.
#
# Both run from the same code so Part G can quote the difference rather than
# argue it. Set the Airflow Variable `nammamart_master_dq_policy` to switch.
POLICY_VAR = "nammamart_master_dq_policy"
POLICY_LENIENT = "lenient"
POLICY_STRICT = "strict"


def master_content_severity(policy: str) -> str:
    """Severity to apply to master-data *content* defects under `policy`."""
    if policy not in (POLICY_LENIENT, POLICY_STRICT):
        raise ValueError(
            f"{POLICY_VAR} must be '{POLICY_LENIENT}' or '{POLICY_STRICT}', got {policy!r}")
    return CRITICAL if policy == POLICY_STRICT else WARNING


def read_policy(default: str = POLICY_LENIENT) -> str:
    """Read the policy from an Airflow Variable, falling back to the default
    outside Airflow (tests, local runs)."""
    try:
        from airflow.models import Variable
        return Variable.get(POLICY_VAR, default_var=default)
    except Exception:
        return default


# ── standardisation ───────────────────────────────────────────────────────────

def norm_key(s: pd.Series) -> pd.Series:
    """Upper-case, whitespace-stripped lookup key. NaN stays NaN."""
    return s.astype("string").str.strip().str.upper()


def standardise_payment_mode(s: pd.Series) -> pd.Series:
    return norm_key(s).map(PAYMENT_MODE_MAP).astype("string")


def standardise_status(s: pd.Series) -> pd.Series:
    return norm_key(s)


def standardise_tier(s: pd.Series) -> pd.Series:
    """'BRONZE', 'bronze' and 'Bronze' are one tier. Never reject on casing."""
    return s.astype("string").str.strip().str.title()


# ── PII masking (Part E.4) ────────────────────────────────────────────────────

def mask_email(value) -> str | None:
    """pr***@example.com — keeps the domain so deliverability is still auditable."""
    if value is None or pd.isna(value):
        return None
    s = str(value).strip()
    if "@" not in s:
        return "***"           # malformed: reveal nothing at all
    local, _, domain = s.partition("@")
    return f"{local[:2]}***@{domain}"


def mask_phone(value) -> str | None:
    """98******21 — first two and last two digits, enough to confirm a match
    over the phone without handing out a dialable number."""
    if value is None or pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) < 4:
        return "*" * max(len(digits), 1)
    return f"{digits[:2]}{'*' * (len(digits) - 4)}{digits[-2:]}"


def mask_name(value) -> str | None:
    """'Priya Hegde' -> 'Priya H.' — the top-customer report needs a human
    label, not a full legal name."""
    if value is None or pd.isna(value):
        return None
    parts = str(value).strip().split()
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]} {parts[-1][:1]}."


# ── the check runner (Part D) ─────────────────────────────────────────────────

def run_checks(df: pd.DataFrame, checks, *, dataset: str, run_id: str, dag_id: str):
    """Apply `checks` to `df`.

    checks: iterable of (check_name, dimension, severity, fn) where fn(df)
            returns a boolean Series that is True for FAILING rows.

    Returns (passed, quarantined, dq_rows).
      passed       — rows with no Critical failure, plus a `dq_warnings` column
      quarantined  — rows with >=1 Critical failure, plus a `failure_reason` column
      dq_rows      — one dict per check, shaped for the `dq_results` table
    """
    checked_at = pd.Timestamp.now("UTC").isoformat()
    reasons: dict[object, list[str]] = {}
    critical_mask = pd.Series(False, index=df.index)
    dq_rows = []

    for name, dimension, severity, fn in checks:
        bad = fn(df)
        # A check that returns NA for a row must not silently quarantine it.
        bad = bad.reindex(df.index).fillna(False).astype(bool)
        n_failed = int(bad.sum())

        for idx in df.index[bad]:
            reasons.setdefault(idx, []).append(f"{severity}:{name}")

        if severity == CRITICAL:
            critical_mask |= bad

        dq_rows.append({
            "run_id": run_id,
            "dag_id": dag_id,
            "check_name": f"{dataset}.{name}",
            "dimension": dimension,
            "severity": severity,
            "rows_checked": len(df),
            "rows_failed": n_failed,
            "status": "PASS" if n_failed == 0 else "FAIL",
            "checked_at": checked_at,
        })

    quarantined = df[critical_mask].copy()
    quarantined["failure_reason"] = [
        "; ".join(reasons.get(i, [])) for i in quarantined.index
    ]

    passed = df[~critical_mask].copy()
    passed["dq_warnings"] = [
        "; ".join(reasons.get(i, [])) for i in passed.index
    ]

    return passed, quarantined, dq_rows


# ── warehouse writers ─────────────────────────────────────────────────────────

DQ_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    dag_id        VARCHAR,
    check_name    VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,
    checked_at    VARCHAR
)
"""

PIPELINE_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id           VARCHAR,
    dag_id           VARCHAR,
    start_time       VARCHAR,
    end_time         VARCHAR,
    rows_extracted   BIGINT,
    rows_loaded      BIGINT,
    rows_quarantined BIGINT,
    status           VARCHAR
)
"""


def write_dq_results(con: duckdb.DuckDBPyConnection, dq_rows: list[dict]) -> int:
    """Append check results. Idempotent per run: the run's own rows are cleared
    first, so a re-run restates its scorecard instead of doubling it."""
    con.execute(DQ_RESULTS_DDL)
    if not dq_rows:
        return 0
    frame = pd.DataFrame(dq_rows)
    run_ids = sorted(set(frame["run_id"]))
    dag_ids = sorted(set(frame["dag_id"]))
    con.execute(
        "DELETE FROM dq_results WHERE run_id IN ("
        + ",".join("?" * len(run_ids)) + ") AND dag_id IN ("
        + ",".join("?" * len(dag_ids)) + ")",
        run_ids + dag_ids,
    )
    con.register("dq_frame", frame)
    con.execute("INSERT INTO dq_results SELECT * FROM dq_frame")
    con.unregister("dq_frame")
    return len(frame)


def write_pipeline_run(con: duckdb.DuckDBPyConnection, **row) -> None:
    """One row per run (Part F run audit). Same restate-on-rerun rule."""
    con.execute(PIPELINE_RUNS_DDL)
    frame = pd.DataFrame([{
        "run_id": row["run_id"],
        "dag_id": row["dag_id"],
        "start_time": row.get("start_time"),
        "end_time": row.get("end_time"),
        "rows_extracted": int(row.get("rows_extracted") or 0),
        "rows_loaded": int(row.get("rows_loaded") or 0),
        "rows_quarantined": int(row.get("rows_quarantined") or 0),
        "status": row.get("status"),
    }])
    con.execute(
        "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
        [row["run_id"], row["dag_id"]],
    )
    con.register("run_frame", frame)
    con.execute("INSERT INTO pipeline_runs SELECT * FROM run_frame")
    con.unregister("run_frame")


def quarantine_to_csv(df: pd.DataFrame, directory: str, dataset: str, run_id: str) -> str | None:
    """Write quarantined rows with their reasons. Returns the path, or None."""
    if df.empty:
        return None
    os.makedirs(directory, exist_ok=True)
    safe_run = re.sub(r"[^A-Za-z0-9_.-]", "_", str(run_id))
    path = os.path.join(directory, f"{dataset}_failed_{safe_run}.csv")
    df.to_csv(path, index=False)
    return path


# ── self-check ────────────────────────────────────────────────────────────────

def _demo():
    assert mask_email("priya.hegde199@example.com") == "pr***@example.com"
    assert mask_email("not-an-email") == "***"
    assert mask_email(None) is None
    assert mask_phone("9774349021") == "97******21"
    assert mask_phone("+91 97743 49021") == "91" + "*" * 8 + "21"  # 12 digits in
    assert mask_phone("12") == "**"
    assert mask_name("Priya Hegde") == "Priya H."
    assert mask_name("Priya") == "Priya"

    assert standardise_payment_mode(pd.Series(["upi", "UPI ", "cod", "Cashh", "Wallet"])).tolist() \
        == ["UPI", "UPI", "Cash on Delivery", "Cash on Delivery", "Wallet"]
    # an unmappable value becomes NA, so a validity check can catch it
    assert pd.isna(standardise_payment_mode(pd.Series(["Bitcoin"]))[0])
    assert standardise_tier(pd.Series(["BRONZE", "bronze", "Silver"])).tolist() \
        == ["Bronze", "Bronze", "Silver"]

    df = pd.DataFrame({
        "order_id": ["A", "B", "C", "D"],
        "qty": [1, -2, 3, 0],
        "mode": ["UPI", "UPI", "Bitcoin", "UPI"],
    })
    checks = [
        ("qty_positive", "Accuracy", CRITICAL, lambda d: d["qty"] <= 0),
        ("mode_allowed", "Validity", WARNING, lambda d: ~d["mode"].isin(PAYMENT_MODES)),
    ]
    passed, bad, dq = run_checks(df, checks, dataset="orders", run_id="r1", dag_id="d1")

    # B and D fail a Critical check; C fails only a Warning so it still loads
    assert sorted(bad["order_id"]) == ["B", "D"], bad["order_id"].tolist()
    assert sorted(passed["order_id"]) == ["A", "C"], passed["order_id"].tolist()
    assert bad.loc[bad.order_id == "B", "failure_reason"].iloc[0] == "Critical:qty_positive"
    assert passed.loc[passed.order_id == "C", "dq_warnings"].iloc[0] == "Warning:mode_allowed"
    assert passed.loc[passed.order_id == "A", "dq_warnings"].iloc[0] == ""

    # every check is recorded, pass or fail, with the dataset prefix
    assert [r["check_name"] for r in dq] == ["orders.qty_positive", "orders.mode_allowed"]
    assert [r["rows_failed"] for r in dq] == [2, 1]
    assert [r["status"] for r in dq] == ["FAIL", "FAIL"]

    # a row failing two Critical checks keeps both reasons
    df2 = pd.DataFrame({"order_id": ["X"], "qty": [-1], "mode": ["Bitcoin"]})
    _, bad2, _ = run_checks(df2, [
        ("qty_positive", "Accuracy", CRITICAL, lambda d: d["qty"] <= 0),
        ("mode_allowed", "Validity", CRITICAL, lambda d: ~d["mode"].isin(PAYMENT_MODES)),
    ], dataset="orders", run_id="r1", dag_id="d1")
    assert bad2["failure_reason"].iloc[0] == "Critical:qty_positive; Critical:mode_allowed"

    # a check returning NA must not quarantine the row
    df3 = pd.DataFrame({"order_id": ["Y"], "v": [None]})
    passed3, bad3, _ = run_checks(df3, [
        ("na_check", "Accuracy", CRITICAL, lambda d: pd.Series([pd.NA], index=d.index, dtype="boolean")),
    ], dataset="t", run_id="r1", dag_id="d1")
    assert len(bad3) == 0 and len(passed3) == 1

    # writers round-trip, and re-running restates rather than doubling
    con = duckdb.connect()
    assert write_dq_results(con, dq) == 2
    assert write_dq_results(con, dq) == 2
    assert con.execute("SELECT count(*) FROM dq_results").fetchone()[0] == 2
    write_pipeline_run(con, run_id="r1", dag_id="d1", start_time="t0", end_time="t1",
                       rows_extracted=4, rows_loaded=2, rows_quarantined=2, status="SUCCESS")
    write_pipeline_run(con, run_id="r1", dag_id="d1", start_time="t0", end_time="t2",
                       rows_extracted=4, rows_loaded=2, rows_quarantined=2, status="SUCCESS")
    assert con.execute("SELECT count(*) FROM pipeline_runs").fetchone()[0] == 1
    con.close()

    print("nammamart_dq: all self-checks passed")


if __name__ == "__main__":
    _demo()
