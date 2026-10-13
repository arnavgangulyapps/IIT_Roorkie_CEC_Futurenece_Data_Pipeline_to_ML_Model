# NammaMart ETL & ELT Pipeline — Submission

**Author:** Arnav Ganguly
**Module:** FDAIE Module 2 · Data Engineering Foundations
**Repo:** fork of `UmashankarKH/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model`, branch `assignment`
**Warehouse:** `include/warehouse_arnav_ganguly.duckdb`
**DAGs:** `dags/nammamart_etl_arnav_ganguly.py`, `dags/nammamart_elt_arnav_ganguly.py`
**Screenshots:** [`screenshots/`](screenshots/) — see its README for what each one evidences

---

# Part 0 — Discovery and data profiling

## How I profiled

`include/profile_nammamart.py` — 44 declarative checks over the six CSVs, run through DuckDB
with no Airflow and no Docker needed:

```bash
.venv/bin/python -I include/profile_nammamart.py              # console table
.venv/bin/python -I include/profile_nammamart.py --markdown    # the log below
```

**Every file is read as all-VARCHAR, deliberately.** A type-sniffing reader silently turns
`'Pending??'` into `NULL` and `'-3'` into a valid integer, so the evidence of the problem is
destroyed before you can log it. Reading as text and then using `TRY_CAST` inside each check
is what makes the bad rows visible and quotable. The script also asserts the row count of
every file against the client brief (2412/2228/405/152/15/160), so if a CSV or the reader
ever drifts, the run fails loudly instead of reporting confident nonsense.

## Profile summary

| File | Rows | Distinct key | Implied key problem |
|---|---|---|---|
| `orders.csv` | 2,412 | 2,400 `order_id` | 12 duplicated orders (24 rows involved) |
| `payments.csv` | 2,228 | 2,224 `payment_id` | 4 duplicated payments (8 rows involved) |
| `customers.csv` | 405 | 400 `customer_id` | 5 duplicated customers (10 rows involved) |
| `products.csv` | 152 | 150 `product_id` | 2 duplicated products (4 rows involved) |
| `stores.csv` | 15 | 15 `store_id` | key is clean |
| `orders_2026-10-01_increment.csv` | 160 | 160 `order_id` | 150 new + **10 restatements of September orders** |

Cardinality that gave the dirt away immediately: `orders.store_id` has **18** distinct values
against a 15-store master, `orders.product_id` has **159** against 150 real products,
`order_status` has **5** distinct values against 3 allowed, and `payment_mode` has **9**
against 4 allowed. `orders.delivery_minutes` is blank on 164 rows — but only **8** of those are
`DELIVERED`; the other 156 are `CANCELLED`/`RETURNED`, where a blank is correct and must not be
quarantined. That distinction is the difference between a working check and one that throws
away 156 good rows.

## Min / max of every numeric and date column

```bash
.venv/bin/python -I include/profile_nammamart.py --summarize
```

Two passes, deliberately. The all-VARCHAR view gives true null and distinct
counts for the data as it arrived; a second type-sniffed read gives meaningful
min/max, because on a VARCHAR column those comparisons are lexicographic —
correct for ISO dates and wrong for numbers, where `'9' > '100'`.

The extremes alone identify most of the dataset's problems, and several are
sharper than the per-rule checks:

| File | Column | Min | Max | What the extreme tells you |
|---|---|---|---|---|
| orders | `quantity` | **−3** | 5 | negative quantities exist |
| orders | `discount_pct` | 0 | **250** | a 250% discount is impossible |
| orders | `order_amount` | **−1593.0** | 9576.0 | negative revenue |
| orders | `delivery_minutes` | 8 | **1440** | **1440 = exactly 24 hours in minutes** — this is the hours/minutes unit error, confirmed rather than hypothesised |
| orders | `store_id` | **S00** | **S99** | masters run S01–S15, so both extremes are orphans |
| orders | `product_id` | P0001 | **P9989** | masters run P0001–P0152 |
| orders | `order_ts` | 2026-09-01 06:30:15 | **`N/A`** | a literal string `'N/A'`, which is why the column sniffs as VARCHAR and not TIMESTAMP |
| payments | `payment_ts` | 2026-09-01 | **2027-01-01** | future-dated |
| payments | `order_id` | ORD000001 | **ORD997176** | an order id far outside the issued range |
| payments | `amount_paid` | **−1593.0** | 9576.0 | negative payments |
| customers | `phone` | **9804341** | 9999393305 | 7 digits at one end, 10 at the other — not all phones are 10 digits |
| customers | `pincode` | **000000** | 560102 | `000000` is not a pincode |
| customers | `signup_date` | 2024-06-01 | **2027-01-05** | future-dated signups |
| customers | `email` | **`N/A`** | vikram.pillai207@… | literal `'N/A'` again |
| customers | `city` | Bengaluru | Bengaluru | **only one distinct value** — the column carries no information at all |
| stores | `capacity_orders_per_day` | **0** | 600 | a zero-capacity store |
| products | `launch_date` | 2023-01-03 | 2025-09-20 | clean, all in the past |
| increment | everything | — | — | **clean on every extreme**: quantity 1–4, discount 0–20, status only DELIVERED/RETURNED |

Three findings here that the rule-based checks alone did not give me:

1. **`delivery_minutes` maxes at exactly 1440**, which is 24 × 60. That is not a
   slow delivery or a random corruption — it is a value entered in a different
   unit, or a day-long timeout recorded as a duration. Knowing the exact number
   turns "reject implausible values" into a specific question for the client.
2. **`'N/A'` is used as a null placeholder** in `order_ts` (1 row) and `email`
   (4 rows). That is why those columns refuse to sniff as TIMESTAMP. It also
   means a pipeline that treated `'N/A'` as a legitimate string would carry it
   into the warehouse as data. Reading as text and using `TRY_CAST` is what
   catches it.
3. **`customers.city` has exactly one distinct value** ("Bengaluru") plus 6
   blanks. The column is pure overhead: it cannot discriminate anything, and the
   6 blanks are the only variation in it. Worth telling the client the field is
   not worth collecting, rather than writing a rule to defend it.

The increment file being clean on every extreme is also worth noting: the only
problem with it is *semantic* — that 10 of its rows restate existing September
orders — which no column-level profile would ever reveal.

## The exact bad values, for the record

- `order_status`: `DELIVERED` 2182, `CANCELLED` 156, `RETURNED` 69, **`Pending??` 3**, **`delivered` 2**
- `orders.payment_mode`: `UPI` 1329, `Card` 493, `Cash on Delivery` 326, `Wallet` 254, **`upi` 4**, **`UPI ` 2**, **`Cashh` 2**, **`cod` 1**, **`CARD` 1**
- `quantity`: **`-3` ×2, `-2` ×1, `-1` ×3, `0` ×4**, then 1–5 legitimately
- `discount_pct`: 0/5/10/15/20 legitimately, then **`120`, `150`, `250`** one row each
- `loyalty_tier`: `Bronze`/`Silver`/`Gold`/`Platinum`, plus **`BRONZE` 3, `bronze` 3, `SILVER` 2**
- `products.category`: 6 real categories, **3 rows blank**
- `stores.zone`: North/South/East/West, **1 row blank (`S07`)**

## Data Issues Log

36 of 44 checks found problems, spanning **all six files** and **all six quality dimensions**.
192 Critical row-failures in total.

| File | Row key (examples) | Column | Problem | Dimension | Severity | Rows failed | Proposed rule |
|---|---|---|---|---|---|---|---|
| `orders.csv` | ORD000022, ORD000580, ORD000583 | `customer_id` | null/blank customer_id | Completeness | Critical | 18 | Reject: an order with no customer cannot be attributed or reconciled |
| `orders.csv` | ORD000362 | `order_ts` | null/blank order_ts | Completeness | Critical | 1 | Reject: without a timestamp the row cannot be assigned to a report day |
| `orders.csv` | ORD000326, ORD000731, ORD000878 | `delivery_minutes` | DELIVERED with no delivery time | Completeness | Critical | 8 | Reject: a DELIVERED order must carry a delivery time (the SLA KPI depends on it) |
| `orders.csv` | ORD000539, ORD000783, ORD001110 | `order_id` | duplicate order_id | Uniqueness | Critical | 24 | Keep the last row per order_id; duplicates double-count revenue |
| `orders.csv` | ORD000500, ORD001637, ORD001970 | `order_status` | status outside allowed list (`Pending??`) | Validity | Critical | 3 | Reject: must be DELIVERED/CANCELLED/RETURNED after upper-casing |
| `orders.csv` | ORD000159, ORD000433, ORD000691 | `payment_mode` | casing/whitespace variants (`upi`, `UPI `, `Cashh`, `cod`) | Validity | Warning | 8 | Standardise case and whitespace; reject only genuinely unmappable values |
| `orders.csv` | ORD000403, ORD000534, ORD000946 | `order_ts` | timestamp will not parse | Validity | Critical | 5 | Reject: order_ts must parse as a timestamp |
| `orders.csv` | ORD001237, ORD001468, ORD002103 | `discount_pct` | discount of 120/150/250 % | Validity | Critical | 3 | Reject: discount_pct must be between 0 and 100 |
| `orders.csv` | ORD000099, ORD000462, ORD000469 | `quantity` | quantity <= 0 | Accuracy | Critical | 10 | Reject: quantity must be > 0 — **see client question 1** |
| `orders.csv` | ORD000677, ORD000801, ORD000845 | `order_amount` | stored amount ≠ formula | Accuracy | Critical | 18 | Reject: must equal `round(qty × unit_price × (1 − discount/100), 2)` — **see client question 2** |
| `orders.csv` | ORD000020, ORD000424, ORD000455 | `delivery_minutes` | delivery_minutes >= 120 | Accuracy | Critical | 6 | Reject: this is an hours/minutes unit error, not a slow delivery; it distorts the 10-minute-promise KPI |
| `orders.csv` | ORD000070, ORD000134, ORD000596 | `product_id` | product_id not in products master | Consistency | Critical | 9 | Reject: no `cost_price` means no gross margin |
| `orders.csv` | ORD001046, ORD001275, ORD001492 | `store_id` | store_id not in stores master | Consistency | Critical | 5 | Reject: no zone means the row cannot appear in the COO's zone report |
| `orders.csv` | ORD001089, ORD001121, ORD001693 | `order_ts` | future-dated order | Timeliness | Critical | 7 | Reject: order_ts must not be after the run timestamp |
| `payments.csv` | PAY0000009, PAY0000030, PAY0000048 | `gateway_ref` | missing gateway reference | Completeness | Warning | 301 | Load but flag: finance cannot trace these at the gateway — **see client question 3** |
| `payments.csv` | PAY0000509, PAY0000755, PAY0001651 | `payment_id` | duplicate payment_id | Uniqueness | Critical | 8 | Keep one row per payment_id; duplicates inflate collected cash |
| `payments.csv` | PAY0000509, PAY0000755, PAY0001651 | `order_id` | more than one payment per order | Uniqueness | Warning | 8 | Flag for investigation: a retry or a genuine double charge |
| `payments.csv` | PAY0000044, PAY0000522, PAY0000817 | `payment_mode` | casing/whitespace variants | Validity | Warning | 8 | Standardise to the allowed list |
| `payments.csv` | PAY0000116, PAY0000737, PAY0000751 | `amount_paid` | amount_paid <= 0 | Accuracy | Critical | 10 | Reject: a payment of zero or less is not a payment |
| `payments.csv` | PAY0002217, PAY0002218, PAY0002219 | `order_id` | payment references unknown order | Consistency | Critical | 8 | Reject: cash with no order is an unexplained gateway credit |
| `payments.csv` | PAY0000044, PAY0000161, PAY0000323 | `amount_paid` | SUCCESS payment ≠ order_amount | Consistency | Critical | 14 | Route to `*_payment_reconciliation` with a reason — this is the finance gap, criterion 7 |
| `payments.csv` | PAY0000127, PAY0000267, PAY0000337 | `payment_ts` | future-dated payment | Timeliness | Critical | 7 | Reject: payment_ts must not be after the run timestamp |
| `customers.csv` | C00142, C00219, C00233 | `city` | blank city | Completeness | Warning | 6 | Load but flag; default to Bengaluru only with client sign-off |
| `customers.csv` | C00036, C00103, C00226 | `customer_id` | duplicate customer_id | Uniqueness | Critical | 10 | Dedupe to one row per customer (latest `signup_date` wins) |
| `customers.csv` | C00045, C00062, C00063 | `email` | malformed email | Validity | Warning | 12 | Load but flag; a bad email does not invalidate the order |
| `customers.csv` | C00095, C00130, C00151 | `phone` | phone not exactly 10 digits | Validity | Warning | 7 | Load but flag |
| `customers.csv` | C00083, C00132, C00166 | `pincode` | pincode not exactly 6 digits | Validity | Warning | 3 | Load but flag |
| `customers.csv` | C00031, C00032, C00092 | `loyalty_tier` | tier casing inconsistent (`BRONZE`, `bronze`, `SILVER`) | Consistency | Warning | 8 | Standardise to title case; never reject — this would silently drop real customers from the top-10 report |
| `customers.csv` | C00038, C00398, C00400 | `signup_date` | future-dated signup | Timeliness | Critical | 3 | Reject: a customer cannot sign up tomorrow |
| `products.csv` | P0051, P0060, P0068 | `category` | blank category | Completeness | Critical | 3 | Reject: category drives the margin-by-category report |
| `products.csv` | P0030, P0140 | `product_id` | duplicate product_id | Uniqueness | Critical | 4 | Dedupe to one row per product |
| `products.csv` | P0109 | `mrp` | mrp <= 0 | Accuracy | Critical | 1 | Reject: a free product breaks every margin calculation |
| `products.csv` | P0019, P0073, P0074 | `cost_price` | cost_price > mrp | Accuracy | Critical | 5 | Reject: implies a guaranteed loss on a healthy product; almost certainly a data error |
| `stores.csv` | S07 | `zone` | blank zone | Completeness | Critical | 1 | Reject: zone drives the revenue-by-zone report the COO reads every morning |
| `stores.csv` | S12 | `capacity_orders_per_day` | capacity <= 0 | Accuracy | Critical | 1 | Reject: a zero-capacity store makes the capacity check meaningless |
| `orders_2026-10-01_increment.csv` | ORD000017, ORD000271, ORD000332 | `order_id` | restatement of an existing September order | Consistency | Warning | 10 | **MERGE on `order_id`**, do not append — these are corrections, and appending them double-counts |

Checks that passed clean, which is itself worth recording: `orders.unit_price > 0`,
`orders.customer_id` referential integrity (every non-null customer_id does exist in the
master — the problem there is nullness, not orphanhood), `payments.payment_mode` agreeing with
its order's mode, `products.launch_date` not future-dated, `stores.store_id` uniqueness,
`stores.pincode` format, and the increment file's `order_status` and key uniqueness.

## The single most dangerous row in the dataset

`orders_2026-10-01_increment.csv` row 1 is `ORD000017`, dated **2026-09-01**, status
**`RETURNED`**. It is not a new order — it is a September order being restated a month later.
A pipeline that appends the increment file produces 2,572 orders where 2,562 exist, and counts
`ORD000017` twice. This is acceptance criterion 6 and the reason staging must MERGE on
`order_id` rather than `INSERT`.

## Three questions for the client

**1. Are the 10 negative/zero-quantity orders returns, or data-entry errors?**
`ORD000099`, `ORD000462`, `ORD000469` and 7 others carry `quantity` of `-3`, `-2`, `-1` or `0`,
and `order_amount` moves with the sign. If these are the ops team's way of recording a return,
they belong in revenue as negative lines and quarantining them *overstates* September. If they
are typos, quarantining is right. The two answers differ in the reported number, so I cannot
pick one on the client's behalf. **Asked of:** ops lead. *Current assumption: data-entry error,
quarantined as Critical.*

**2. When `order_amount` disagrees with `quantity × unit_price × (1 − discount/100)`, which one does finance treat as the truth?**
18 orders disagree, and **the disagreement is systematic, not random noise.** Dividing the stored
amount by the recomputed one gives only three ratios:

| Ratio | Rows | Reads like |
|---|---|---|
| **×10.0** | 6 | a decimal-point shift (`675.75` stored as `6757.5`) |
| **×1.5** | 5 | the discount applied with the wrong sign or a 50 % uplift |
| **×0.5** | 4 | half the amount — a split payment, or quantity halved |

The remaining 3 of the 18 are the impossible-discount rows (120 %, 150 %, 250 %), which produce a
*negative* computed amount while `order_amount` stays positive — so on those three the discount
field is clearly the corrupt one, not the amount.

That pattern matters for the answer. Random corruption would argue for rejecting the rows;
three tight clusters argue that these are *recoverable* with a repair rule (shift the decimal,
flip the sign) once finance says which field is authoritative. If the stored `order_amount` is
what the customer's card was actually charged, it is the formula inputs that are broken and
repairing them is the right move; if the formula is the contract, the amounts are corrupt.
I can quarantine or I can repair, but picking without finance is picking the September revenue
number on their behalf. **Asked of:** finance (CFO's analyst). *Current assumption: the formula is
authoritative, all 18 quarantined as Critical and none repaired — the conservative choice, and
criterion 2 says no bad row reaches the finance tables.*

**3. An order DELIVERED on 3 September is RETURNED on 1 October. Does September's reported revenue get restated, or does it stay as first published?**
This decides the whole incremental design, not just one rule. Finance usually wants the closed
period frozen and the return recognised in October; analytics usually wants the current truth of
every order. Criterion 6 requires the status correction to be applied, so the pipeline must at
minimum restate — but if finance needs the as-first-published number too, the fact table needs
effective-dating (SCD Type 2) rather than an in-place update, and that is a design decision to
take before building, not after. **Asked of:** CTO + finance jointly. *Current assumption:
restate in place, and keep the raw layer as the as-received record so both views are
recoverable.* (Answered in full in Part E.9.)

Also open, lower stakes: are the 301 payments with no `gateway_ref` an export bug or a genuine
class of payment (cash on delivery reconciled offline)? And should the 8 orders with more than
one SUCCESS payment be treated as retries or as double charges to refund?

---

# Part C — Scheduling

Both DAGs carry the identical schedule block:

```python
@dag(
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "arnav_ganguly",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=20),
    },
)
```

**What is your cron expression?**
`0 9 * * 1-6` — 09:00 on days 1 to 6 of the week, Monday through Saturday, with
Sunday (`0`) excluded. The client's review runs Monday to Saturday, so a plain
`0 9 * * *` would fire a seventh run nobody reads, and `1-5` would miss
Saturday's stand-up.

**What time does the run show in UTC in the Airflow UI?**
**03:30 UTC.** IST is UTC+05:30, so 09:00 IST minus 5h30m is 03:30 the same day.
Airflow stores and displays instants in UTC by default, so the UI shows a 03:30
next-run time even though the cron reads `0 9`. That gap is not an error and is
worth telling the client before they open the UI and think the schedule is wrong.
Note that India observes no daylight saving, so this offset is fixed year-round —
a pipeline scheduled for a European or US timezone would see the UTC time shift
twice a year and the local time stay put.

**What would happen at 9 AM if you had left the `start_date` in UTC?**
The cron would be interpreted in UTC, so the DAG would fire at **09:00 UTC =
14:30 IST** — five and a half hours *after* the 09:30 leadership stand-up. The
review would run on yesterday's spreadsheet every single day, and nothing would
look broken: the DAG would be green, the tasks would succeed, the data would be
correct, and it would simply be five hours late. That is the worst class of
scheduling bug, because there is no error to find. Making the timezone explicit
with `pendulum.datetime(..., tz="Asia/Kolkata")` is what prevents it.

**What happens if your laptop or Docker is off at 9 AM?**
Nothing runs. There is no scheduler process alive to notice that the time has
passed, so no run is created — and with `catchup=False` the missed interval is
*not* made up when Docker comes back. The next run is simply the next scheduled
one. Starting Airflow at 11:00 does not produce the 09:00 run.

This is the honest limitation of the local setup and I would say so to the
client rather than let them believe a laptop is a production scheduler. Three
things would fix it, in increasing order of cost: run the scheduler on an
always-on host (a small cloud VM or Astronomer/MWAA), add an SLA miss callback
so a missed 09:00 is alerted rather than discovered at 09:30, and give the DAG a
`timetable` with a data-interval the COO's dashboard can display, so a stale
number is visibly stale.

**Why `catchup=False`, justified.**
With a `start_date` of 1 October and catchup enabled, the first unpause would
immediately queue every missed interval — about 12 runs by mid-October — and
each one would re-read the same September CSVs and rewrite the same tables. The
result would not be wrong, because both pipelines are idempotent, but it would
be 12 pointless runs competing for the same DuckDB file and 12 rows of noise in
`pipeline_runs`. The client wants this morning's number, not a reconstruction of
every morning since the start date. `max_active_runs=1` is set for the same
reason: DuckDB is a single-writer embedded database, and two concurrent runs
would collide on the warehouse file.

**Why these retry settings.**
`retries=2` with `retry_delay=1 minute` covers the realistic transient failure
here — a source file still being written by the upstream export when the DAG
starts. Two retries a minute apart is enough to ride that out and not enough to
hide a genuine outage. `execution_timeout=20 minutes` is roughly 10x the
observed runtime, so a hung task fails loudly instead of occupying the scheduler
until someone notices; without it, a task blocked on a file lock would sit in
`running` forever and the 09:00 SLA would pass with the DAG looking healthy.

*(Observed during a real run: when the container was missing a Parquet engine,
the five extract tasks went `up_for_retry` twice at 1-minute intervals and then
`failed`, and the downstream tasks went `upstream_failed` — Part F experiment 4
behaviour, reached by accident before I ran the experiment deliberately.)*

---

# The six business questions

Answered from the canonical run: the five base source files, no increment, under
the recommended **lenient** master-data policy — ETL fact 2310 rows, ELT 2400
staged, **Rs 1,135,628.80** both.

## The six business questions

Each is one query on the final tables, answered from **both** pipelines. Generated by `scripts/business_questions.py`, which asserts that the two pipelines return identical rows wherever they should — so the claim that they agree is checked when the document is produced rather than asserted in prose.


### Question 1 — Daily net revenue by zone and by store (delivered orders only, after discount)

**ETL**

```sql
-- ETL: the reporting table the pipeline builds
SELECT zone, store_name,
       sum(delivered_orders)      AS delivered_orders,
       round(sum(net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
GROUP BY zone, store_name
ORDER BY net_revenue DESC
LIMIT 8
```

| zone | store_name | delivered_orders | net_revenue |
|---|---|---|---|
| South | NammaMart Banashankari | 148 | 89,808.30 |
| West | NammaMart Malleshwaram | 141 | 85,151.65 |
| UNKNOWN | NammaMart BTM Layout | 145 | 82,725.60 |
| West | NammaMart Basaveshwaranagar | 150 | 79,323.95 |
| East | NammaMart Marathahalli | 137 | 78,968.00 |
| South | NammaMart JP Nagar | 130 | 77,794.45 |
| North | NammaMart RT Nagar | 138 | 77,254.85 |
| North | NammaMart Hebbal | 136 | 76,552.35 |

**ELT**

```sql
-- ELT: the mart table, same grain
SELECT zone, store_name,
       sum(delivered_orders)      AS delivered_orders,
       round(sum(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
GROUP BY zone, store_name
ORDER BY net_revenue DESC
LIMIT 8
```

| zone | store_name | delivered_orders | net_revenue |
|---|---|---|---|
| South | NammaMart Banashankari | 148 | 89,808.30 |
| West | NammaMart Malleshwaram | 141 | 85,151.65 |
| UNKNOWN | NammaMart BTM Layout | 145 | 82,725.60 |
| West | NammaMart Basaveshwaranagar | 150 | 79,323.95 |
| East | NammaMart Marathahalli | 137 | 78,968.00 |
| South | NammaMart JP Nagar | 130 | 77,794.45 |
| North | NammaMart RT Nagar | 138 | 77,254.85 |
| North | NammaMart Hebbal | 136 | 76,552.35 |

**identical to the ETL result above**


### Question 2 — Revenue and gross margin by product category (margin = revenue - quantity x cost_price)

**ETL**

```sql
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct
FROM etl_category_margin
ORDER BY net_revenue DESC
```

| category | delivered_orders | net_revenue | gross_margin | margin_pct |
|---|---|---|---|---|
| Staples | 306 | 266,500.90 | 58,728.22 | 22.00 |
| Personal Care | 329 | 205,016.80 | 44,444.98 | 21.70 |
| Household | 339 | 199,699.45 | 51,145.33 | 25.60 |
| Dairy & Bakery | 380 | 169,528.75 | 44,791.84 | 26.40 |
| Snacks & Beverages | 322 | 161,886.95 | 37,724.49 | 23.30 |
| Fruits & Vegetables | 370 | 93,284.30 | 19,600.62 | 21.00 |
| UNCATEGORISED | 42 | 39,711.65 | 4,563.95 | 11.50 |

**ELT**

```sql
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct
FROM mart_category_margin
ORDER BY net_revenue DESC
```

| category | delivered_orders | net_revenue | gross_margin | margin_pct |
|---|---|---|---|---|
| Staples | 306 | 266,500.90 | 58,728.22 | 22.00 |
| Personal Care | 329 | 205,016.80 | 44,444.98 | 21.70 |
| Household | 339 | 199,699.45 | 51,145.33 | 25.60 |
| Dairy & Bakery | 380 | 169,528.75 | 44,791.84 | 26.40 |
| Snacks & Beverages | 322 | 161,886.95 | 37,724.49 | 23.30 |
| Fruits & Vegetables | 370 | 93,284.30 | 19,600.62 | 21.00 |
| UNCATEGORISED | 42 | 39,711.65 | 4,563.95 | 11.50 |

**identical to the ETL result above**


### Question 3 — Average delivery time per store, and the % of delivered orders within 15 minutes

**ETL**

```sql
-- ETL: aggregated from the fact table to store grain
SELECT store_name,
       round(avg(delivery_minutes), 1) AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE within_15_min)
             / count(*), 1)            AS pct_within_15_min,
       count(*)                        AS delivered_orders
FROM etl_fact_orders
WHERE is_delivered AND delivery_minutes IS NOT NULL
GROUP BY store_name
ORDER BY pct_within_15_min DESC
LIMIT 8
```

| store_name | avg_delivery_minutes | pct_within_15_min | delivered_orders |
|---|---|---|---|
| NammaMart Whitefield | 20.50 | 36.00 | 150 |
| NammaMart BTM Layout | 21.30 | 32.40 | 145 |
| NammaMart Hebbal | 20.70 | 30.90 | 136 |
| NammaMart RT Nagar | 20.60 | 30.40 | 138 |
| NammaMart Vijayanagar | 21.90 | 30.30 | 152 |
| NammaMart JP Nagar | 21.50 | 30.00 | 130 |
| NammaMart HSR Layout | 21.10 | 28.70 | 143 |
| NammaMart Yelahanka | 21.40 | 28.40 | 141 |

**ELT**

```sql
-- ELT: the purpose-built SLA mart
SELECT store_name, avg_delivery_minutes, pct_within_15_min, delivered_orders
FROM mart_delivery_sla
ORDER BY pct_within_15_min DESC
LIMIT 8
```

| store_name | avg_delivery_minutes | pct_within_15_min | delivered_orders |
|---|---|---|---|
| NammaMart Whitefield | 20.50 | 36.00 | 150 |
| NammaMart BTM Layout | 21.30 | 32.40 | 145 |
| NammaMart Hebbal | 20.70 | 30.90 | 136 |
| NammaMart RT Nagar | 20.60 | 30.40 | 138 |
| NammaMart Vijayanagar | 21.90 | 30.30 | 152 |
| NammaMart JP Nagar | 21.50 | 30.00 | 130 |
| NammaMart HSR Layout | 21.10 | 28.70 | 143 |
| NammaMart Yelahanka | 21.40 | 28.40 | 141 |

**identical to the ETL result above**


### Question 4 — Revenue by payment mode, and the total that does not reconcile with the gateway

**ETL**

```sql
SELECT payment_mode,
       count(*)                        AS delivered_orders,
       round(sum(net_revenue), 2)      AS net_revenue
FROM etl_fact_orders
WHERE is_delivered
GROUP BY payment_mode
ORDER BY net_revenue DESC
```

| payment_mode | delivered_orders | net_revenue |
|---|---|---|
| UPI | 1169 | 620,387.85 |
| Card | 415 | 230,913.90 |
| Cash on Delivery | 286 | 160,688.25 |
| Wallet | 218 | 123,638.80 |

**ELT**

```sql
SELECT payment_mode,
       count(*)                        AS delivered_orders,
       round(sum(order_amount), 2)     AS net_revenue
FROM stg_orders
WHERE dq_status <> 'QUARANTINED' AND order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY net_revenue DESC
```

| payment_mode | delivered_orders | net_revenue |
|---|---|---|
| UPI | 1169 | 620,387.85 |
| Card | 415 | 230,913.90 |
| Cash on Delivery | 286 | 160,688.25 |
| Wallet | 218 | 123,638.80 |

**identical to the ETL result above**


### Question 5 — Top 10 customers by spend, with loyalty tier (no raw email or phone shown)

**ETL**

```sql
-- ETL: fact joined to the masked customer dimension.
-- NOTE the GROUP BY must include customer_id: two different customers can mask
-- to the same name and email, and grouping on the masked columns alone merges
-- them and overstates the top spender.
SELECT f.customer_id, d.customer_name, d.email_masked, d.phone_masked,
       d.loyalty_tier, round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f
JOIN etl_dim_customer d USING (customer_id)
WHERE f.is_delivered
GROUP BY f.customer_id, d.customer_name, d.email_masked, d.phone_masked,
         d.loyalty_tier
ORDER BY total_spend DESC
LIMIT 10
```

| customer_id | customer_name | email_masked | phone_masked | loyalty_tier | total_spend |
|---|---|---|---|---|---|
| C00136 | Sanjay B. | sa***@example.com | 96******88 | Bronze | 8,738.10 |
| C00283 | Nikhil R. | ni***@example.com | 91******43 | Bronze | 7,934.60 |
| C00071 | Mahesh P. | ma***@example.com | 91******42 | Silver | 7,349.30 |
| C00310 | Kiran P. | ki***@example | 91******35 | Bronze | 7,258.95 |
| C00236 | Sneha N. | sn***@example.com | 90******01 | Platinum | 7,016.40 |
| C00338 | Bhavana I. | bh***@example.com | 94******05 | Silver | 6,848.55 |
| C00209 | Kavya P. | ka***@example.com | 98******97 | Platinum | 6,519.40 |
| C00115 | Sanjay K. | sa***@example.com | 96******85 | Bronze | 6,483.70 |
| C00036 | Deepa D. | de***@example.com | 98******57 | Bronze | 6,466.50 |
| C00072 | Nandini M. | na***@example.com | 98******05 | Silver | 6,433.95 |

**ELT**

```sql
SELECT customer_id, customer_name, email_masked, phone_masked,
       loyalty_tier, total_spend
FROM mart_top_customers
ORDER BY total_spend DESC
LIMIT 10
```

| customer_id | customer_name | email_masked | phone_masked | loyalty_tier | total_spend |
|---|---|---|---|---|---|
| C00136 | Sanjay B. | sa***@example.com | 96******88 | Bronze | 8,738.10 |
| C00283 | Nikhil R. | ni***@example.com | 91******43 | Bronze | 7,934.60 |
| C00071 | Mahesh P. | ma***@example.com | 91******42 | Silver | 7,349.30 |
| C00310 | Kiran P. | ki***@example | 91******35 | Bronze | 7,258.95 |
| C00236 | Sneha N. | sn***@example.com | 90******01 | Platinum | 7,016.40 |
| C00338 | Bhavana I. | bh***@example.com | 94******05 | Silver | 6,848.55 |
| C00209 | Kavya P. | ka***@example.com | 98******97 | Platinum | 6,519.40 |
| C00115 | Sanjay K. | sa***@example.com | 96******85 | Bronze | 6,483.70 |
| C00036 | Deepa D. | de***@example.com | 98******57 | Bronze | 6,466.50 |
| C00072 | Nandini M. | na***@example.com | 98******05 | Silver | 6,433.95 |

**identical to the ETL result above**


### Question 6 — Number of rows rejected today, by file and by reason

**ETL**

```sql
SELECT split_part(check_name, '.', 1)  AS file,
       split_part(check_name, '.', 2)  AS reason,
       severity,
       rows_failed
FROM dq_results
WHERE dag_id = 'nammamart_etl_arnav_ganguly'
  AND status = 'FAIL' AND severity = 'Critical'
ORDER BY rows_failed DESC
LIMIT 10
```

| file | reason | severity | rows_failed |
|---|---|---|---|
| orders | customer_id_present | Critical | 18 |
| orders | amount_matches_formula | Critical | 18 |
| orders | order_id_unique | Critical | 12 |
| orders | quantity_positive | Critical | 10 |
| payments | amount_paid_positive | Critical | 10 |
| orders | product_id_known | Critical | 9 |
| orders | delivered_has_delivery_minutes | Critical | 8 |
| orders | order_ts_not_future | Critical | 7 |
| payments | payment_ts_not_future | Critical | 7 |
| orders | delivery_minutes_plausible | Critical | 6 |

**ELT**

```sql
SELECT split_part(check_name, '.', 1)  AS file,
       split_part(check_name, '.', 2)  AS reason,
       severity,
       rows_failed
FROM dq_results
WHERE dag_id = 'nammamart_elt_arnav_ganguly'
  AND status = 'FAIL' AND severity = 'Critical'
ORDER BY rows_failed DESC
LIMIT 10
```

| file | reason | severity | rows_failed |
|---|---|---|---|
| orders | customer_id_present | Critical | 18 |
| orders | amount_matches_formula | Critical | 18 |
| orders | quantity_positive | Critical | 10 |
| payments | amount_paid_positive | Critical | 10 |
| orders | product_id_known | Critical | 9 |
| orders | delivered_has_delivery_minutes | Critical | 8 |
| orders | order_ts_not_future | Critical | 7 |
| payments | payment_ts_not_future | Critical | 7 |
| orders | delivery_minutes_plausible | Critical | 6 |
| orders | store_id_known | Critical | 5 |

### Question 4b — The unreconciled total the CFO asks about

**ETL**

```sql
SELECT reason, count(*) AS orders, round(sum(variance), 2) AS variance
FROM etl_payment_reconciliation
GROUP BY reason ORDER BY abs(sum(variance)) DESC
```

| reason | orders | variance |
|---|---|---|
| payment with no matching order | 80 | -80,986.30 |
| amount collected does not equal order amount | 122 | 63,972.45 |
| delivered order with no payment recorded | 29 | 17,533.30 |

**ELT**

```sql
SELECT reason, count(*) AS orders, round(sum(variance), 2) AS variance
FROM mart_payment_reconciliation
GROUP BY reason ORDER BY abs(sum(variance)) DESC
```

| reason | orders | variance |
|---|---|---|
| payment with no matching order | 80 | -80,986.30 |
| amount collected does not equal order amount | 122 | 63,972.45 |
| delivered order with no payment recorded | 29 | 17,533.30 |

**identical to the ETL result above**


### Notes on these answers

**Question 5 has a trap worth naming.** The obvious ETL query groups by the
masked columns — `customer_name`, `email_masked`, `loyalty_tier` — and that
produces a *wrong* answer. Masking is lossy by design, so two different
customers can share a masked name and email; grouping on them merges the
customers and overstates the top spender. My first attempt did exactly this and
reported Deepa D. at Rs 12,788.60 as the top customer. Grouping by
`customer_id` gives the correct answer: **Sanjay B. (C00136) at Rs 8,738.10**,
which matches `mart_top_customers` exactly. The lesson generalises — once PII is
masked, the masked column is no longer a safe grouping key, and only the
surrogate id is.

**Question 6 is per-pipeline by design**, not a disagreement. Each query filters
`dq_results` to its own `dag_id`, because the question is "what did *this* run
reject". The counts match for the rules both pipelines share; they differ where
the two express a rule differently — the ETL records
`orders.order_id_unique` as 12 rejected duplicate copies, while the ELT records
the same 12 from `stg_dedup_stats`, since its staging layer keeps the surviving
row and sets the superseded copies aside separately.

**One masked value looks broken and is not.** Customer C00310 shows
`ki***@example` with no `.com`. The source value is literally
`kiran.patil310@example` — the TLD is missing in the CRM export. The mask is
faithful; the data is not. The malformed emails fall into three classes worth
reporting to the client: the literal placeholder `N/A` (4 rows), a missing `@`
(`arjun.rao307example.com`), and a missing TLD (`prakash.pillai63@example`).
All are Warning severity, so these customers still appear in the reports.

**Why the unreconciled total is negative on one line.** In question 4b,
"payment with no matching order" shows **−Rs 80,986.30**: the variance is
`order_amount − amount_collected`, and for a payment with no order there is no
order amount, so the whole collected sum is negative variance. That is cash the
gateway received that the order system cannot explain — the single line Finance
should look at first.


---

# Part E — Data governance

## 1. Ownership

| Dataset | Proposed owner | Why them |
|---|---|---|
| `orders.csv` | Head of Operations | Owns the order management system that produces it and the store managers who create the rows |
| `payments.csv` | Finance Controller | Owns the gateway relationship and is accountable for the cash figure |
| `customers.csv` | Head of Growth/CRM, with the **DPO as data steward** | Owns the CRM, but it is the only file with personal data, so a second signature is needed on any change to who can read it |
| `products.csv` | Category/Merchandising Manager | Owns `cost_price`, which is the only input to gross margin |
| `stores.csv` | Head of Operations | Owns the store network and `capacity_orders_per_day` |
| Warehouse tables (`etl_*`, `raw_*`, `stg_*`, `mart_*`) | **Me, the data engineer** | I own the transformation. I do *not* own the business definitions, and that distinction is the point |

**Who approves a change to a quality rule, such as the 120-minute delivery limit?**
Not me. I own the implementation; the business owns the threshold. A change to
`MAX_DELIVERY_MINUTES` moves the reported KPI, so it needs the **dataset owner
for the affected file (Head of Operations) plus the consumer of the metric
(the COO)** — because the number lands in her 09:30 review. The rule lives in
one named constant in `include/nammamart_dq.py` precisely so the approval has
one artefact to point at, and the change shows up in `dq_results` as a step
change in `rows_failed` on `orders.delivery_minutes_plausible` the morning it
takes effect.

The master-data severity policy is a worked example. I did not get to choose it:
it changes September revenue by a measured Rs 204,470.85, so it is an Airflow Variable
(`nammamart_master_dq_policy`) with both settings implemented, and the client
decides. An engineer silently picking `strict` would have deleted a fifth of the
revenue on their own authority.

## 2. Data dictionary

**`etl_fact_orders`** (grain: one row per `order_id`)

| Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| `order_id` | VARCHAR | Business key, unique | `orders.csv` | `ORD\d{6}` | No |
| `order_date` | DATE | Report day, derived from `order_ts` | derived | 2026-09-01 … | No |
| `order_ts` | TIMESTAMP | When the order was placed | `orders.csv` | not future | No |
| `customer_id` | VARCHAR | FK to `etl_dim_customer` | `orders.csv` | must exist in master | **Pseudonymous** |
| `store_id` | VARCHAR | FK to `etl_dim_store` | `orders.csv` | `S01`–`S15` | No |
| `store_name`, `zone` | VARCHAR | Denormalised from store dim | `stores.csv` | zone ∈ N/S/E/W + `UNKNOWN` | No |
| `product_id` | VARCHAR | FK to `etl_dim_product` | `orders.csv` | must exist in master | No |
| `category`, `sub_category` | VARCHAR | Denormalised from product dim | `products.csv` | 6 categories + `UNCATEGORISED` | No |
| `loyalty_tier` | VARCHAR | Denormalised from customer dim | `customers.csv` | Bronze/Silver/Gold/Platinum | No |
| `quantity` | INT64 | Units ordered | `orders.csv` | > 0 | No |
| `unit_price`, `discount_pct`, `cost_price` | DOUBLE | Pricing inputs | orders/products | price > 0; discount 0–100 | No |
| `order_amount` | DOUBLE | Charged amount | `orders.csv` | = qty×price×(1−disc/100) | No |
| `net_revenue` | DOUBLE | **Revenue. 0 unless DELIVERED** | derived | ≥ 0 | No |
| `gross_margin` | DOUBLE | `net_revenue − quantity×cost_price` | derived | may be negative | No |
| `payment_mode` | VARCHAR | Standardised mode | `orders.csv` | UPI/Card/Cash on Delivery/Wallet | No |
| `order_status` | VARCHAR | Lifecycle state | `orders.csv` | DELIVERED/CANCELLED/RETURNED | No |
| `channel` | VARCHAR | Order origin | `orders.csv` | App/Web | No |
| `delivery_minutes` | DOUBLE | Door-to-door time | `orders.csv` | < 120, null if not delivered | No |
| `within_15_min` | BOOLEAN | SLA flag | derived | true/false | No |
| `is_delivered` | BOOLEAN | Revenue-recognition flag | derived | true/false | No |
| `_source_file`, `_run_id`, `_loaded_at` | VARCHAR | Lineage | pipeline | — | No |

**`etl_dim_customer`** (grain: one row per `customer_id`) — the PII boundary.

| Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| `customer_id` | VARCHAR | Business key | `customers.csv` | `C\d{5}` | **Pseudonymous** |
| `customer_name` | VARCHAR | **Masked**: `Priya H.` | derived from `full_name` | — | **Reduced** |
| `email_masked` | VARCHAR | **Masked**: `pr***@example.com` | derived from `email` | — | **Reduced** |
| `phone_masked` | VARCHAR | **Masked**: `98******21` | derived from `phone` | — | **Reduced** |
| `city`, `area`, `pincode` | VARCHAR | Location | `customers.csv` | pincode 6 digits | **Quasi-identifier** |
| `signup_date` | DATE | CRM signup | `customers.csv` | not future | No |
| `loyalty_tier` | VARCHAR | Title-cased tier | `customers.csv` | 4 tiers | No |
| `is_active` | BOOLEAN | Active flag | `customers.csv` | true/false | No |

**`etl_dim_product`**: `product_id` (PK), `product_name`, `category`,
`sub_category`, `brand`, `mrp`, `cost_price` (**commercially sensitive — not
PII, but not for store managers either**), `unit`, `is_perishable`,
`launch_date`. No PII.

**`etl_dim_store`**: `store_id` (PK), `store_name`, `zone`, `area`, `pincode`,
`open_date`, `manager_name` (**masked to `Arjun H.` — a named employee is
personal data**), `capacity_orders_per_day`. 

## 3. Lineage — tracing one number back to source rows

**The number:** `mart_daily_store_kpis` says NammaMart Marathahalli (`S10`, East
zone) made **Rs 6,683.00** from **3 delivered orders** on **2026-09-18**.

**ELT — two queries, no guessing.**

```sql
-- step 1: which orders make up the number
SELECT order_id, order_amount, _source_file
FROM stg_orders
WHERE order_date = '2026-09-18' AND store_id = 'S10'
  AND dq_status <> 'QUARANTINED' AND order_status = 'DELIVERED';
-- ORD001342 1292.00 | ORD001363 4095.00 | ORD001372 1296.00   -> 6683.00

-- step 2: the same rows exactly as the file delivered them
SELECT order_id, order_amount, _source_file, _row_num, _loaded_at
FROM raw_orders WHERE order_id IN ('ORD001342','ORD001363','ORD001372');
-- ORD001342 '1292.0' orders.csv line 259
-- ORD001363 '4095.0' orders.csv line 928
-- ORD001372 '1296.0' orders.csv line 32
```

I can name **the file and the line number** for every rupee of that Rs 6,683.00,
and `_loaded_at` tells me which run put it there.

**ETL — possible, but not as good.** `etl_fact_orders` carries `_source_file`
and `_run_id` (I added them for exactly this), so I can get back to
`orders.csv`. What I *cannot* get is the line number or the pre-transformation
text, because the row was cast and standardised in pandas before it was stored
and the original was never persisted. To see what the file actually said I have
to go open the CSV — and if the file has since been replaced by the next day's
export, that evidence is gone.

**ELT is easier, and the reason is structural, not cosmetic.** ELT keeps the
as-received row *inside the warehouse*, so lineage is a join. ETL keeps only the
transformed row, so lineage is a join plus an act of faith that the CSV on disk
is still the one that was read. This is the single strongest argument in Part G.

## 4. Personal data

**Every personal-data column in the dataset:**

| Column | File | Classification |
|---|---|---|
| `full_name` | `customers.csv` | **Direct identifier** |
| `email` | `customers.csv` | **Direct identifier** (also a contact channel) |
| `phone` | `customers.csv` | **Direct identifier** (also, in India, often a UPI handle and an Aadhaar-linked number) |
| `manager_name` | `stores.csv` | **Direct identifier** — an employee, easy to overlook |
| `customer_id` | orders, customers | **Pseudonymous identifier** — not personal alone, re-identifying when joined to the CRM |
| `area`, `pincode`, `city` | `customers.csv` | **Quasi-identifiers** — with a loyalty tier and an order history, a 6-digit pincode narrows a person a long way |
| `signup_date`, `order_ts` | customers, orders | **Behavioural** — a precise timestamp is a pattern of life |

**How I masked it.** `mask_email` keeps the first two characters and the whole
domain (`pr***@example.com`); `mask_phone` keeps the first and last two digits
(`98******21`); `mask_name` keeps the first name and a last initial
(`Priya H.`). Written once in `include/nammamart_dq.py` and mirrored exactly as
DuckDB macros in `include/sql/00_macros.sql`, so the two pipelines cannot drift
and produce different masks for the same person.

The structural part matters more than the function: **`stg_customers` does not
have `email`, `phone` or `full_name` as columns at all.** They are consumed in
the SELECT that builds it and never land. So `mart_top_customers` cannot leak
PII even if someone later edits it carelessly — the unmasked values are not in
its inputs. The test asserts this by checking `duckdb_columns()`, not by
checking values.

**Why mask rather than delete?** Three reasons, in order of how often they
actually bite:
1. **Support needs to confirm identity.** When a customer calls about an order,
   an agent must verify them. `98******21` lets the agent confirm a number the
   customer reads out without the warehouse ever handing out a dialable number.
2. **The masked value is still a join key for debugging.** Two customer rows
   that mask to the same email are probably the same person — which is how
   duplicate-customer problems get spotted. Deleting the column destroys that.
3. **Deletion breaks the audit trail.** If the column is gone, you cannot later
   prove *what* you held and therefore cannot answer a data-subject request
   accurately. Masking is reversible at the source (`raw_customers`) under
   controlled access; deletion is not.

**Which Indian law applies, and what it expects.** The **Digital Personal Data
Protection Act, 2023 (DPDP Act)** — NammaMart is a Data Fiduciary processing the
digital personal data of Data Principals in India. Its core expectations here:

- **Lawful purpose and notice/consent** — collect for a stated purpose; an order
  history gathered to deliver groceries is not automatically available for
  marketing.
- **Purpose limitation and data minimisation** — this is what the masking
  implements. The reporting layer's purpose is *analytics*, and analytics does
  not need a phone number, so the reporting layer does not get one.
- **Accuracy** — relevant to our quality rules: the 3 future-dated
  `signup_date` values are a DPDP accuracy issue, not only a pipeline nuisance.
- **Storage limitation** — personal data must not be kept once the purpose is
  served. See retention below; this is the obligation our current design meets
  least well.
- **Security safeguards** and **breach notification to the Data Protection
  Board and affected principals**.
- **Data Principal rights** — access, correction and **erasure**. Erasure is the
  hard one for us: a request to erase a customer must reach `raw_customers`,
  which is otherwise append-only and never deleted. That tension is a real
  design gap and I would resolve it with a documented, logged, exception-only
  deletion path on the raw layer rather than pretending the audit rule wins.
- **Reasonable security** and accountability for processors — if the gateway or
  CRM is a third party, a data-processing agreement is needed.

## 5. Access control

| Layer | Who reads it | Why |
|---|---|---|
| `raw_customers` | **Nobody routinely.** Data engineer on-call + DPO, break-glass, access logged | It holds unmasked name, email and phone. This is the one table where "who can read it" has a legal answer, not a convenience answer |
| `raw_orders`, `raw_payments`, `raw_products`, `raw_stores` | Data engineering; finance on request for an audit | No direct identifiers, but raw and unvalidated — a finance user querying it would get the duplicate and bad rows and reach a wrong number |
| `stg_*` | Data engineering + analysts who understand `dq_status` | Safe (PII already masked) but easy to misuse: forget `WHERE dq_status <> 'QUARANTINED'` and you get the bad rows back in your total |
| `mart_*` / `etl_*` reporting tables | **Broad** — COO, store managers (own store), finance, analysts | This is the point of the mart: pre-filtered, pre-masked, hard to get wrong |
| Quarantine files + `stg_orders_quarantine` | Data engineering + the dataset owner whose file failed | The owner is the one who can get the source fixed. Note these files inherit the source's PII, so the `customers_failed_*.csv` file needs the same protection as `raw_customers` |
| `dq_results`, `pipeline_runs` | **Everyone, including the COO** | "Did today's run work?" should not require asking an engineer |

**Raw `customers` contains unmasked phone numbers — who, if anyone, should see
it?** Essentially no standing human access. Routine work never needs it, because
everything downstream reads the masked columns. It should be readable by the
pipeline's service identity (to build `stg_customers`) and by a named
break-glass role for two specific jobs: fulfilling a data-subject access or
erasure request, and investigating a suspected data-quality or fraud incident.
Both are exceptional, both should be ticketed, and both should be logged. If a
store manager can `SELECT phone FROM raw_customers`, the masking everywhere
else was decoration.

I will also flag the practical version of this: **the DuckDB warehouse file
itself is the access-control boundary.** DuckDB has no row-level security or
user accounts — anyone with the file has everything in it, including
`raw_customers`. For a real deployment that is the strongest argument for moving
the raw layer into a database with actual GRANTs. For this assignment it means
`include/warehouse_arnav_ganguly.duckdb` must not be committed to GitHub, and
that is enforced by `.gitignore`, which is a weak control standing in for a
real one.

## 6. Retention

| Data | Retention | What happens then |
|---|---|---|
| `raw_orders`, `raw_payments`, `raw_products`, `raw_stores` | **8 years** | Aligns with the Companies Act, 2013 requirement to preserve books of account for 8 financial years. Then deleted. These are financial records |
| `raw_customers` | **Shortest of the set — review at 12 months after the customer goes inactive** | DPDP storage limitation. Then the identifying columns are purged and only the `customer_id` shell is kept so historical orders still aggregate |
| `stg_*` | **Rebuilt every run; no independent retention** | Derived data. Dropping and rebuilding costs seconds, so there is nothing to retain |
| `mart_*` / `etl_*` | **3 years of history** | Enough for year-on-year comparison. Beyond that, keep a monthly aggregate and drop the daily grain |
| Quarantine files | **90 days** | Long enough for the dataset owner to fix the source and re-run. `customers_failed_*.csv` carries PII, so it gets the shorter of 90 days and the `raw_customers` rule |
| `dq_results`, `pipeline_runs` | **2 years** | These are the quality trend. They hold no PII and are tiny, so keeping them is nearly free and throwing them away destroys the only record of whether quality improved |

The honest gap: **none of this is implemented.** There is no retention job in
either DAG. Today the tables grow forever and `raw_customers` keeps unmasked
phone numbers indefinitely, which is the clearest DPDP non-compliance in the
current design. The fix is a scheduled `prune` DAG with per-table rules, and I
would put it in the first sprint after go-live rather than list it as done.

## 7. Secrets

**Where they live.** Nowhere in the code. The pipelines read runtime
configuration through `Variable.get()`:

```python
from airflow.models import Variable
Variable.get("nammamart_master_dq_policy", default_var="lenient")
```

For the bonus Slack alert, `SLACK_WEBHOOK_URL` is read the same way
(`include/slack_notifier.py` already follows this pattern) and set in the
Airflow UI under Admin → Variables, or injected from the environment. In
production the backing store would be a real secrets manager —
`AIRFLOW__SECRETS__BACKEND` pointed at AWS Secrets Manager, GCP Secret Manager
or Vault — so Airflow never persists the value in its own metadata database
either. Variables holding secrets should be named so Airflow masks them in logs
(Airflow redacts values whose key contains `secret`, `password`, `token`,
`apikey`, and so on).

**Why they must never be in the DAG file or on GitHub.** Four reasons that are
all about the same thing — a secret in git is not a secret:

1. **Git has no delete.** Committing a webhook URL and removing it in the next
   commit leaves it in the history, in every clone, and in every fork. The only
   real remediation is rotating the credential, which means the leak has a cost
   whether or not anyone exploited it.
2. **The blast radius is everyone with repo access**, which is a much larger set
   than everyone who should hold the credential — and for this assignment the
   repository is **public**.
3. **Rotation becomes a code deploy.** If the secret is a literal in
   `nammamart_etl_arnav_ganguly.py`, changing it needs a commit, a review and a
   release. That friction is why leaked credentials stay un-rotated for months.
4. **Secrets in code get copied.** Someone pastes the DAG into a ticket to ask
   for help, and the credential goes with it.

This is also why `include/data/` holding real-looking PII is itself a smell: the
assignment's `example.com` addresses are synthetic, but the same repository
layout in production would put customer emails in git. Real deployment reads the
source files from object storage with scoped credentials, not from a path inside
the checkout.

## 8. Audit — "show me a rejected order and prove it isn't in the revenue total"

Finance asks. Here is the complete answer for **`ORD001459`**.

**Exactly as received**, from the raw layer, untouched:

```sql
SELECT order_id, quantity, unit_price, order_amount, order_status,
       _source_file, _row_num
FROM raw_orders WHERE order_id = 'ORD001459';
```
```
ORD001459 | quantity '-3' | unit_price '531.0' | order_amount '-1593.0'
          | DELIVERED | orders.csv | line 815
```

**The rule it broke:**

```sql
SELECT failure_reason FROM stg_orders WHERE order_id = 'ORD001459';
-- Critical:quantity_positive
```

A quantity of −3 with a matching negative amount. Severity Critical, so the row
is quarantined — and per **client question 1** this is the case where I need the
client's answer: if ops uses negative quantities to record returns, this row
belongs in revenue as a negative line and quarantining it *overstates* September.

**Proof it is not in the CFO's total:**

```sql
SELECT count(*) FROM etl_fact_orders WHERE order_id = 'ORD001459';                       -- 0
SELECT count(*) FROM stg_orders
 WHERE order_id = 'ORD001459' AND dq_status <> 'QUARANTINED';                            -- 0
SELECT check_name, severity, rows_failed FROM dq_results
 WHERE check_name = 'orders.quantity_positive';          -- Critical, 10 rows, both DAGs
```

Zero rows in the ETL fact table, zero rows eligible for the ELT mart, and the
check that caught it is recorded for the run with a count of 10 — so this is one
of a known class, not a one-off someone noticed by luck.

**How each pipeline answers.**

- **ELT answers entirely in SQL**, in one session, including the as-received
  text and the file line number. Raw is in the warehouse, so "show it as
  received" and "prove it's excluded" are two queries against the same database.
- **ETL answers in two places.** The exclusion proof is a query
  (`etl_fact_orders` has no such row) and `dq_results` names the rule, but "show
  it exactly as received" means opening
  `include/quarantine/arnav_ganguly/orders_failed_<run_id>.csv` — a file on
  disk, outside the warehouse, which must have been retained. That file does
  carry the full original row plus `failure_reason`, so the answer exists; it is
  just not SQL, and it depends on a file nobody has pruned.

For an auditor who wants to re-derive the answer themselves six months later,
ELT is materially better. This is the auditability half of the Part G argument.

## 9. Change history — an order DELIVERED on 3 September, RETURNED on 1 October

**What each pipeline shows for September after the incremental load.**

Both show the **same restated number**, because both key on `order_id` and both
take the latest version of the row:

```
September revenue as first published : Rs 1,135,628.80
the 10 restated orders are worth     : Rs     4,193.70   (all 10 passed DQ)
September revenue after restatement  : Rs 1,131,435.10
```

The returned orders keep their original September `order_date`, so flipping them
to RETURNED makes `net_revenue` zero *on the September day they happened*.
September's reported revenue therefore **drops by Rs 4,193.70 a month after the
month closed** — in both pipelines. Example: `ORD000337`, 5 September, store
S09, Rs 1,748.00, now RETURNED.

**Which answer does finance want?** Neither, on its own — and that is the real
finding. Finance needs **both**, for different purposes:

- The **as-first-published** figure (Rs 1,135,628.80) is what was reported to
  the board, filed in the September management accounts and possibly used in a
  GST return. It must not change retrospectively, or the books stop tying to
  what was filed.
- The **current-truth** figure (Rs 1,131,435.10) is what the business is
  actually worth and what the returns rate should be measured against.

A single mutable number cannot be both. Silently restating a closed period is
the failure mode here: the COO's September deck and the warehouse would disagree
a month later with nothing to explain the gap.

**How I would keep both views.** Three options, in increasing cost:

1. **Recognise the return in the period it was reported** (the conventional
   accounting answer). Keep September's `net_revenue` as published and book the
   Rs 4,193.70 as an October contra-revenue line, carrying both `order_date` and
   a `returned_date`. September stops moving; October shows the reversal. This
   is almost certainly what finance wants and it needs a `returned_date` column
   the increment file does not currently give us — **a question for the client.**
2. **Snapshot the mart.** Add a `period_closed_snapshot` table written once when
   a month closes. The live mart restates freely; the snapshot preserves what was
   published. Cheap, and it answers "what did we report on 1 October?" exactly.
3. **SCD Type 2 on the fact table** (the bonus task). Replace the upsert with
   `valid_from`/`valid_to`/`is_current` rows so every state of every order is
   retained and any as-at date is reconstructible: `WHERE '2026-09-30' BETWEEN
   valid_from AND valid_to`. The most complete and the most expensive — it
   changes the grain of the fact table and every downstream query.

What the current design already gives us, and should not be undersold: the ELT's
`raw_orders` holds **both versions of the row** — the September DELIVERED record
from `orders.csv` and the October RETURNED record from the increment file — each
with its own `_source_file` and `_loaded_at`. So the as-first-published figure is
*recoverable* today without any schema change.

It is not a naive sum over raw, though. Raw deliberately contains the bad rows,
so `sum(order_amount) WHERE _source_file = 'orders.csv'` returns
**Rs 1,219,020.88** — Rs 83,392.08 too high, because it includes the duplicates,
the negative quantities and the formula mismatches. Reconstructing the published
number means replaying the quality verdict against only the pre-increment rows:

```sql
-- September revenue as it stood before the increment file landed
WITH pre_increment AS (
    SELECT r.order_id, r.order_status, r.order_amount,
           row_number() OVER (PARTITION BY r.order_id
                              ORDER BY r._row_num DESC) AS rn
    FROM raw_orders r
    WHERE r._source_file = 'orders.csv'       -- exclude the increment file
)
SELECT round(sum(TRY_CAST(p.order_amount AS DOUBLE)), 2) AS september_as_published
FROM pre_increment p
JOIN stg_orders s ON s.order_id = p.order_id AND s.dq_status <> 'QUARANTINED'
WHERE p.rn = 1                                 -- last copy wins, as staging does
  AND upper(trim(p.order_status)) = 'DELIVERED';
-- 1135628.80, matching what was published
```

Two details that query has to get right, and that are worth stating because
getting them wrong is how a "simple" audit query produces a confident wrong
number: `rn = 1` reproduces staging's last-copy-wins rule, without which the 12
duplicated orders are counted twice; and the join to `stg_orders` reapplies the
quality verdict rather than trusting raw.

The ETL cannot do this. Its fact table holds one row per order and the upsert
overwrote the September version — the pre-restatement number is gone from the
warehouse, reconstructible only by re-reading an `orders.csv` that may since have
been replaced. **This is the clearest single illustration of what the raw layer
buys you**, and it is the argument that decides Part G.

---

# Part F — Observability

## How I ran these

Eleven experiments — the ten the brief specifies plus an **8b** of my own — each
against both DAGs, driven by [`scripts/part_f_experiments.py`](scripts/part_f_experiments.py)
and reported by [`scripts/part_f_report.py`](scripts/part_f_report.py).

Scripted rather than clicked, for three reasons. Twenty-two runs is too many to
do reliably by hand. Every experiment has to put its source files back
afterwards, and a forgotten restore silently poisons the next experiment. And
an experiment nobody can re-run is an anecdote, not evidence — a grader or a
client can reproduce every number below with one command.

Observations come from three places, none of them a screenshot:

| Source | What it gives |
|---|---|
| Airflow **metadata database** (`task_instance`, `dag_run`) | real task states, `try_number`, durations, and genuine execution-window overlap |
| The **DuckDB warehouse** | row counts, revenue, `dq_results`, `pipeline_runs` |
| **Task logs** | the error text for schema and dependency failures |

### One methodology decision that changed the results

I started with `astro dev run dags test`, which is what the brief suggests for
checking a DAG runs. Experiment 1 reported `parallel: []` — nothing ran
concurrently. That is because **`dags test` executes every task sequentially in
a single process.** It therefore hides the two things Part F is most about:
which tasks actually run in parallel, and how retries are spaced in wall-clock
time.

Switching to `dags trigger` and the real scheduler changed the outcome of four
experiments and exposed three defects. **The headline result of Part F is that
running the pipelines for real found four bugs that ~700 lines of passing
assertions and `dags test` both missed.** That is worth stating plainly, because
it is the whole argument for why this part is worth 15 marks.

## The five defects, found and fixed

| # | Defect | Found by | Why it mattered |
|---|---|---|---|
| 1 | DuckDB write-lock contention *within* the ELT | exp 1, real scheduler | ELT loaded **nothing**: 2 failed, 10 `upstream_failed` |
| 2 | False-green dag runs | exp 4, 6, 7 | A run that loaded 0 rows reported `success` |
| 3 | No volume / freshness / distribution checks | exp 7, 8, 9 | A 98% volume collapse and a 100× price drift both ran green |
| 4 | Stale audit row after recovery | exp 10 | `pipeline_runs` said `FAILED, loaded=0` while the fact table held 2310 rows |
| 5 | Write-lock contention *between the two DAGs* | restoring the warehouse after the policy comparison | Both DAGs are scheduled at the same minute and write the same file |

**Defect 1 — concurrent writers against a single-writer database.**
The ELT's five `load_raw_*` tasks have no dependencies on each other, so Airflow
ran all five at once, and all five write the same DuckDB file. The task log says
`IOException: Could not set lock on file`. The retry pattern is textbook
contention:

```
load_raw_products   success  try 1
load_raw_customers  success  try 2
load_raw_payments   success  try 3
load_raw_orders     failed   try 3   (exhausted)
load_raw_stores     failed   try 3   (exhausted)
```

The ETL is immune by accident of shape: its five extracts write Parquet, and
`validate` — the first task to touch DuckDB — is serial.

Fixed with an Airflow **pool, `duckdb_writer`, of 1 slot**, applied to all nine
warehouse-touching ELT tasks. My first attempt pooled only the raw loaders and
still failed, because the **five mart builds** fan out and write too. Retries
alone would have been the wrong fix: they convert a hard failure into a flaky
one. The pool is the correct mechanism for a resource that tolerates exactly one
concurrent user, and it keeps one task per table so the DAG still documents its
sources.

```bash
astro dev run pools set duckdb_writer 1 "single-writer warehouse"
```

**Defects 2 and 4 — one change fixed both.**
`run_audit` is the only leaf task and carries `trigger_rule="all_done"`, so it
succeeded even when every other task failed. Airflow derives dag-run state from
its leaves, so a run that loaded nothing reported green. Criterion 9 says anyone
can see whether today's run worked — and a green run that loaded nothing is
*worse* than a red one, because nobody goes looking.

Separately, because `run_audit` had *succeeded* during the broken run,
`tasks clear --only-failed --downstream` skipped it on recovery. So after a
successful recovery `pipeline_runs` still read `status=FAILED, rows_loaded=0`
while `etl_fact_orders` held 2310 rows.

The fix is three lines: `run_audit` writes its row **and then raises** when it
records FAILED. The ordering matters — an audit task that raises before
recording leaves no evidence of the failure it is reporting. And because the
task is now genuinely failed, a recovery clear includes it, so the audit row
restates itself. Measured:

```
exp 4   before:  run_state=success   (1 failed, 5 upstream_failed, 5 success)
        after:   run_state=failed    (2 failed, 5 upstream_failed, 4 success)

exp 10  before:  re-ran 6, run_audit untouched,
                 pipeline_runs FAILED/loaded=0  vs 2310 rows actually present
        after:   re-ran 7 incl. run_audit, final=success,
                 pipeline_runs SUCCESS/loaded=2310   <- agrees with reality
```

**Defect 3 — every rule I had written was row-level.** Covered under experiments
7, 8 and 8b below.

**Defect 5 — the two DAGs collide with each other at 09:00.**
This one was not found by an experiment. I noticed it while restoring the
warehouse after the policy comparison, by triggering both DAGs and realising
they had both succeeded *by luck*.

Both DAGs are scheduled `0 9 * * 1-6`. At 09:00 IST they therefore run
**concurrently**, and both write the same single-writer DuckDB file.
`max_active_runs=1` does not help: it is per-DAG and says nothing about two
different DAGs. After fixing defect 1 I had pooled only the ELT's tasks, so the
ETL's `validate`, `transform`, `load`, `reconcile`, `report` and `run_audit`
were still free to write the warehouse while the ELT held the lock.

The fix relies on a property of Airflow pools that makes this clean: **pools are
global, not per-DAG.** Putting both DAGs' warehouse-writing tasks in the same
1-slot `duckdb_writer` pool serialises access across the pair. The five ETL
extracts stay out of the pool, because they write Parquet rather than the
warehouse — so the DAG keeps its parallel extract, which is an acceptance
criterion.

Verified by triggering both DAGs simultaneously:

```
nammamart_etl_arnav_ganguly  success  start 15:56:08.784953
nammamart_elt_arnav_ganguly  success  start 15:56:08.776714   <- 8ms apart
ETL fact 2310 rows / Rs 1,135,628.80
ELT mart 2088 delivered / Rs 1,135,628.80      agree: True
```

This is the defect I would most expect to have reached production. It cannot be
reproduced by running one DAG, it cannot be reproduced by `dags test`, and it
only bites on the days both DAGs happen to interleave badly — which is to say,
intermittently, at 09:00, on the number the COO is reading. The lesson for the
client is specific: **two pipelines sharing an embedded database are one
pipeline with extra steps**, and the real fix is the warehouse migration in
Part G, not a bigger pool.

## Measured results

Everything in this section is generated from `include/part_f_results.json` by
`scripts/part_f_report.py`, so the numbers in the submission are the numbers the
experiments produced. All runs below are from one code version — the post-fix
code — with the pre-fix results kept in `include/part_f_results_before_fix.json`
for the before/after comparisons above.

Two reading notes. Experiment 10 runs the **ETL only**, because `Clear` is about
Airflow's recovery semantics rather than a difference between the pipelines, so
its row shows the ELT tables empty. And `run_state` is now `failed` for
experiments 4 and 6 — before the defect-2 fix both reported `success`.

### Task states, per experiment, per DAG

| # | Experiment | DAG | Run state | Task states | Max try | Wall |
|---|---|---|---|---|---|---|
| 1 | Happy path | ETL | `success` | 11 success | 1 | 14.2s |
| 1 | Happy path | ELT | `success` | 16 success | 1 | 8.4s |
| 2 | Re-run (idempotency) | ETL | `success` | 11 success | 1 | 10.3s |
| 2 | Re-run (idempotency) | ELT | `success` | 16 success | 1 | 8.7s |
| 3 | Incremental load | ETL | `success` | 11 success | 1 | 9.1s |
| 3 | Incremental load | ELT | `success` | 16 success | 1 | 7.7s |
| 4 | Missing source file | ETL | `failed` | 2 failed, 5 upstream_failed, 4 success | 3 | 4.9s |
| 4 | Missing source file | ELT | `failed` | 2 failed, 10 upstream_failed, 4 success | 3 | 3.7s |
| 5 | New bad data | ETL | `success` | 11 success | 1 | 9.0s |
| 5 | New bad data | ELT | `success` | 16 success | 1 | 8.6s |
| 6 | Schema change | ETL | `failed` | 2 failed, 4 upstream_failed, 5 success | 3 | 7.0s |
| 6 | Schema change | ELT | `failed` | 2 failed, 8 upstream_failed, 6 success | 3 | 4.2s |
| 7 | Volume anomaly | ETL | `success` | 11 success | 1 | 10.8s |
| 7 | Volume anomaly | ELT | `success` | 16 success | 1 | 8.3s |
| 8 | Distribution drift | ETL | `success` | 11 success | 1 | 9.5s |
| 8 | Distribution drift | ELT | `success` | 16 success | 1 | 9.8s |
| 9 | Freshness | ETL | `success` | 11 success | 1 | 10.8s |
| 9 | Freshness | ELT | `success` | 16 success | 1 | 7.5s |
| 11 | Distribution drift, CONSISTENT (8b) | ETL | `success` | 11 success | 1 | 7.8s |
| 11 | Distribution drift, CONSISTENT (8b) | ELT | `success` | 16 success | 1 | 6.8s |

### Warehouse state after each experiment

| # | Experiment | Warehouse |
|---|---|---|
| 1 | Happy path | ETL fact 2310 / Rs 1,135,628.80 · ELT staged 2400 / Rs 1,135,628.80 · raw_orders 2412 |
| 2 | Re-run (idempotency) | ETL fact 2310 / Rs 1,135,628.80 · ELT staged 2400 / Rs 1,135,628.80 · raw_orders 4824 |
| 3 | Incremental load | ETL fact 2460 / Rs 1,211,087.30 · ELT staged 2550 / Rs 1,211,087.30 · raw_orders 7396 |
| 4 | Missing source file | ETL fact 0 / Rs 0.00 · ELT staged 0 / Rs 0.00 · raw_orders 2412 |
| 5 | New bad data | ETL fact 2310 / Rs 1,135,628.80 · ELT staged 2405 / Rs 1,135,628.80 · raw_orders 2417 |
| 6 | Schema change | ETL fact 0 / Rs 0.00 · ELT staged 0 / Rs 0.00 · raw_orders 2412 |
| 7 | Volume anomaly | ETL fact 49 / Rs 24,014.40 · ELT staged 50 / Rs 24,014.40 · raw_orders 50 |
| 8 | Distribution drift | ETL fact 2154 / Rs 1,059,076.45 · ELT staged 2400 / Rs 1,059,076.45 · raw_orders 2412 |
| 9 | Freshness | ETL fact 2317 / Rs 1,138,845.95 · ELT staged 2400 / Rs 1,138,845.95 · raw_orders 2412 |
| 10 | Recovery via Clear | ETL fact 2310 / Rs 1,135,628.80 · ELT staged 0 / Rs 0.00 · raw_orders 0 |
| 11 | Distribution drift, CONSISTENT (8b) | ETL fact 2310 / Rs 8,714,311.45 · ELT staged 2400 / Rs 8,714,311.45 · raw_orders 2412 |

### Quarantine counts per experiment

| # | Experiment | orders | payments | customers | products |
|---|---|---|---|---|---|
| 1 | Happy path | 102 | 21 | 5 | 2 |
| 2 | Re-run (idempotency) | 102 | 21 | 5 | 2 |
| 3 | Incremental load | 112 | 21 | 5 | 2 |
| 5 | New bad data | 107 | 21 | 5 | 2 |
| 7 | Volume anomaly | 1 | 21 | 5 | 2 |
| 8 | Distribution drift | 258 | 21 | 5 | 2 |
| 9 | Freshness | 95 | 21 | 5 | 2 |
| 11 | Distribution drift, CONSISTENT (8b) | 102 | 21 | 5 | 2 |

### Experiment 10 — recovery via Clear, measured

- failed run: `manual__2026-10-13T15:40:12.964195+00:00` → 2 failed, 5 upstream_failed, 4 success
- command: `airflow tasks clear nammamart_etl_arnav_ganguly --only-failed --downstream --yes`
- re-ran (7): `extract_products`, `load`, `reconcile`, `report`, `run_audit`, `transform`, `validate`
- untouched (4): `extract_customers`, `extract_orders`, `extract_payments`, `extract_stores`
- final run state: `success`

### Observed errors, by experiment


## Experiment results

Each entry is **Expected → Observed → Where I saw it → What I would change.**

### 1. Happy path

- **Expected** — both DAGs green; the five extracts and five raw loads run
  concurrently; 2310 ETL fact rows, 2400 ELT staged, Rs 1,135,628.80 both.
- **Observed** — ETL 11/11 success. **ELT failed**: 4 success, 2 failed, 10
  `upstream_failed`, `max_try=3`, `raw_orders=0`, `stg_orders=0`. After the pool
  fix: ELT 16/16 success, `max_try=1`, and both pipelines at
  **Rs 1,135,628.80** over 2088 delivered orders. ETL's five extracts overlap in
  wall-clock time; the ELT's writers are now deliberately serial.
- **Where** — Airflow Graph view (the red tasks), the task log line
  `IOException: Could not set lock on file`, and `task_instance.try_number` in
  the metadata DB. The row counts came from the `[load]` and `[staging]` log
  lines and from the warehouse.
- **What I would change** — the real fix at scale is a warehouse with MVCC
  (Postgres, BigQuery, Snowflake), where the pool becomes unnecessary. I would
  also add a DAG-level assertion that no two tasks writing the warehouse are
  schedulable together, so this cannot regress silently when someone adds a
  sixth source file.

### 2. Re-run (idempotency)

- **Expected** — fact row counts and revenue unchanged; ELT raw tables grow.
- **Observed** — ETL fact stayed at **2310 rows / Rs 1,135,628.80**, identical
  to the digit. ELT `stg_orders` stayed **2400** and the mart stayed
  **Rs 1,135,628.80**, while **`raw_orders` grew 2412 → 4824**.
- **Where** — the `[load] etl_fact_orders: 2310 -> 2310 rows (2310 restated, 0 new)`
  log line, and `SELECT count(*) FROM raw_orders`.
- **Is the raw growth correct?** **Yes, and it is the point.** Criterion 3 says
  every raw record is kept for audit, so the raw layer must be append-only; two
  runs of the same file legitimately give two records of having received it,
  each with its own `_loaded_at` and `_run_id`. Idempotency belongs one layer
  up: `stg_orders` upserts on `order_id` with the most recent copy winning, so
  the mart does not move. A pipeline where re-running changes the *reported
  number* is broken; a pipeline where re-running adds to the *audit trail* is
  behaving.
- **What I would change** — `raw_orders` growing without bound is the storage
  cost of auditability and needs the retention job from Part E.6, which does not
  exist yet. I would also partition raw by `_loaded_at` date so pruning is a
  partition drop rather than a `DELETE`.

### 3. Incremental load

- **Expected** — 150 new orders inserted, 10 September orders restated to
  RETURNED, no duplicates.
- **Observed** — exactly that. ETL `2310 -> 2460 rows (2310 restated, 150 new)`;
  ELT `stg_orders 2400 -> 2550 (+150)`. All **10** corrections flipped
  DELIVERED → RETURNED, each appearing exactly **once**. Both pipelines then
  reported **Rs 1,211,087.30**. `raw_orders` reached 7396 across three runs.
- **Where** — the `[load]`/`[staging]` log lines, and a per-`order_id` count
  against `stg_orders` and `etl_fact_orders`.
- **Inserted vs updated** — 150 inserted, 10 updated. The ETL reports "2310
  restated" because its load deletes and re-inserts the whole batch by
  `order_id`; the 10 *semantic* corrections are the ones whose `order_status`
  actually changed. That distinction is worth noting: "rows restated" and "rows
  whose values changed" are different numbers, and only the second is
  interesting to the client.
- **What I would change** — the ETL's log should report the semantic count, not
  the mechanical one. As written it would say "2310 restated" on a day when
  nothing changed, which trains people to ignore it.

### 4. Missing source file

- **Expected** — `extract_products` / `load_raw_products` retries twice at
  1-minute intervals then fails; downstream goes `upstream_failed`; `run_audit`
  still writes a FAILED row via `all_done`.
- **Observed** — precisely that, and then the defect. ETL: 1 failed, 5
  `upstream_failed`, 5 success, `try_number=3`, retries one minute apart. ELT:
  1 failed, 10 `upstream_failed`, 5 success. **But `run_state=success` on both.**
  Nothing reached any reporting table (`etl_fact_rows=0`,
  `elt_mart_revenue=0`), yet the UI was green. After the fix both report
  `run_state=failed`.
  One detail worth keeping: `raw_orders=2412` in the ELT even on the failed run
  — the orders raw load completed before products failed. Partial raw, zero
  mart. Criterion 2 held while criterion 9 failed.
- **Where** — `up_for_retry` then `failed` in Grid view, the retry timestamps in
  `task_instance.start_date`, and `FileNotFoundError: source file missing:
  /usr/local/airflow/include/data/nammamart/products.csv` in the task log.
- **What alerted me?** **Nothing did, and that is the finding.** There is no
  email, no Slack message, and before the fix not even a red dag run. The only
  way to know was to go and look. The bonus Slack alert
  (`include/slack_notifier.py`, `SLACK_WEBHOOK_URL` via `Variable.get()`) is
  wired for the trainer's DAG but not mine — and an SLA this tight needs a push
  notification, not a dashboard someone might open.
- **What I would change** — besides the alert: an `on_failure_callback` on
  `default_args` so every task failure notifies, and an **SLA miss** on the DAG
  so a run that has not *finished* by 09:15 IST alerts even if no task has
  failed yet. A pipeline that fails loudly at 09:05 and a pipeline that hangs
  silently until 10:00 look identical to the COO otherwise.

### 5. New bad data

- **Expected** — all five planted rows quarantined with the right reason, and
  each reason's count in `dq_results` up by one.
- **Observed** — a clean arithmetic proof. Every count moved by exactly five and
  revenue did not move at all:

  | | before | after |
  |---|---|---|
  | `raw_orders` | 2412 | **2417** (+5, raw keeps everything) |
  | `stg_orders` | 2400 | **2405** (+5) |
  | ELT quarantined | 90 | **95** (+5) |
  | ETL `orders` quarantine file | 102 | **107** (+5) |
  | `etl_fact_orders` | 2310 | **2310** (+0) |
  | net revenue | 1,135,628.80 | **1,135,628.80** (unchanged) |

  The five defects — null `customer_id`, `quantity -4`, unknown store `S99`,
  status `SHIPPED`, a 2027 timestamp — were each caught by their own rule:
  `customer_id_present`, `quantity_positive`, `store_id_known`,
  `status_in_allowed_set`, `order_ts_not_future`.
- **Where** — the quarantine CSV's `failure_reason` column, `stg_orders`'s
  `failure_reason`, and `dq_results` for the per-check counts.
- **Did `dq_results` capture it?** Yes — each of the five checks' `rows_failed`
  incremented, and every planted row is retrievable by `order_id` from both the
  quarantine file and `raw_orders`.
- **What I would change** — nothing in the detection. But five new bad rows
  arriving on one day should itself be a signal: I would add a check on the
  *rate* of quarantine, so a jump from 4% to 20% rejected alerts even when every
  individual rule is working exactly as designed.

### 6. Schema change

- **Expected** — fails loudly on the missing `order_amount` rather than loading
  nulls.
- **Observed** — it failed loudly, and the two pipelines failed in **different
  places**, which is the interesting part.
  - **ETL**: 6 success, 1 failed, 4 `upstream_failed`. The five extracts
    succeeded (the file exists; only a column was renamed) and **`validate`**
    failed — the first task that needs the column.
  - **ELT**: 7 success, 1 failed, 8 `upstream_failed`. All five raw loads *and*
    `stage_masters` succeeded, and **`stage_orders`** failed. The raw layer
    accepted the renamed column without complaint, because it reads
    `all_varchar` and takes its column names from the file header — it has **no
    schema contract at all**.
- **Where** — Graph view showed which task went red; the task log gave the
  pandas `KeyError: 'order_amount'` for the ETL and a DuckDB binder error naming
  the missing column for the ELT.
- **Loudly or silently?** Loudly, in both. Reading the CSV with `dtype=str` and
  no column coercion means an absent column raises rather than materialising as
  NaN. Had I read with type inference and `.get()`-style access,
  `order_amount` would have become all-null and the pipeline would have
  cheerfully reported **zero revenue** for the day.
- **Two caveats I verified rather than assumed.** First, this run started from a
  fresh warehouse, so `raw_orders` was *created* with the `total` column;
  against an existing warehouse the `INSERT` hits a column mismatch and fails at
  the raw load instead — an earlier, blunter failure, and a different task goes
  red. Second, schema drift reaching the raw layer means the "exact copy of what
  we received" no longer has the same shape as yesterday's, so the audit layer
  itself has drifted.
- **What I would change** — two things. The raw layer needs an explicit
  **expected-columns assertion** at load time, so a schema change is caught at
  the door with a message naming the added and removed columns, instead of
  surfacing three tasks later as a missing-identifier error.

  The retries are also waste here — three attempts against a renamed column can
  never succeed, and they cost two minutes of a 09:00 SLA. But I have **not**
  set `retries=0` on `validate`, and the reason is a genuine trade-off rather
  than an oversight: `validate` also writes `dq_results` and the quarantine
  files, so it has real I/O that a retry legitimately protects. Removing its
  retries to fail faster on schema errors would remove resilience to a transient
  write failure. The right fix is not fewer retries but an earlier, cheaper
  check — the schema assertion above, on a task that does nothing but assert,
  which *can* safely carry `retries=0`.

  I did apply `retries=0` to `run_audit` in both DAGs, where there is no
  trade-off: that task fails as a function of the run's outcome, so a retry can
  never succeed. I only noticed because the fix for defect 2 made it retry to
  `try_number=3`, delaying the client's only failure signal by two minutes.

### 7. Volume anomaly

- **Expected** — the ETL's volume check warns; the ELT has no equivalent.
- **Observed** — worse than expected. With **50 orders** instead of 2412:
  ETL fact **49 rows**, ELT staged **50**, both reporting **Rs 24,014.40**
  against a normal Rs 1,135,628.80 — and **both DAGs ran completely green**,
  11/11 and 16/16 success. The ETL logged its warning; the ELT said nothing.
  A 98% volume collapse and a revenue figure 47× too low produced two green
  pipelines.
  After adding the checks, both pipelines now report
  `orders.batch_volume_not_collapsed` → **FAIL**, `rows_checked=50`, and
  `orders.batch_freshness_within_1_day` → **FAIL**.
- **Where** — the `[dq] WARNING orders.batch_volume_not_collapsed` log line and
  a `dq_results` query filtered to `dimension='Volume'`.
- **What I added** — the ELT now computes volume in SQL inside the `dq_results`
  step, compared against **this DAG's own trailing median** rather than a fixed
  floor, so it keeps working as NammaMart grows; the 500 fallback applies only
  on the very first run, when there is no history.
- **What I would change** — these are Warning severity, so the run is still
  green and someone must read `dq_results`. For a 09:00 SLA a >50% volume drop
  should page a human, which again means the alerting that does not exist yet.
  The threshold is also crude: a 40% drop on Diwali is normal and a 40% drop on
  a Tuesday is an incident, and nothing here knows the difference.

### 8. Distribution drift — and 8b, the version that matters

- **Expected** — some check catches a 100× price on one store; a mart metric
  moves.
- **Observed (8, the brief's literal mutation: `unit_price` × 100 only)** —
  `amount_matches_formula` caught all **156** of S01's rows, because scaling
  `unit_price` without scaling `order_amount` breaks the amount formula. So
  revenue **fell**: `etl_fact_orders 2310 -> 2154`, quarantined `90 -> 246`,
  revenue `Rs 1,135,628.80 -> Rs 1,059,076.45` (−6%). Both pipelines agreed on
  the degraded figure.
- **The problem with that result** — the check that appears to catch drift is
  really an *arithmetic consistency* check wearing a disguise. A real pricing
  incident — a price list loaded with the wrong multiplier — is internally
  consistent. So I ran **8b**: scale `unit_price` **and** `order_amount`
  together.

  | Same 100× error on S01 | Rows caught | Reported revenue | Distribution check |
  |---|---|---|---|
  | 8 — inconsistent (as specified) | 156 | Rs 1,059,076.45 (−6%) | n/a |
  | **8b — consistent (realistic)** | **0** | **Rs 8,714,311.45 (+667%)** | **FAIL, after the fix** |

  Before the fix, 8b passed **every** row-level check in both pipelines: the
  formula holds, prices are positive, quantities and discounts are untouched.
  A seven-fold revenue overstatement would have reached the CFO's dashboard
  silently. That is the single most dangerous result in Part F, and the brief's
  literal mutation would have hidden it.
- **Where** — `dq_results` filtered to `dimension='Distribution'`
  (`rows_checked=15` stores, `rows_failed=1`), and `mart_daily_store_kpis` for
  S01's revenue.
- **Which metric moved, and by how much** — S01's own revenue went
  **Rs 84,604.30 → Rs 8,460,430.00 (×100)**, dragging total reported revenue
  from Rs 1.22M to Rs 9.59M at the source-data level and to Rs 8,714,311.45
  through the pipeline.
- **What I added** — `orders.store_value_distribution` in both pipelines
  (pandas in ETL, SQL in ELT): each store's mean delivered order value against
  the **median store's**, flagged beyond 5× or below 1/5. Cross-sectional on
  purpose — comparing stores to each other needs no history, which matters on
  day one when there is none. Severity **Warning**, also on purpose: a genuine
  promotion can move a store's average, and a Critical rule here would
  quarantine a real day's revenue to catch a maybe.
- **What I would change** — the 5× threshold is a blunt instrument that would
  miss a 3× error. The better version compares each store against **its own
  trailing distribution** and alerts on a z-score, which `dq_results` now has
  the history to support. I would also check `unit_price` against the product's
  `mrp` — a unit price far above MRP is impossible by definition, and that one
  rule would have caught 8b at row level.

### 9. Freshness

- **Expected** — the timeliness check warns with `staleness_days ≈ 30`.
- **Observed** — it warned, and something I did not predict happened as well.
  `[dq] WARNING orders.batch_freshness_within_1_day: rows=2412
  latest_order_ts=... staleness_days=30`. But the fact table **grew**:
  `2310 -> 2317 rows`, quarantined `90 -> 83`, revenue
  `Rs 1,135,628.80 -> Rs 1,138,845.95` (**+Rs 3,217.15**).
- **Why** — ageing every order by 30 days moved the **7 future-dated orders**
  from the Part 0 log out of the future, so `order_ts_not_future` stopped firing
  and they loaded.
- **Where** — the `[dq] WARNING` log line, `dq_results` for the timeliness
  dimension, and the fact-table counts.
- **The lesson is about the check, not the data** — a rule that compares against
  `now()` is sensitive to the *absolute* dates in the file, so the same row can
  pass or fail depending on when the pipeline runs. That is correct for a
  freshness check and a trap for a validity one: if NammaMart ever backfills a
  historical month, those 7 rows silently become acceptable. Worth raising with
  the client rather than treating my own test mutation as the discovery.
- **What I would change** — judge `order_ts_not_future` against the **data
  interval's** end rather than wall-clock `now()`, so the verdict on a row is
  reproducible regardless of when the DAG runs. That is also what makes a
  backfill give the same answer as the original run.

### 10. Recovery via Clear

- **Expected** — after fixing the cause, clearing only the failed task re-runs
  it and its downstream, leaving the successful tasks alone.
- **Observed** — correct recovery, plus defect 4. With `products.csv` renamed, a
  triggered run failed (1 failed, 5 `upstream_failed`, 5 success). I restored
  the file and ran
  `airflow tasks clear nammamart_etl_arnav_ganguly --only-failed --downstream --yes`:

  ```
  re-ran (7):     extract_products, validate, transform, load,
                  reconcile, report, run_audit
  untouched (4):  extract_customers, extract_orders,
                  extract_payments, extract_stores
  final run_state=success
  ```

  The four healthy extracts kept `success` and `try_number=1` — they were not
  re-fetched. Only the failed task and its downstream re-ran.
  **Before the defect-2 fix**, `run_audit` was in the untouched list (it had
  "succeeded"), so `pipeline_runs` still read `FAILED, rows_loaded=0` while
  `etl_fact_orders` held 2310 rows. After the fix the audit row restates itself
  to `SUCCESS, rows_loaded=2310`.
- **Where** — `task_instance.try_number` and `start_date` before and after the
  clear, Grid view showing the cleared column, and `pipeline_runs`.
- **What I would change** — document the recovery runbook, because the flag
  choice has consequences: `--only-failed --downstream` is right *now*, but it
  was subtly wrong before the audit task failed with the run. A one-page runbook
  ("restore the file, clear failed+downstream, confirm `pipeline_runs` agrees
  with `etl_fact_orders`") is the difference between a 5-minute recovery and an
  on-call engineer guessing at 08:50.

## The five pillars of data observability

| Pillar | Coverage | How |
|---|---|---|
| **Freshness** | 🟢 Good | `batch_freshness_within_1_day` in both pipelines; `_loaded_at` on every raw row; `pipeline_runs` start/end times |
| **Volume** | 🟡 Adequate | `batch_volume_not_collapsed` against the DAG's trailing median, both pipelines; `rows_extracted`/`rows_loaded`/`rows_quarantined` per run |
| **Schema** | 🔴 **Weakest** | Nothing declares an expected schema. Failures are *loud* but late and badly described |
| **Distribution** | 🟡 Adequate | `store_value_distribution`, cross-sectional, 5× threshold — catches an order of magnitude, would miss 3× |
| **Lineage** | 🟢 **Strongest (ELT)** | `raw_*` keeps `_source_file`, `_row_num`, `_loaded_at`, `_run_id`, so any mart number traces to a file and line number |

**Strongest: lineage, in the ELT.** Part E.3 traces Rs 6,683.00 on
`mart_daily_store_kpis` to three orders and then to `orders.csv` **lines 259,
928 and 32** — in two SQL queries, inside one database. The ETL reaches the file
via `_source_file` but cannot give the line or the pre-transformation text,
because the original row was cast in pandas and never persisted.

**Weakest: schema.** Experiment 6 is the proof. Neither pipeline declares what
it expects, so a renamed column is discovered by whichever task happens to
reference it first — `validate` in the ETL, `stage_orders` in the ELT — and the
error names a missing identifier rather than saying "the export changed shape".
Worse, the ELT's raw layer *accepts* the drift, so the audit copy of
`orders.csv` silently stops matching yesterday's. Three things would fix it, in
order of value: an expected-columns assertion at raw load naming added and
removed columns; a `schema_history` table recording each file's column list per
run, making drift queryable rather than discovered; and `retries=0` on
validation tasks so a schema failure fails in seconds instead of three minutes.

The honest summary is that this pipeline is **strong on explaining the past and
weak on noticing the present**. Lineage and audit are genuinely good: I can
reconstruct any number and prove why any row was rejected. But every detection
mechanism here is a *pull* — someone has to open the UI or query `dq_results`.
There is no push. For a 09:00 SLA feeding a 09:30 meeting, that is the gap I
would close first, before any of the refinements above.

---

# Part G — Recommendation to the client

**To:** Sameer, CTO, NammaMart Quick Commerce
**From:** Arnav Ganguly, Forward Deployed Engineer
**Re:** ETL or ELT for the 9 AM operations report
**Date:** 13 October 2026

## Recommendation

**Adopt ELT, with the ETL's validation rules kept as a gate in front of the
mart.** Build raw → staging → mart in the warehouse, and keep the discipline
that no row reaches a reporting table until it has passed every Critical check.

You do not have to choose between Finance's requirement and Analytics'. Both
pipelines are running and they produce **identical numbers** — 2,460 fact rows,
2,228 delivered orders, Rs 1,211,087.30, and all seven category margins agree to
the paisa. The quality guarantee Finance wants is not a property of ETL; it is a
property of applying the rules. ELT applies them one layer later and keeps the
original row, which buys you things ETL structurally cannot give back.

## Why, on the five things you asked about

### Auditability — decisive, and the reason for the recommendation

This is not a close call. A worked example from the running system:
`mart_daily_store_kpis` reports Rs 6,683.00 for NammaMart Marathahalli on
18 September. Two SQL queries take that number to three order IDs and then to
**`orders.csv` lines 259, 928 and 32**, with the timestamp of the run that
loaded them. The ETL reaches the file via `_source_file` but cannot produce the
line number or the original text, because the row was cast in pandas and the
pre-transformation value was never stored.

The sharper version is the one Finance will hit in week one. An order delivered
on 3 September was returned on 1 October. After the incremental load, September
revenue falls from **Rs 1,135,628.80 to Rs 1,131,435.10** — a closed month moves
by Rs 4,193.70. Finance will ask what was originally published. In ELT the
answer is a query, because `raw_orders` still holds both versions of every
restated row. In ETL that number is **gone**: the upsert overwrote it, and the
only copy is a CSV on disk that tomorrow's export may already have replaced.

When the CFO asks "why is September different from what you told me?", ELT
answers in SQL and ETL answers "I'll have to check the files."

### Data quality guarantees — ETL wins on paper; it matters less than it looks

In ETL, a bad row cannot reach `etl_fact_orders` because it was never passed
along. That is a structural guarantee and it is genuinely stronger than ELT's
`WHERE dq_status <> 'QUARANTINED'`, which depends on every future query
remembering the filter. One forgotten `WHERE` and a rejected row is in a report.

Two things reduce that advantage to something manageable. First, it is testable:
`tests/` asserts that no row failing a Critical check appears in the fact table,
and the same assertion runs against the ELT mart. Second, and more important,
ELT's quarantine is *queryable*. When Finance asks why an order is missing, the
ETL answer lives in a CSV file in `include/quarantine/`; the ELT answer is a
`SELECT` against `stg_orders` with the reason in a column. Over a year of
month-end questions, the second is worth more than the first.

Mitigation, and I would make it a condition of the design: expose the mart to
reporting tools through **views** that embed the `dq_status` filter, so no
analyst writes the predicate themselves. That recovers most of ETL's structural
guarantee without giving up the raw layer.

### Ease of change — ELT, with one caveat

A new quality rule in ELT is an edit to a `.sql` file. In ETL it is a change to
pandas code, which means a Python developer and a redeploy. Your team has no
data engineers today, and SQL is the skill you can actually hire for in
Bengaluru. Adding "flag any order over Rs 10,000" is a two-line change one of
your analysts could make and review.

The caveat is real and experiment 6 measured it: ELT's raw layer has **no schema
contract**. When I renamed `order_amount` to `total`, all five raw loads
succeeded — the raw layer reads every column as text and takes its names from
the file header — and the failure surfaced two tasks later in `stage_orders`.
The ETL failed earlier, at `validate`. Neither failed *silently*, which is the
important thing, but ELT let the drift into the audit layer. Fix this before go
live with an expected-columns assertion at load time.

### Performance as data grows 1000× — this changes the architecture, not the choice

At 2,400 orders a month both pipelines finish in **8–14 seconds**. At 2.4 million
orders a month, neither works as built, and I have direct evidence of the limit
rather than a projection.

DuckDB is a single-writer embedded database. When I ran the ELT under the real
scheduler, its five parallel raw loads fought over the warehouse file and two of
them died after three attempts with `IOException: Could not set lock on file` —
the pipeline loaded nothing. The fix in place today is an Airflow pool with one
slot, which serialises every warehouse write. **That is a correctness patch for a
constraint that will not survive growth**: at 1000× volume you cannot serialise
the writers and still be finished by 09:00.

So the warehouse must move — to Postgres now, or BigQuery/Snowflake if you want
to stop managing it. And this is where ELT gets decisively cheaper: **its
transformations are already SQL**, so they run on the new engine essentially
unchanged, and they get faster because a real warehouse parallelises them. The
ETL's transformations are pandas running on one Airflow worker, holding the
batch in memory; at 2.4M rows that worker needs to become a Spark job, which is a
rewrite, not a migration. Choosing ELT today means the scaling story is "change
the connection string"; choosing ETL means "rewrite the transforms in 18 months".

### Cost

Compute favours ELT at scale for the reason above. Storage favours ETL, and
visibly: `raw_orders` grew 2,412 → 4,824 → 7,396 rows across three runs, because
the raw layer is append-only by design. That is the price of auditability, it is
trivial at today's volume, and at 1000× it needs the retention policy that
**does not exist yet** — there is no pruning job in either DAG today, and raw
currently grows without bound.

The dominant cost is neither. It is **people**. ELT's transformations are SQL
that an analyst can read, modify and review; ETL's are Python that needs an
engineer. For a company with no data engineers, that is the number that matters,
and it points the same way as everything else.

## The decision you have not been told you are making

One thing deserves your attention more than the ETL/ELT question, because it
will move the number you report and nobody will notice.

The brief's quality rules suggest treating master-data defects as Critical —
reject a product with no category, a store with a missing zone. Applied
literally, that rejects every *order* pointing at the bad master row. I built
both policies and ran both pipelines under each:

| | lenient | strict | delta |
|---|---|---|---|
| ETL fact rows | 2,310 | 1,897 | **−413** |
| delivered orders | 2,088 | 1,718 | −370 |
| **net revenue** | **Rs 1,135,628.80** | **Rs 931,157.95** | **−Rs 204,470.85** |
| gross margin | Rs 260,999.43 | Rs 227,955.56 | −Rs 33,043.87 |
| zones in the report | 5 | **4** | −1 |
| categories in the report | 7 | **6** | −1 |
| orders quarantined (ETL quarantine file) | 102 | 515 | +413 |
| orders marked QUARANTINED (ELT staging) | 90 | 503 | +413 |

**24 defective master rows cost Rs 204,470.85 — 18% of September revenue.** One
store with an empty `zone` cell removes that store's entire month from the COO's
zone report. A whole zone and a whole product category vanish from the reports
under strict: not shown as unknown, simply absent.

I have shipped **lenient** as the default: master rows are rejected only when
the key itself is unusable, and content defects are repaired to a visible
sentinel (`zone='UNKNOWN'`, `category='UNCATEGORISED'`) and flagged in
`dq_results`. Kavya sees Rs 82,725.60 sitting in an `UNKNOWN` zone and asks why
— which is the correct outcome, because the money is real and the defect is a
blank cell in a spreadsheet someone can fix in a minute.

But this is **your call, not mine**, and it is one line: the Airflow Variable
`nammamart_master_dq_policy`. An engineer quietly choosing `strict` would have
deleted a fifth of your revenue on their own authority. That is the kind of
decision that should have a name against it.

## Top 3 risks of the recommended design

**1. The single-writer warehouse — measured, and already constraining us.**
DuckDB allows one writer. The ELT's parallel loads failed with lock errors under
the real scheduler, and — separately — the two DAGs are scheduled at the same
minute and collide with each other. Today a single global one-slot Airflow pool
serialises every warehouse write across both DAGs to prevent it. It works at this volume and it will not scale: the pool is
the thing that stops the pipeline being parallel. *Mitigation:* move the
warehouse to Postgres before volume grows, and treat the pool's removal as the
acceptance test for that migration. *Cost of ignoring it:* the 09:00 SLA fails
silently as volume rises, because the serialised writes simply take longer each
week.

**2. No schema contract on the raw layer — ELT's specific weakness.**
A renamed column loads into `raw_orders` without complaint and surfaces as a SQL
error two tasks later. Worse, the audit copy of today's export then has a
different shape from yesterday's, so the thing we rely on for lineage has itself
drifted. *Mitigation:* an expected-columns assertion at raw load, failing with a
message that names the added and removed columns, plus a `schema_history` table
so drift is queryable rather than discovered. This is the weakest of the five
observability pillars and the first thing I would build next.

**3. Every detection mechanism is a pull, not a push.**
This is the risk I would rank highest on business impact. The pipeline is
genuinely good at explaining the past — I can trace any number to a file and
line, and prove why any row was rejected. It is poor at telling anyone that
something is wrong *right now*. There is no email and no Slack alert. Experiment
7 is the demonstration: a run with **50 orders instead of 2,412**, reporting
Rs 24,014 instead of Rs 1.14M, completed **fully green** on both pipelines. A
checked-in volume check now flags it — in a table someone has to open.

For a 09:00 SLA feeding a 09:30 meeting, a dashboard nobody opens at 08:55 is
not observability. *Mitigation, in order:* an `on_failure_callback` to Slack via
`Variable.get("SLACK_WEBHOOK_URL")`; an SLA miss alert so a run not *finished*
by 09:15 IST escalates even when no task has failed; and a daily digest of
`dq_results` so Warnings are seen rather than accumulated. The plumbing exists
in `include/slack_notifier.py` and is not yet wired to my DAGs.

## What I would do in the first two weeks

1. Wire failure and SLA-miss alerts. Nothing else on this list matters if a
   broken 09:00 run goes unnoticed until 09:30.
2. Get Sameer's and Finance's answer on the master-data severity policy, and the
   three open questions from Part 0 — the negative quantities, the authoritative
   `order_amount`, and whether a closed month may be restated.
3. Add the expected-columns assertion at raw load.
4. Expose the mart through `dq_status`-filtered views so no analyst can
   accidentally query past the quality gate.
5. Write the retention job. Raw currently grows forever and holds unmasked
   phone numbers indefinitely, which is the clearest DPDP exposure in the
   current design.

The 9 AM number is right today, and it is explainable, which is what you asked
for. What it is not yet is *defended* — and that is a fortnight of work, not a
rebuild.
