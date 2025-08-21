# Home Exercise — Web Scraping → Data Warehouse → BI

This repo contains a **three‑phase pipeline** that scrapes monthly ZIPs per ID, normalizes CSVs, loads them into **PostgreSQL** with **full lineage**, and outlines how to surface insights in **Metabase OSS**.

> ✅ This submission focuses on correctness, resilience, and **generic handling of 25+ table types** without hard‑coding per‑table schemas.

---

## Contents

- `phase1_scraping.py` — Phase 1: robust, generic scraper + normalizer
- `phase2_load.py` — Phase 2: generic Postgres loader with schema evolution + lineage
- `data/` — created at runtime
  - `raw/` — original ZIPs (provenance)
  - `normalized/` — UTF‑8, comma CSVs partitioned by table/id/yyyy/mm
  - `manifest.sqlite3` — run manifest + table‑type registry
- `Home Exercise.docx` — assignment brief (as provided)

---

## Environment

- OS: Ubuntu 24.04
- Python: 3.12.x
- Database: PostgreSQL (local or remote)

### Python dependencies
Install with either option:

```bash
# If you prefer a requirements file:
pip install -r requirements.txt

# Or install directly:
pip install requests psycopg2-binary python-dateutil
```
> `requests` is only required when actually scraping a live site (`DRY_RUN=false`).

---

## Phase 1 — Scraping & Normalization

**Goal:** For each `(id, year, month)` from **Jan 2010 → current month**, download one ZIP containing **1–10 CSVs**, validate safely, and normalize all CSVs to UTF‑8 `,` delimiter.

### Highlights (how it meets the spec)
- **Scale & Idempotency:** A lightweight SQLite **manifest** tracks each task `(id, yyyy, mm)` with status, attempts, error, `zip_sha256`. Completed tasks are skipped.
- **Network & ZIP safety:** Timeouts + retries, magic‑byte & CRC checks. Downloads are atomic (`.part → rename`).
- **Table‑type discovery (25 types):** Each CSV filename’s token is sanitized and **mapped into canonical buckets** `table_01`…`table_25`. If more unique tokens are seen, allocation **continues as `table_26+`** and a warning is emitted (and marked as `overflow` in SQLite). **No hard‑coding** of names.
- **Normalization:** Detects delimiter, handles UTF‑8 / Windows‑1255 fallbacks, writes clean CSVs under `data/normalized/{table_xx}/{id}/{yyyy}/{mm}/…`.
- **Provenance:** Raw ZIPs are kept under `data/raw/…` for audit.

### Quick start (dry run: fabricates ZIPs with safe data)
```bash
# Uses built-in demo IDs and fabricated ZIPs
export DRY_RUN=true
python phase1_scraping.py
```

### Real run against a live site
```bash
export DRY_RUN=false
export URL_TEMPLATE="https://host/path?download&id={id}&y={yyyy}&m={mm}"
# Provide IDs via a file (one per line) if desired:
export IDS_FILE=/path/to/ids.txt

python phase1_scraping.py
```

**Outputs**
```
data/
  raw/{id}/{yyyy}/{mm}/source.zip
  normalized/{table_xx}/{id}/{yyyy}/{mm}/*.csv
  manifest.sqlite3  (tables: manifest, types)
```

---

## Phase 2 — Loading to Postgres (with Lineage)

**Goal:** Load **all normalized CSVs** generically into Postgres, handling **schema drift** and maintaining a **complete audit trail** from source file to canonical tables.

### What the loader does
- Registers each file in `lineage.files` using `sha256` (idempotent).
- Stages rows as JSON in `staging.rows` with rich metadata (table, id, yyyy, mm, row_num, file_id, batch_id).
- Ensures canonical **`public.table_xx`** exists and **evolves** (AUTO `ALTER TABLE` to add new columns).
- Upserts data from staging into canonical, preserving **`src_file_id`** and **`src_row_num`** for traceability.
- Records each run in `lineage.batches` with `started_at/ended_at`.

### Run it
```bash
# Point to your database; example for local Postgres with DB "exercise"
export PG_DSN="postgresql://username:password@localhost:5432/exercise"

python phase2_load.py
```

The script will create required schemas/tables if missing and print batch stats:
```
[OK] Batch <uuid> files=<N> rows=<M>
```

### Lineage / Audit trail
With the combo of:
- Raw ZIPs in `data/raw/`
- SQLite `manifest` (Phase 1)
- Postgres `lineage.*` tables (Phase 2)
you can answer **“which source row produced this BI row?”** and **“which BI rows came from this file?”** using `src_file_id`, `src_row_num`, and `batch_id`.

---

## Phase 3 — BI (Metabase OSS)

- Set up Metabase and connect it to the Postgres DB.
- Use **Permissions** (Collections + Data Model) to separate **admin / creator / viewer** access:
  - *Admin*: all schema management + data access.
  - *Creator*: can build questions/dashboards in approved collections, read only canonical schemas.
  - *Viewer*: read specific collections/dashboards; no data model edits.
- Limitations to be aware of in OSS:
  - Granular column‑level security and row‑level policies are basic; for strict controls, enforce **SQL views** and **DB‑level roles/privileges** (recommended).
- Add **performance dashboards** (server & DB):
  - Server: CPU%, RAM, disk, network (export from `node_exporter` / `psutil` tables or an Ops tool).
  - Postgres: QPS, avg latency, active connections, buffer/cache hit ratio, slow queries, table bloat, index hit ratio, `autovacuum` activity.
  - Surface these in Metabase via **views** that read from monitoring tables or extensions (e.g., `pg_stat_statements`).

---

## Configuration Reference

| Variable          | Meaning                                                | Default |
|-------------------|--------------------------------------------------------|---------|
| `DRY_RUN`         | Fabricate tiny ZIPs for Phase 1                        | `true`  |
| `URL_TEMPLATE`    | Download URL (use `{id}`, `{yyyy}`, `{mm}`)            | n/a     |
| `IDS_FILE`        | Path to newline‑delimited list of IDs                  | n/a     |
| `MAX_TASKS`       | Stop after N tasks (for testing)                       | `0`     |
| `PG_DSN`          | Postgres DSN for Phase 2                               | n/a     |

> Phase 1 also keeps a **types** registry in SQLite to map arbitrary filename tokens to `table_01..table_25` (and `table_26+` with warning).

---

## How this meets the assignment prompts

- **Potential issues & mitigations:** Addressed via manifest, retries/backoff, ZIP CRC checks, delimiter/encoding normalization, generic type bucketing (25 types), schema evolution, and full provenance.
- **Generic handling of 25 table types:** No table is hard‑coded. Filename tokens are sanitized and **bucketed**; overflow beyond 25 is allowed but **warned** and flagged.
- **Audit trail:** Raw ZIPs + manifest + Postgres lineage (files, batches, src row links).
- **Large tables:** Loader creates partition‑friendly canonical tables; in production, add partitioning, indexes, VACUUM/ANALYZE automation, and BI via views.

---

## Typical workflow

```bash
# 1) Phase 1: Scrape & normalize
export DRY_RUN=true            # or false + URL_TEMPLATE
python phase1_scraping.py

# 2) Phase 2: Load to Postgres
export PG_DSN="postgresql://user:pass@localhost:5432/exercise"
python phase2_load.py

# 3) Phase 3: Point Metabase to the same Postgres and build dashboards
```

---

## Notes on AI assistance

Per the preface, AI tools were used to:
- Draft robust error‑handling patterns and generic schema‑evolution SQL.
- Tighten explanations and structure of this README.

All design choices are explained in‑line; happy to walk through any piece live.
