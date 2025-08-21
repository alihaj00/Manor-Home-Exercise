#!/usr/bin/env python3
"""
Phase 2 loader: generic, schema-drift tolerant, full lineage.

- Reads Phase 1 outputs in: data/normalized/{table_xx}/{id}/{yyyy}/{mm}/*.csv
- Registers each CSV in lineage.files (sha256-based idempotency).
- Loads rows into staging.rows as JSONB with rich metadata.
- Ensures canonical public.{table_xx} exists and evolves (ALTER TABLE for new cols).
- Merges staged rows into canonical, linking (src_file_id, src_row_num, batch_id).

Env:
  PG_DSN: Postgres connection string (required)
"""

from __future__ import annotations
import csv, hashlib, json, os, sys, uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, Iterable, List, Tuple
from dateutil.relativedelta import relativedelta

import psycopg2
import psycopg2.extras as extras

BASE = Path("data/normalized")  # From Phase 1
MAX_TYPES = 25  # soft cap; we still allow table_26+ with a warning

# -------------------- small utils --------------------
def file_sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def infer_type(values: List[str]) -> str:
    """
    Minimal type inference for canonical columns: INT -> NUMERIC -> TIMESTAMP -> TEXT.
    Keeps it safe by preferring TEXT on ambiguity.
    """
    import re, datetime as dt
    if not values:
        return "text"
    ints, nums, dates = True, True, True
    for v in values:
        if v == "" or v is None:
            continue
        if ints:
            if not re.fullmatch(r"[-+]?\d+", v or ""):
                ints = False
        if nums:
            if not re.fullmatch(r"[-+]?\d+(\.\d+)?", v or ""):
                nums = False
        if dates:
            ok = False
            for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%d/%m/%Y"):
                try:
                    dt.datetime.strptime(v, fmt); ok = True; break
                except Exception:
                    pass
            if not ok:
                dates = False
        if not (ints or nums or dates):
            return "text"
    if ints:  return "bigint"
    if nums:  return "numeric"
    if dates: return "timestamp"
    return "text"

# -------------------- DB bootstrap --------------------
DDL = """
CREATE SCHEMA IF NOT EXISTS lineage;
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS lineage.batches (
  batch_id     uuid PRIMARY KEY,
  started_at   timestamptz NOT NULL DEFAULT now(),
  ended_at     timestamptz
);

CREATE TABLE IF NOT EXISTS lineage.files (
  file_id      bigserial PRIMARY KEY,
  rel_path     text NOT NULL,
  table_name   text NOT NULL,         -- e.g., table_01
  entity_id    text NOT NULL,         -- path {id}
  yyyy         int  NOT NULL,
  mm           int  NOT NULL,
  filename     text NOT NULL,
  file_sha256  text NOT NULL UNIQUE,
  zip_sha256   text,
  first_seen   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS staging.rows (
  row_id       bigserial PRIMARY KEY,
  file_id      bigint NOT NULL REFERENCES lineage.files(file_id) ON DELETE CASCADE,
  row_num      int    NOT NULL,       -- 1-based index within the CSV (excluding header)
  table_name   text   NOT NULL,
  entity_id    text   NOT NULL,
  yyyy         int    NOT NULL,
  mm           int    NOT NULL,
  rec          jsonb  NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_staging_file ON staging.rows(file_id);
CREATE INDEX IF NOT EXISTS idx_staging_json ON staging.rows USING GIN(rec);

CREATE TABLE IF NOT EXISTS lineage.schema_registry (
  table_name   text NOT NULL,
  column_name  text NOT NULL,
  data_type    text NOT NULL,
  version      int  NOT NULL,
  added_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (table_name, column_name)
);
"""

def pg():
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("PG_DSN env var is required", file=sys.stderr)
        sys.exit(2)
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn

def ensure_base(cur):
    cur.execute(DDL)

# -------------------- file registration --------------------
def parse_path(p: Path) -> Tuple[str, str, int, int, str]:
    """
    Expect: data/normalized/{table}/{id}/{yyyy}/{mm}/{filename}.csv
    Returns: (table_name, entity_id, yyyy, mm, filename)
    """
    parts = p.parts
    try:
        i = parts.index("normalized")
    except ValueError:
        raise ValueError(f"Bad base path for {p}")
    table_name = parts[i+1]
    entity_id  = parts[i+2]
    yyyy       = int(parts[i+3])
    mm         = int(parts[i+4])
    filename   = parts[i+5]
    return table_name, entity_id, yyyy, mm, filename

def register_file(cur, rel_path: str, table_name: str, entity_id: str, yyyy: int, mm: int, filename: str, sha256: str) -> int:
    cur.execute("""
      INSERT INTO lineage.files(rel_path, table_name, entity_id, yyyy, mm, filename, file_sha256)
      VALUES (%s,%s,%s,%s,%s,%s,%s)
      ON CONFLICT (file_sha256) DO NOTHING
      RETURNING file_id
    """, (rel_path, table_name, entity_id, yyyy, mm, filename, sha256))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT file_id FROM lineage.files WHERE file_sha256=%s", (sha256,))
    return cur.fetchone()[0]

# -------------------- staging load --------------------
def load_csv_to_staging(cur, file_id: int, table_name: str, entity_id: str, yyyy: int, mm: int, csv_path: Path) -> int:
    count = 0
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for i, rec in enumerate(reader, start=1):
            rows.append( (file_id, i, table_name, entity_id, yyyy, mm, json.dumps(rec)) )
            if len(rows) >= 10_000:
                extras.execute_values(
                    cur,
                    "INSERT INTO staging.rows(file_id,row_num,table_name,entity_id,yyyy,mm,rec) VALUES %s",
                    rows
                )
                count += len(rows); rows.clear()
        if rows:
            extras.execute_values(
                cur,
                "INSERT INTO staging.rows(file_id,row_num,table_name,entity_id,yyyy,mm,rec) VALUES %s",
                rows
            )
            count += len(rows)
    return count

# -------------------- canonical ensure/evolve --------------------
def ensure_canonical_table(cur, table_name: str):
    cur.execute(f"""
      CREATE TABLE IF NOT EXISTS public.{table_name} (
        src_file_id  bigint NOT NULL REFERENCES lineage.files(file_id) ON DELETE RESTRICT,
        src_row_num  int    NOT NULL,
        batch_id     uuid   NOT NULL,
        entity_id    text   NOT NULL,
        yyyy         int    NOT NULL,
        mm           int    NOT NULL
      );
    """)
    cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_meta ON public.{table_name}(entity_id, yyyy, mm);")

def discover_keys_for_file(cur, file_id: int) -> List[str]:
    cur.execute("""
      SELECT DISTINCT key
      FROM staging.rows,
           LATERAL jsonb_object_keys(rec) AS key
      WHERE file_id = %s
    """, (file_id,))
    return [r[0] for r in cur.fetchall()]

def ensure_columns(cur, table_name: str, keys: List[str], sample_types: Dict[str, str]) -> None:
    cur.execute("""
      SELECT column_name FROM information_schema.columns
      WHERE table_schema='public' AND table_name=%s
    """, (table_name,))
    existing = {r[0] for r in cur.fetchall()}
    version = 1
    for key in keys:
        if key in ("src_file_id","src_row_num","batch_id","entity_id","yyyy","mm"):
            continue
        if key not in existing:
            dt = sample_types.get(key, "text")
            cur.execute(psycopg2.sql.SQL("ALTER TABLE public.{t} ADD COLUMN {c} {}")
                        .format(psycopg2.sql.Identifier(table_name),
                                psycopg2.sql.Identifier(key),
                                psycopg2.sql.SQL(dt)))
            cur.execute("""
              INSERT INTO lineage.schema_registry(table_name,column_name,data_type,version)
              VALUES (%s,%s,%s,%s)
              ON CONFLICT (table_name,column_name) DO UPDATE SET data_type=EXCLUDED.data_type
            """, (table_name, key, dt, version))

def sample_types_for_keys(cur, file_id: int, keys: List[str]) -> Dict[str, str]:
    types = {}
    for k in keys:
        cur.execute("""
          SELECT rec ->> %s
          FROM staging.rows
          WHERE file_id=%s AND (rec ? %s)
          LIMIT 500
        """, (k, file_id, k))
        vals = [r[0] for r in cur.fetchall() if r[0] is not None]
        types[k] = infer_type(vals)
    return types

def merge_file_into_canonical(cur, table_name: str, file_id: int, batch_id: uuid.UUID):
    keys = discover_keys_for_file(cur, file_id)
    types = sample_types_for_keys(cur, file_id, keys)
    ensure_columns(cur, table_name, keys, types)

    cols = [k for k in keys if k not in ("src_file_id","src_row_num","batch_id","entity_id","yyyy","mm")]
    cols_sql = ", ".join([psycopg2.extensions.quote_ident(c, cur) for c in cols])
    vals_sql = ", ".join([f"(r.rec ->> {extras.Json(c)})::{types.get(c,'text')}" for c in cols]) if cols else ""

    if cols:
        cur.execute(psycopg2.sql.SQL(f"""
          INSERT INTO public.{table_name} (src_file_id, src_row_num, batch_id, entity_id, yyyy, mm, {psycopg2.extensions.AsIs(cols_sql)})
          SELECT r.file_id, r.row_num, %s, r.entity_id, r.yyyy, r.mm, {psycopg2.extensions.AsIs(vals_sql)}
          FROM staging.rows r
          WHERE r.file_id = %s
        """), (str(batch_id), file_id))
    else:
        cur.execute(psycopg2.sql.SQL(f"""
          INSERT INTO public.{table_name} (src_file_id, src_row_num, batch_id, entity_id, yyyy, mm)
          SELECT r.file_id, r.row_num, %s, r.entity_id, r.yyyy, r.mm
          FROM staging.rows r
          WHERE r.file_id = %s
        """), (str(batch_id), file_id))

# -------------------- driver --------------------
def run():
    conn = pg()
    cur  = conn.cursor()
    ensure_base(cur)

    batch_id = uuid.uuid4()
    cur.execute("INSERT INTO lineage.batches(batch_id) VALUES (%s)", (str(batch_id),))

    files = list(BASE.rglob("*.csv"))
    files.sort()
    loaded_files = 0
    staged_rows  = 0

    for csv_path in files:
        rel = csv_path.as_posix()
        try:
            table_name, entity_id, yyyy, mm, filename = parse_path(csv_path)
        except Exception as e:
            print(f"[WARN] skipping {rel}: {e}", file=sys.stderr)
            continue

        try:
            idx = int(table_name.split("_")[-1])
            if idx > MAX_TYPES:
                print(f"[WARN] {table_name} exceeds soft cap {MAX_TYPES}", file=sys.stderr)
        except Exception:
            pass

        sha = file_sha256(csv_path)
        file_id = register_file(cur, rel, table_name, entity_id, yyyy, mm, filename, sha)

        cur.execute("SELECT 1 FROM staging.rows WHERE file_id=%s LIMIT 1", (file_id,))
        if cur.fetchone():
            ensure_canonical_table(cur, table_name)
            merge_file_into_canonical(cur, table_name, file_id, batch_id)
            conn.commit()
            loaded_files += 1
            continue

        staged = load_csv_to_staging(cur, file_id, table_name, entity_id, yyyy, mm, csv_path)
        staged_rows += staged

        ensure_canonical_table(cur, table_name)
        merge_file_into_canonical(cur, table_name, file_id, batch_id)

        conn.commit()
        loaded_files += 1
        if loaded_files % 25 == 0:
            print(f"[INFO] files={loaded_files} staged_rows={staged_rows}")

    cur.execute("UPDATE lineage.batches SET ended_at=now() WHERE batch_id=%s", (str(batch_id),))
    conn.commit()
    cur.close(); conn.close()
    print(f"[OK] Batch {batch_id} files={loaded_files} rows={staged_rows}")

if __name__ == "__main__":
    run()
