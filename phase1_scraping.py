#!/usr/bin/env python3
"""
Phase 1 (generic):
- Generate (id, yyyy, mm) tasks (2010 → current month).
- Download (or fabricate in DRY_RUN) a ZIP per task.
- Validate ZIP (magic + CRC), compute sha256, save under data/raw/…
- For each CSV inside, infer a table token from the filename and map it to a
  canonical bucket: table_01..table_25. If more than 25 unique tokens are
  discovered, continue as table_26, table_27, … and emit warnings + mark overflow.
- Normalize each CSV to UTF-8 + comma delimiter into data/normalized/{table_xx}/…
- Track progress in data/manifest.sqlite3 (status, attempts, error, sha).
"""
from __future__ import annotations
import csv
import hashlib
import io
import os
import random
import re
import sqlite3
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Tuple, Optional

try:
    import requests  # Only used when DRY_RUN=False
except Exception:
    requests = None  # keep import optional for DRY_RUN

# -------------------- config --------------------
DATA = Path("data")
RAW  = DATA / "raw"
NORM = DATA / "normalized"
DB   = DATA / "manifest.sqlite3"

# For production runs: export URL_TEMPLATE like:
#   export URL_TEMPLATE="https://host/download?id={id}&y={yyyy}&m={mm}"
URL_TEMPLATE = os.getenv("URL_TEMPLATE", "https://example.invalid/download?id={id}&y={yyyy}&m={mm}")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() in ("1", "true", "yes")

# Demo IDs for DRY_RUN; replace/pipe from a file if you want:
IDS = [f"{i:05d}" for i in range(1, 51)]
# You can also point IDS_FILE to a newline-delimited file of IDs
IDS_FILE = os.getenv("IDS_FILE")
if IDS_FILE and Path(IDS_FILE).exists():
    IDS = [line.strip() for line in Path(IDS_FILE).read_text().splitlines() if line.strip()]

START_YM = (2010, 1)
END_YM   = (date.today().year, date.today().month)

TIMEOUT = (10, 60)  # (connect, read) seconds
MAX_RETRIES = 3
MAX_TYPES = 25  # target bucket cap (we will keep assigning beyond this and warn)
MAX_TASKS = int(os.getenv("MAX_TASKS", "0"))  # 0 = no limit (useful in DRY_RUN)

# CSV filename is assumed to include id, year, month, and a table token somewhere.
# The pattern is intentionally flexible, e.g. "12345_2018-03_tableXYZ.csv"
FNAME_RE = re.compile(
    r"(?P<id>\d+).*?(?P<yyyy>20\d{2}).*?(?P<mm>\d{1,2}).*?(?P<table>[A-Za-z0-9_\-]+)",
    re.I,
)

# -------------------- small utils --------------------
@dataclass
class Task:
    """Unit of work for a single (id, year, month)."""
    id: str
    yyyy: int
    mm: int

def months_range(start: Tuple[int, int], end: Tuple[int, int]) -> Iterable[Tuple[int, int]]:
    """
    Yield (yyyy, mm) pairs for each month inclusively from `start` to `end`.

    Args:
        start: (year, month) start (inclusive).
        end:   (year, month) end (inclusive).
    """
    sy, sm = start
    ey, em = end
    a = sy * 12 + (sm - 1)
    b = ey * 12 + (em - 1)
    for n in range(a, b + 1):
        yy, mm = divmod(n, 12)
        yield yy, mm + 1

def sha256_bytes(b: bytes) -> str:
    """
    Return hex sha256 for a bytes buffer (used to fingerprint raw ZIPs)."""
    return hashlib.sha256(b).hexdigest()

def sanitize_token(token: str) -> str:
    """
    Normalize a table token from filename into a safe identifier:
    - keep [A-Za-z0-9_], lower-case, collapse underscores
    - prefix with 't_' if it starts with a digit
    """
    s = re.sub(r"[^A-Za-z0-9_]", "_", token).lower()
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        s = "table"
    if s[0].isdigit():
        s = f"t_{s}"
    return s

# -------------------- sqlite (manifest + types) --------------------
def init_db() -> sqlite3.Connection:
    """
    Initialize the local SQLite manifest and directory structure.

    Creates:
      - manifest(id, yyyy, mm, status, attempts, error, zip_path, zip_sha256, timestamps)
      - types(token -> canonical table_xx, first_seen, overflow flag)
    """
    RAW.mkdir(parents=True, exist_ok=True)
    NORM.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB)
    con.execute("PRAGMA journal_mode=WAL;")
    con.executescript("""
      CREATE TABLE IF NOT EXISTS manifest(
        id TEXT, yyyy INTEGER, mm INTEGER,
        status TEXT, attempts INTEGER, error TEXT,
        zip_path TEXT, zip_sha256 TEXT,
        created_at TEXT, updated_at TEXT,
        PRIMARY KEY(id, yyyy, mm)
      );

      -- table name discovery & mapping
      CREATE TABLE IF NOT EXISTS types(
        token      TEXT PRIMARY KEY,      -- sanitized token found in filenames
        canonical  TEXT UNIQUE NOT NULL,  -- table_01, table_02, ...
        first_seen TEXT NOT NULL,
        overflow   INTEGER NOT NULL DEFAULT 0  -- 1 if index > MAX_TYPES
      );
    """)
    con.commit()
    return con

def set_status(con: sqlite3.Connection, t: Task, status: str, **kw) -> None:
    """
    Upsert a manifest row for task `t`, bumping attempts and storing details.

    Kwargs accepted:
      error: str | None
      zip_path: str | None
      zip_sha256: str | None
    """
    now = datetime.utcnow().isoformat()
    row = con.execute("SELECT attempts FROM manifest WHERE id=? AND yyyy=? AND mm=?",
                      (t.id, t.yyyy, t.mm)).fetchone()
    attempts = (row[0] if row else 0) + 1
    con.execute("""
      INSERT INTO manifest(id,yyyy,mm,status,attempts,error,zip_path,zip_sha256,created_at,updated_at)
      VALUES(?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(id,yyyy,mm) DO UPDATE SET
        status=excluded.status,
        attempts=?,
        error=excluded.error,
        zip_path=excluded.zip_path,
        zip_sha256=excluded.zip_sha256,
        updated_at=excluded.updated_at
    """, (t.id,t.yyyy,t.mm,status,attempts,kw.get("error"),kw.get("zip_path"),kw.get("zip_sha256"),
          now, now, attempts))
    con.commit()

def get_or_alloc_type(con: sqlite3.Connection, token: str) -> str:
    """
    Map a sanitized filename token to a canonical bucket 'table_XX'.

    - Reuses existing mapping if token seen before.
    - Otherwise allocates next sequential table index.
    - Marks overflow=1 once index exceeds MAX_TYPES and prints a warning.

    Returns:
        canonical table name like 'table_01'
    """
    token = sanitize_token(token)

    row = con.execute("SELECT canonical FROM types WHERE token=?", (token,)).fetchone()
    if row:
        return row[0]

    # find highest assigned index so far
    row = con.execute("SELECT canonical FROM types ORDER BY canonical DESC LIMIT 1").fetchone()
    if row:
        m = re.search(r"(\d+)$", row[0])
        next_idx = (int(m.group(1)) if m else 0) + 1
    else:
        next_idx = 1

    canonical = f"table_{next_idx:02d}"
    overflow = 1 if next_idx > MAX_TYPES else 0

    con.execute(
        "INSERT INTO types(token, canonical, first_seen, overflow) VALUES (?,?,?,?)",
        (token, canonical, datetime.utcnow().isoformat(), overflow)
    )
    con.commit()

    if overflow:
        print(f"[WARN] Discovered more than {MAX_TYPES} unique table types; "
              f"new token '{token}' assigned '{canonical}'.", file=sys.stderr)

    return canonical

def print_overflow_summary(con: sqlite3.Connection) -> None:
    """
    Print a human-readable summary of any canonical types allocated beyond MAX_TYPES.
    """
    rows = list(con.execute("SELECT canonical, token FROM types WHERE overflow=1 ORDER BY canonical"))
    if rows:
        print(f"[WARN] {len(rows)} type(s) beyond the first {MAX_TYPES} detected:", file=sys.stderr)
        for c, tkn in rows:
            print(f"  {c:<9}  (from token: {tkn})", file=sys.stderr)

# -------------------- IO / normalization --------------------
def safe_open_zip(buf: bytes) -> zipfile.ZipFile:
    """
    Defensive ZIP open:
      - Verify PK magic bytes.
      - Run CRC test across members (ZipFile.testzip()).
    Raises:
      ValueError on bad magic or CRC errors.
    """
    if not buf.startswith(b"PK\x03\x04"):
        raise ValueError("Not a ZIP (bad magic bytes)")
    zf = zipfile.ZipFile(io.BytesIO(buf))
    bad = zf.testzip()  # CRC check
    if bad:
        raise ValueError(f"ZIP CRC failed for member: {bad}")
    return zf

def normalize_and_write_csv(canonical_type: str, id_: str, yyyy: int, mm: int,
                            arcname: str, raw_bytes: bytes) -> None:
    """
    Normalize a CSV member to UTF-8 + comma delimiter and write to disk.

    Steps:
      - Decode bytes (utf-8 → cp1255 → utf-8 with 'replace').
      - Sniff delimiter among [ , ; \\t | ] with csv.Sniffer (fallback ',').
      - Preserve header, write rows to data/normalized/{table}/{id}/{yyyy}/{mm}/<name>.csv
    """
    # Decode to UTF-8 (fallback to cp1255, then 'replace')
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = raw_bytes.decode("cp1255")
        except UnicodeDecodeError:
            text = raw_bytes.decode("utf-8", errors="replace")

    # Sniff delimiter; fallback to comma
    try:
        sample = "\n".join(text.splitlines()[:10]) or ","
        delim = csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except Exception:
        delim = ","

    rows = list(csv.reader(io.StringIO(text), delimiter=delim))
    if not rows:
        return
    header, rest = rows[0], rows[1:]

    out_dir = NORM / canonical_type / id_ / f"{yyyy:04d}" / f"{mm:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / (Path(arcname).stem + ".csv")
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=",", quoting=csv.QUOTE_MINIMAL)
        w.writerow(header)
        w.writerows(rest)

def fabricate_zip_bytes(t: Task) -> bytes:
    """
    DRY_RUN helper: create a small valid ZIP containing 1..3 CSVs per task.

    - File names look like: "<id>_<yyyy>-<mm>_<token>.csv"
    - Tokens cycle over token_01..token_35 to demonstrate overflow (>25).
    """
    rng = random.Random(f"{t.id}-{t.yyyy}-{t.mm}")
    tokens = [f"token_{i:02d}" for i in range(1, 36)]  # up to 35 unique tokens
    count = rng.randint(1, 3)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for _ in range(count):
            tok = rng.choice(tokens)
            name = f"{t.id}_{t.yyyy}-{t.mm:02d}_{tok}.csv"
            sio = io.StringIO()
            w = csv.writer(sio)
            w.writerow(["col_a", "col_b", "value"])
            for i in range(3):
                w.writerow([f"A{i}", f"B{i}", rng.randint(1, 100)])
            z.writestr(name, sio.getvalue().encode("utf-8"))
    return buf.getvalue()

def download_zip(t: Task) -> bytes:
    """
    Download the ZIP for task `t` using URL_TEMPLATE, with simple retry/backoff.
    In DRY_RUN, fabricates a small ZIP instead.

    Raises:
        RuntimeError or requests exceptions after MAX_RETRIES.
    """
    if DRY_RUN:
        return fabricate_zip_bytes(t)
    if requests is None:
        raise RuntimeError("requests not available; install it or enable DRY_RUN.")
    url = URL_TEMPLATE.format(id=t.id, yyyy=t.yyyy, mm=f"{t.mm:02d}")
    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=TIMEOUT)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            return r.content
        except Exception as e:
            last_err = e
            if attempt < MAX_RETRIES:
                time.sleep((2 ** attempt) + random.random())
    assert last_err is not None
    raise last_err

# -------------------- task processing --------------------
def already_ok(con: sqlite3.Connection, t: Task) -> bool:
    """
    Check idempotency: return True if task `t` is already marked ok and ZIP exists on disk.
    """
    row = con.execute(
        "SELECT status, zip_path FROM manifest WHERE id=? AND yyyy=? AND mm=?",
        (t.id, t.yyyy, t.mm)
    ).fetchone()
    return bool(row and row[0] == "ok" and row[1] and Path(row[1]).exists())

def process_task(con: sqlite3.Connection, t: Task) -> None:
    """
    Execute the end-to-end pipeline for a single task:
      1) download/fabricate ZIP
      2) validate ZIP (magic + CRC)
      3) persist raw ZIP atomically to data/raw/…
      4) for each CSV:
           - infer token from filename (fallback to last _part)
           - map to canonical table (table_XX), tracking overflow beyond 25
           - normalize and write UTF-8 comma CSV under data/normalized/…
      5) update manifest status (ok/failed) with sha256 and paths
    """
    if already_ok(con, t):
        return  # idempotent

    try:
        buf = download_zip(t)
        sha = sha256_bytes(buf)
        zf = safe_open_zip(buf)

        # Save raw ZIP atomically
        raw_dir = RAW / t.id / f"{t.yyyy:04d}" / f"{t.mm:02d}"
        raw_dir.mkdir(parents=True, exist_ok=True)
        tmp = raw_dir / "source.zip.part"
        final = raw_dir / "source.zip"
        with tmp.open("wb") as f:
            f.write(buf)
        os.replace(tmp, final)

        # Iterate archive members
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = Path(info.filename).name

            # Try to parse tokens from filename; fallback to task context
            m = FNAME_RE.search(name)
            if m:
                id_ = m.group("id")
                yyyy = int(m.group("yyyy"))
                mm = int(m.group("mm"))
                token = sanitize_token(m.group("table"))
            else:
                id_, yyyy, mm = t.id, t.yyyy, t.mm
                # Use last underscore piece of stem as token, e.g., "..._<token>.csv"
                token = sanitize_token(Path(name).stem.split("_")[-1])

            canonical = get_or_alloc_type(con, token)

            with zf.open(info) as fp:
                raw_bytes = fp.read()
            normalize_and_write_csv(canonical, id_, yyyy, mm, name, raw_bytes)

        # success
        set_status(con, t, "ok", zip_path=str(final), zip_sha256=sha, error=None)

    except Exception as e:
        set_status(con, t, "failed", error=str(e))
        print(f"[ERROR] {t.id} {t.yyyy}-{t.mm:02d}: {e}", file=sys.stderr)

# -------------------- main --------------------
def main() -> None:
    """
    Program entry point:
      - Initialize DB and folders.
      - Lazily generate tasks for each ID and month (2010 → current).
      - Process tasks with idempotency and periodic progress logs.
      - Print overflow type summary and final run stats.
    """
    con = init_db()

    # Generate tasks lazily
    tasks: Iterable[Task] = (
        Task(id=id_, yyyy=yy, mm=mm)
        for id_ in IDS
        for (yy, mm) in months_range(START_YM, END_YM)
    )

    processed = 0
    for t in tasks:
        if MAX_TASKS and processed >= MAX_TASKS:
            break
        process_task(con, t)
        processed += 1
        # light progress
        if processed % 50 == 0:
            print(f"[INFO] processed {processed} tasks…", file=sys.stderr)

    # Optional: print any overflow types summary
    print_overflow_summary(con)

    # Tiny run summary
    ok = con.execute("SELECT COUNT(*) FROM manifest WHERE status='ok'").fetchone()[0]
    failed = con.execute("SELECT COUNT(*) FROM manifest WHERE status='failed'").fetchone()[0]
    types_cnt = con.execute("SELECT COUNT(*) FROM types").fetchone()[0]
    print(f"[INFO] Done. ok={ok}, failed={failed}, discovered_types={types_cnt}", file=sys.stderr)

if __name__ == "__main__":
    main()
