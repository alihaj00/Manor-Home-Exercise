# Manor Project — Home Exercise (Design & Code Sketch)

This repository contains my **home exercise** for the Manor project recruiting process.  
It shows my approach to **Phase 1 (scraping), Phase 2 (DB loading & lineage), and Phase 3 (BI planning)**.

> **Note:** The code is **not intended to run end-to-end**. It is written to illustrate structure, decisions, and robustness (idempotency, typing, lineage). Real endpoints/credentials are intentionally omitted. Use `DRY_RUN=true` only if you want to skim the flow without external dependencies.

## What’s included
- `phase1_scraping.py` — Generic, table-agnostic scraping & normalization (with DRY_RUN stubs).
- `phase2_load.py` — Postgres loading strategy + audit trail/lineage notes.
- `Home Exercise.docx` — The original prompt/notes for reference.

## How to review
Read the code and inline comments. The focus is **design**, not execution.  
No real web target or database is required for this submission.

---

