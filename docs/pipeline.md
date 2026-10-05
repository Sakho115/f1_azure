# Pipeline Execution, Scheduling, and Backfill Guide

## Orchestration Overview
The Formula 1 Lakehouse Pipeline supports modular stage execution, conditional skipping of unchanged data, and deterministic historical backfills.

### CLI Execution Command
```bash
python orchestration/pipeline.py --stage all --mode full
```

### Stage Options
- `--stage bronze`: Executes Raw → Bronze extraction and metadata injection.
- `--stage silver`: Executes Bronze → Silver transformations and Delta MERGE upserts.
- `--stage gold`: Recomputes Gold championship standings and telemetry marts.
- `--stage quality`: Executes Great Expectations style automated assertions.
- `--stage all`: Runs the complete pipeline end-to-end.

---

## Historical Backfilling
To replay historical seasons or specific time windows deterministically:
```bash
python orchestration/backfill.py --start-year 2021 --end-year 2022
```

### Backfill Features
- Chronological ordering of historical race windows.
- Delta MERGE idempotency: re-running a backfill never duplicates data.
- Audit column tracking: preserves original `create_date` while updating `update_date`.
