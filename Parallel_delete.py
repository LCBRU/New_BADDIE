# parallel_delete.py

import time
import csv
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import pyodbc
import pandas as pd
from typing import List, Tuple, Dict, Any, Optional

# === CONFIG ===
RESEARCH_STUDY_NAME = "AIMI"
TABLE_NAME = "dbo.TBD_LIST"
DELETE_COL = "DeleteThese"

# If None/False → no limit
# If integer → SQL uses TOP(n)
NUMBER_TO_LIMIT_TO: Optional[int] = 0

MAX_WORKERS = max(1, multiprocessing.cpu_count() - 1)
RETRY_COUNT = 1
RETRY_DELAY = 0.5
LOG_FAILED_CSV = "failed_deletes.csv"

DRIVER = "ODBC Driver 18 for SQL Server"
SERVER = r"UHLSQLBRICCS01\BRICCS01"
DATABASE = f"i2b2_app03_{RESEARCH_STUDY_NAME}"

CONN_STR = (
    f"Driver={{{DRIVER}}};"
    f"Server={SERVER};"
    f"Database={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

# Build SQL query
if NUMBER_TO_LIMIT_TO and isinstance(NUMBER_TO_LIMIT_TO, int) and NUMBER_TO_LIMIT_TO > 0:
    SQL_QUERY = f"SELECT TOP ({NUMBER_TO_LIMIT_TO}) {DELETE_COL} FROM {TABLE_NAME} ORDER BY {DELETE_COL};"
else:
    SQL_QUERY = f"SELECT {DELETE_COL} FROM {TABLE_NAME} ORDER BY {DELETE_COL};"


# --------------------------------------------------------------------
# DB → List of paths
# --------------------------------------------------------------------
def get_delete_list(conn_str: str = CONN_STR, sql: str = SQL_QUERY) -> List[str]:
    conn = pyodbc.connect(conn_str, autocommit=True)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    if DELETE_COL not in df.columns:
        raise RuntimeError(f"Expected column {DELETE_COL}")

    # Convert to full path strings
    return df[DELETE_COL].astype(str).tolist()


# --------------------------------------------------------------------
# Worker: delete a file
# --------------------------------------------------------------------
def delete_worker(args: Tuple[str, int, float]) -> Tuple[bool, str, str]:
    path_str, retry_count, retry_delay = args

    try:
        p = Path(path_str)

        if not p.exists():
            return (False, path_str, "missing")

        attempts = 0
        while True:
            try:
                p.unlink()
                return (True, path_str, "deleted")
            except Exception as e:
                attempts += 1
                if attempts > retry_count:
                    return (False, path_str, f"error:{type(e).__name__}:{e}")
                time.sleep(retry_delay)

    except Exception as e:
        return (False, path_str, f"fatal:{type(e).__name__}:{e}")


# --------------------------------------------------------------------
# Parallel delete engine
# --------------------------------------------------------------------
def delete_files(
    paths: List[str],
    max_workers: int = MAX_WORKERS,
    retry_count: int = RETRY_COUNT,
    retry_delay: float = RETRY_DELAY,
) -> Dict[str, Any]:

    total = len(paths)
    deleted = 0
    failed = 0
    failures = []

    with ProcessPoolExecutor(max_workers=max_workers) as exe:
        futures = [exe.submit(delete_worker, (p, retry_count, retry_delay)) for p in paths]

        for f in as_completed(futures):
            success, path, msg = f.result()

            if success:
                deleted += 1
            else:
                failed += 1
                failures.append((path, msg))

            # live-running status line
            print(f"\rDeleted: {deleted}   Failed: {failed}   Total: {total}", end="")

    print()  # newline after status line

    if failures:
        with open(LOG_FAILED_CSV, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["path", "error"])
            writer.writerows(failures)

    return {
        "total": total,
        "deleted": deleted,
        "failed": failed,
        "failed_rows": failures,
    }


# --------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------
def main():
    print("Loading rows from database...")
    paths = get_delete_list()

    total = len(paths)
    print(f"Found {total} files to delete.")

    # Confirmation prompt
    confirm = input(f"Do you want to delete {total} files? (y/n): ").strip().lower()
    if confirm != "y":
        print("Operation cancelled.")
        return

    summary = delete_files(paths)

    print(f"\nCompleted.")
    print(
        f"Total={summary['total']}  "
        f"Deleted={summary['deleted']}  "
        f"Failed={summary['failed']}"
    )

    if summary["failed"]:
        print(f"Failed deletions written to {LOG_FAILED_CSV}")

if __name__ == "__main__":
    main()
