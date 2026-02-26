# file_mover.py
import shutil
import time
import csv
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import pyodbc
import pandas as pd
from tqdm import tqdm
from typing import List, Tuple, Dict, Any, Optional

# === CONFIG ===
RESEARCH_STUDY_NAME = "AIMI"
TABLE_NAME = "dbo.file_copy_list"
SOURCE_COL = "Source"
DEST_COL = "Destination"

# If False or None => no SQL TOP limit and no programmatic limit.
# If an integer (e.g., 40) => SQL will use TOP (40) and the program will only process that many rows.
NUMBER_TO_LIMIT_TO: Optional[int] = 40

MOVE_INSTEAD_OF_COPY = False
MAX_WORKERS = max(1, multiprocessing.cpu_count() - 1)
BATCH_SIZE = 10000
RETRY_COUNT = 2
RETRY_DELAY = 1.0
LOG_FAILED_CSV = "failed_moves.csv"
DRIVER = "ODBC Driver 18 for SQL Server"
SERVER = r"UHLSQLBRICCS01\BRICCS01"
DATABASE = f"i2b2_app03_{RESEARCH_STUDY_NAME}"

# Connection string used by get_pairs_from_db
CONN_STR = (
    f"Driver={{{DRIVER}}};"
    f"Server={SERVER};"
    f"Database={DATABASE};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

# Build SQL query dynamically to include TOP when NUMBER_TO_LIMIT_TO is set
if NUMBER_TO_LIMIT_TO and isinstance(NUMBER_TO_LIMIT_TO, int) and NUMBER_TO_LIMIT_TO > 0:
    SQL_QUERY = f"SELECT TOP ({NUMBER_TO_LIMIT_TO}) {SOURCE_COL}, {DEST_COL} FROM {TABLE_NAME};"
else:
    SQL_QUERY = f"SELECT {SOURCE_COL}, {DEST_COL} FROM {TABLE_NAME};"


def get_pairs_from_db(conn_str: str = CONN_STR, sql: str = SQL_QUERY) -> List[Tuple[str, str]]:
    """
    Read source/destination pairs from SQL Server and return a list of (src, dst) tuples.
    If NUMBER_TO_LIMIT_TO is set, the SQL already includes TOP(...) so only that many rows are returned.
    """
    conn = pyodbc.connect(conn_str, autocommit=True)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()

    if SOURCE_COL not in df.columns or DEST_COL not in df.columns:
        raise RuntimeError(f"Expected columns {SOURCE_COL} and {DEST_COL} in query result")

    pairs = list(zip(df[SOURCE_COL].astype(str).tolist(), df[DEST_COL].astype(str).tolist()))
    return pairs


def _worker_task(args: Tuple[str, str, bool, int, float, bool]) -> Tuple[bool, str, str, str]:
    """
    Worker executed in separate process.
    args: (src_str, dst_str, move_flag, retry_count, retry_delay, resume_flag)
    Returns: (success_bool, src, dst, message)
    message: 'ok', 'skipped_exists', 'source_missing', 'error:...'
    """
    src_str, dst_str, move_flag, retry_count, retry_delay, resume_flag = args
    try:
        src = Path(str(src_str).strip())
        dst = Path(str(dst_str).strip())

        if not src.exists():
            return (False, str(src), str(dst), "source_missing")

        # Ensure destination directory exists
        dst_parent = dst.parent
        dst_parent.mkdir(parents=True, exist_ok=True)

        # Resume logic: if destination exists and appears identical, skip
        if resume_flag and dst.exists():
            try:
                if src.stat().st_size == dst.stat().st_size:
                    return (True, str(src), str(dst), "skipped_exists")
                else:
                    # sizes differ: remove destination and proceed to copy/move
                    dst.unlink()
            except Exception:
                # If any stat/unlink error, proceed to attempt copy/move and let it fail if necessary
                pass

        attempt = 0
        while True:
            try:
                if move_flag:
                    shutil.move(str(src), str(dst))
                else:
                    shutil.copy2(str(src), str(dst))
                    # verify size
                    if src.exists() and src.stat().st_size != dst.stat().st_size:
                        raise IOError("size_mismatch_after_copy")
                return (True, str(src), str(dst), "ok")
            except Exception as e:
                attempt += 1
                if attempt > retry_count:
                    return (False, str(src), str(dst), f"error:{type(e).__name__}:{e}")
                time.sleep(retry_delay)
    except Exception as e:
        return (False, src_str, dst_str, f"fatal:{type(e).__name__}:{e}")


def move_files_from_pairs(
    pairs: List[Tuple[str, str]],
    move_flag: bool = MOVE_INSTEAD_OF_COPY,
    max_workers: int = MAX_WORKERS,
    retry_count: int = RETRY_COUNT,
    retry_delay: float = RETRY_DELAY,
    batch_size: int = BATCH_SIZE,
    log_failed_csv: str = LOG_FAILED_CSV,
    resume: bool = True,
    disable_tqdm: bool = False,
    number_limit: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Move or copy files given a list of (src, dst) pairs.

    If number_limit is provided (int), only the first number_limit pairs are processed.
    Returns a summary dict:
      {
        "total": int,
        "succeeded": int,
        "skipped": int,
        "failed": int,
        "failed_rows": [(src,dst,msg), ...]
      }
    """
    # Apply programmatic limit if provided
    if number_limit and isinstance(number_limit, int) and number_limit > 0:
        pairs = pairs[:number_limit]

    total = len(pairs)
    failed_rows = []
    succeeded = 0
    skipped = 0

    def chunked_iterable(iterable, size):
        it = iter(iterable)
        while True:
            chunk = []
            try:
                for _ in range(size):
                    chunk.append(next(it))
            except StopIteration:
                if chunk:
                    yield chunk
                break
            yield chunk

    workers = max(1, max_workers)
    with ProcessPoolExecutor(max_workers=workers) as exe:
        futures = []
        for batch in chunked_iterable(pairs, batch_size):
            for src, dst in batch:
                futures.append(
                    exe.submit(
                        _worker_task,
                        (src, dst, move_flag, retry_count, retry_delay, resume),
                    )
                )

            # iterate completed futures for this batch
            for f in tqdm(as_completed(futures), total=len(futures), desc="Processing", unit="file", disable=disable_tqdm):
                success, src, dst, msg = f.result()
                if success:
                    if msg == "skipped_exists":
                        skipped += 1
                    else:
                        succeeded += 1
                else:
                    failed_rows.append((src, dst, msg))
            futures.clear()

    # write failures if any
    if failed_rows:
        with open(log_failed_csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["source", "destination", "error"])
            writer.writerows(failed_rows)

    return {
        "total": total,
        "succeeded": succeeded,
        "skipped": skipped,
        "failed": len(failed_rows),
        "failed_rows": failed_rows,
    }


def main():
    # Read pairs from DB. SQL_QUERY already respects NUMBER_TO_LIMIT_TO if set.
    pairs = get_pairs_from_db()
    # As an extra safety, you can also pass number_limit to move_files_from_pairs to enforce a limit
    # regardless of SQL behavior. Here we use the same NUMBER_TO_LIMIT_TO config.
    number_limit = NUMBER_TO_LIMIT_TO if (NUMBER_TO_LIMIT_TO and isinstance(NUMBER_TO_LIMIT_TO, int)) else None
    summary = move_files_from_pairs(pairs, number_limit=number_limit)
    print(f"Processed {summary['total']} files: succeeded={summary['succeeded']}, skipped={summary['skipped']}, failed={summary['failed']}")
    if summary["failed"]:
        print(f"Failures written to {LOG_FAILED_CSV}")


if __name__ == "__main__":
    main()
