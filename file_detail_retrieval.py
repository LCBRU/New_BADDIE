#!/usr/bin/env python3
"""
Fast folder-by-folder file inventory -> SQL Server
- Streams metadata using os.scandir() (fast, minimal overhead)
- Parallelizes across top-level folders (IO-bound, effective on network shares)
- Batches inserts with pyodbc fast_executemany
- Resumable: uses dbo.file_scan_progress to skip already processed top-level folders
- Testing mode: process only N folders if testingmode > 0; 0 = process all

Python: 3.13
Requires: pyodbc, tqdm (optional)
"""

import os
import sys
import time
import queue
import signal
import threading
import traceback
import pyodbc
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# =========================
# Configuration
# =========================
ROOT_PATH = r"V:\Baddie\AIMI"  # your root
EXCLUDES = {"Review"}          # top-level folder names to skip; extend if needed
testingmode = 0                # >0 => only process this many top-level folders; 0 => process all

# SQL connection
research_study_name = "AIMI"  # <<-- set this appropriately
CONN_STR = (
    "Driver={ODBC Driver 18 for SQL Server};"
    r"Server=UHLSQLBRICCS01\BRICCS01;"
    f"Database=i2b2_app03_{research_study_name};"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"
)

# Performance tuning
MAX_WORKERS = 24               # try 16–32; tune per your SMB + SQL throughput
BATCH_SIZE = 2000              # 5k–25k typical; commit per batch
QUEUE_MAXSIZE = 8              # backpressure to avoid RAM blowup
PRINT_EVERY = 4000             # progress log frequency (rows)

# =========================
# SQL helpers
# =========================
DDL_FILE_DETAILS = r"""
IF OBJECT_ID('dbo.file_details', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.file_details (
        full_path        nvarchar(2048) NOT NULL,
        directory_path   nvarchar(2048) NOT NULL,
        file_name        nvarchar(260)  NOT NULL,
        size_bytes       bigint         NOT NULL,
        created_utc      datetime2(7)   NOT NULL,
        scanned_utc      datetime2(7)   NOT NULL,
        CONSTRAINT PK_file_details PRIMARY KEY CLUSTERED (full_path)
    );
    CREATE INDEX IX_file_details_dir ON dbo.file_details(directory_path);
    CREATE INDEX IX_file_details_created ON dbo.file_details(created_utc);
END;
"""

DDL_PROGRESS = r"""
IF OBJECT_ID('dbo.file_scan_progress', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.file_scan_progress (
        folder_path       nvarchar(2048) NOT NULL PRIMARY KEY,
        folder_name       nvarchar(260)  NOT NULL,
        last_scanned_utc  datetime2(7)   NOT NULL DEFAULT (SYSUTCDATETIME())
    );
END;
"""

SQL_INSERT_MAIN = r"""
INSERT INTO dbo.file_details
    (full_path, directory_path, file_name, size_bytes, created_utc, scanned_utc)
VALUES (?,?,?,?,?,?)
"""

SQL_UPSERT_PROGRESS = r"""
MERGE dbo.file_scan_progress AS tgt
USING (VALUES (?, ?, SYSUTCDATETIME())) AS src(folder_path, folder_name, last_scanned_utc)
ON tgt.folder_path = src.folder_path
WHEN MATCHED THEN UPDATE SET last_scanned_utc = src.last_scanned_utc, folder_name = src.folder_name
WHEN NOT MATCHED THEN INSERT (folder_path, folder_name, last_scanned_utc)
VALUES (src.folder_path, src.folder_name, src.last_scanned_utc);
"""

SQL_SELECT_DONE_FOLDERS = r"SELECT folder_path FROM dbo.file_scan_progress;"

# =========================
# Global state for clean shutdown
# =========================
stop_event = threading.Event()

def handle_sigint(signum, frame):
    print("\nReceived interrupt. Attempting graceful shutdown...", file=sys.stderr, flush=True)
    stop_event.set()

signal.signal(signal.SIGINT, handle_sigint)

# =========================
# Inserter thread
# =========================
class Inserter(threading.Thread):
    """
    Single-threaded inserter that:
      - Consumes ('rows', list[tuple]) batches from a queue and executes batched INSERTs.
      - Consumes ('folder_done', folder_path, folder_name) markers and upserts progress.
      - Stops on ('STOP',).
    This guarantees folder_done is recorded only after all its queued batches are committed.
    """
    def __init__(self, conn_str: str, in_q: queue.Queue):
        super().__init__(daemon=True)
        self.conn_str = conn_str
        self.in_q = in_q
        self.row_count = 0
        self.last_print = 0
        self.conn = None
        self.cur = None

    def run(self):
        try:
            self.conn = pyodbc.connect(self.conn_str, autocommit=False)
            self.cur = self.conn.cursor()
            self.cur.fast_executemany = True

            # Ensure tables exist
            self.cur.execute(DDL_FILE_DETAILS)
            self.cur.execute(DDL_PROGRESS)
            self.conn.commit()

            while True:
                item = self.in_q.get()
                if item is None:
                    # Legacy sentinel (unused)
                    break

                kind = item[0]
                if kind == 'STOP':
                    break

                if kind == 'rows':
                    rows = item[1]
                    if not rows:
                        continue
                    self.cur.executemany(SQL_INSERT_MAIN, rows)
                    self.conn.commit()
                    self.row_count += len(rows)
                    if self.row_count - self.last_print >= PRINT_EVERY:
                        print(f"[Inserter] Inserted {self.row_count:,} rows...", flush=True)
                        self.last_print = self.row_count

                elif kind == 'folder_done':
                    folder_path, folder_name = item[1], item[2]
                    self.cur.execute(SQL_UPSERT_PROGRESS, (folder_path, folder_name))
                    self.conn.commit()
                    print(f"[Inserter] Marked done: {folder_name}", flush=True)

                else:
                    print(f"[Inserter] Unknown item kind: {kind}", file=sys.stderr)

        except Exception as e:
            print("[Inserter] ERROR:", e, file=sys.stderr)
            traceback.print_exc()
            stop_event.set()
        finally:
            try:
                if self.cur is not None:
                    self.cur.close()
                if self.conn is not None:
                    self.conn.close()
            except Exception:
                pass
            print("[Inserter] Exiting. Total rows inserted:", f"{self.row_count:,}")

# =========================
# File enumeration per folder
# =========================
def scan_folder(folder_path: str, folder_name: str, out_q: queue.Queue, batch_size: int):
    """
    Walks folder_path recursively using scandir, gathers file metadata,
    and emits ('rows', batch) and final ('folder_done', folder_path, folder_name) to out_q.
    """
    if stop_event.is_set():
        return

    rows_batch = []
    # Use a single scanned_utc per batch to avoid per-file datetime object overhead
    def flush_batch():
        nonlocal rows_batch
        if rows_batch:
            out_q.put(('rows', rows_batch))
            rows_batch = []

    def safe_scandir(path):
        try:
            return list(os.scandir(path))
        except Exception:
            return []

    # Iterative DFS using a stack to avoid recursion limits
    stack = [folder_path]
    files_seen = 0

    while stack and not stop_event.is_set():
        current = stack.pop()
        entries = safe_scandir(current)
        # Prepare scanned_utc for this batch (created_utc is per-file)
        scanned_utc = datetime.utcnow()  # naive UTC for SQL datetime2

        for entry in entries:
            try:
                if entry.is_dir(follow_symlinks=False):
                    stack.append(entry.path)
                elif entry.is_file(follow_symlinks=False):
                    st = entry.stat(follow_symlinks=False)
                    # On Windows, st_ctime is creation time (best-effort over SMB too).
                    # We pass naive UTC datetimes for SQL datetime2.
                    created_utc = datetime.utcfromtimestamp(st.st_ctime)
                    full_path = entry.path
                    directory_path = os.path.dirname(full_path)
                    file_name = entry.name
                    size_bytes = st.st_size

                    rows_batch.append((full_path, directory_path, file_name,
                                       int(size_bytes), created_utc, scanned_utc))
                    files_seen += 1
                    if len(rows_batch) >= batch_size:
                        flush_batch()
                        scanned_utc = datetime.utcnow()  # refresh for next batch
            except FileNotFoundError:
                # File disappeared between dir read and stat
                continue
            except PermissionError:
                continue
            except OSError:
                continue

    # Flush any remaining rows and mark folder as done
    if rows_batch:
        out_q.put(('rows', rows_batch))
    out_q.put(('folder_done', folder_path, folder_name))
    print(f"[Worker] Folder complete: {folder_name} ({files_seen:,} files)")

# =========================
# Discover folders & skip processed
# =========================
def list_top_level_folders(root_path: str):
    try:
        with os.scandir(root_path) as it:
            return [e for e in it if e.is_dir(follow_symlinks=False)]
    except Exception as e:
        print(f"ERROR listing root '{root_path}': {e}", file=sys.stderr)
        return []

def fetch_already_processed(conn_str: str):
    processed = set()
    try:
        with pyodbc.connect(conn_str, autocommit=True) as c:
            with c.cursor() as cur:
                # Create progress table if missing, to avoid first-run error
                cur.execute(DDL_PROGRESS)
                for row in cur.execute(SQL_SELECT_DONE_FOLDERS):
                    processed.add(row[0].strip())
    except Exception as e:
        print(f"WARNING: Could not fetch processed folders: {e}", file=sys.stderr)
    return processed


def _count_files_recursive(folder_path: str) -> int:
    """
    Fast, robust recursive file counter using scandir (no symlinks).
    Returns total files found under folder_path.
    """
    total = 0
    stack = [folder_path]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as it:
                for entry in it:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(entry.path)
                        elif entry.is_file(follow_symlinks=False):
                            total += 1
                    except (PermissionError, FileNotFoundError, OSError):
                        # Skip entries we cannot access
                        continue
        except (PermissionError, FileNotFoundError, OSError):
            # Skip directories we cannot open
            continue
    return total


def check_and_fix_missing_progress(conn_str: str):
    """
    1) Finds folders that appear in dbo.file_details but not in dbo.file_scan_progress.
    2) For each, compares DB count vs filesystem count.
    3) Prints a 'missing from SQL server' report:
          - If counts match, automatically marks folder as done in progress table.
          - If mismatch exists for any folder, signals caller to halt.
    Returns:
        fixed_paths:   list[str] of folders that were auto-marked as done
        mismatches:    list[tuple(folder, db_count, fs_count)]
    NOTE: This function does not start scanning; it's a preflight check.
    """
    fixed_paths = []
    mismatches = []

    # Collect candidate folders from SQL
    try:
        with pyodbc.connect(conn_str, autocommit=True) as cx:
            cur = cx.cursor()
            # Candidate folders: exist in file_details but not in file_scan_progress
            rows = cur.execute(
                """
                SELECT d.directory_path
                FROM dbo.file_details AS d
                LEFT JOIN dbo.file_scan_progress AS p
                  ON p.folder_path = d.directory_path
                WHERE p.folder_path IS NULL
                GROUP BY d.directory_path
                """
            ).fetchall()

            candidates = [r[0].strip() for r in rows if r[0]]
            print("\n=== missing from SQL server ===")
            if not candidates:
                print("No partially scanned folders detected.\n")
                return fixed_paths, mismatches

            for folder in candidates:
                # Count in DB: use full_path LIKE '<folder>\%' to include all subdirectories
                like_path = folder.rstrip("\\") + "\\%"
                db_count = cur.execute(
                    "SELECT COUNT(*) FROM dbo.file_details WHERE full_path LIKE ?",
                    (like_path,)
                ).fetchone()[0]

                # Count on filesystem
                fs_count = _count_files_recursive(folder)

                status = "OK" if db_count == fs_count else "MISMATCH"
                print(f"{folder}  —  DB: {db_count:,}  |  FS: {fs_count:,}  [{status}]")

                if db_count == fs_count:
                    # auto-mark as done (so it won’t be re-scanned)
                    folder_name = os.path.basename(folder.rstrip("\\/"))
                    cur.execute(SQL_UPSERT_PROGRESS, (folder, folder_name))
                    fixed_paths.append(folder)
                else:
                    mismatches.append((folder, db_count, fs_count))

            print()  # newline after the report

    except Exception as e:
        print(f"[Preflight] ERROR during consistency check: {e}", file=sys.stderr)
        traceback.print_exc()

    return fixed_paths, mismatches


# =========================
# Main
# =========================
def main():
    t_start = time.time()

    # Resolve target folders
    all_dirs = list_top_level_folders(ROOT_PATH)
    if not all_dirs:
        print("No top-level directories found. Check ROOT_PATH.")
        return

    # Filter by excludes and already processed (progress table stores full path)
    processed_full_paths = fetch_already_processed(CONN_STR)
    todo = []
    for e in all_dirs:
        if e.name.strip() in EXCLUDES:
            continue
        full = e.path
        if full.strip() in processed_full_paths:
            continue
        todo.append(e)

    # Apply testing mode
    if testingmode > 0:
        todo = todo[:testingmode]

    print(f"Root: {ROOT_PATH}")
    print(f"Top-level folders total: {len(all_dirs)} | Excluded: {len(all_dirs)-len(todo)} | To process: {len(todo)}")
    if not todo:
        print("Nothing to do. Exiting.")
        return

    # === Preflight consistency check ===
    fixed_paths, mismatches = check_and_fix_missing_progress(CONN_STR)

    # Remove any folders we just auto-marked as done from this run's todo list
    if fixed_paths:
        fixed_set = {p.strip() for p in fixed_paths}
        todo = [e for e in todo if e.path.strip() not in fixed_set]
        print(f"Auto-marked as done (progress fixed): {len(fixed_paths)}")
        print(f"Updated 'To process' count: {len(todo)}")

    # Pause so the user can review the report
    input("Hit ENTER to continue...")

    # If any mismatch was found, halt the program (ask user to resolve and re-run)
    if mismatches:
        # mismatches is a list of tuples: (folder_path, db_count, fs_count)
        bad_folders = [folder for folder, _, _ in mismatches]

        # Build quoted SQL literals, one per line, indented
        folder_list = "\n".join(f"    '{f}'" for f in bad_folders)

        print(f"""
        Please resolve mismatch and re-run.

        Consider running this SQL on the SQL Server *after verifying*:

        DELETE FROM [dbo].[file_details]
        WHERE directory_path IN (
        {folder_list}
        );

        Halting.
        """)
        return
    # === END Preflight consistency check  ===

    # Start inserter
    q = queue.Queue(maxsize=QUEUE_MAXSIZE)
    inserter = Inserter(CONN_STR, q)
    inserter.start()

    # Process folders with threads
    futures = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        for e in todo:
            futures.append(ex.submit(scan_folder, e.path, e.name, q, BATCH_SIZE))

        # Monitor futures for early failure
        try:
            for f in as_completed(futures):
                if stop_event.is_set():
                    break
                exc = f.exception()
                if exc:
                    print("[Main] Worker error:", exc, file=sys.stderr)
                    stop_event.set()
                    break
        except KeyboardInterrupt:
            stop_event.set()

    # Tell inserter to stop
    q.put(('STOP',))
    inserter.join()

    dt = time.time() - t_start
    print(f"Done in {dt/60:.1f} min.")

if __name__ == "__main__":
    main()



