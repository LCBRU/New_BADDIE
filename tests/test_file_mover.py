# tests/test_file_mover.py
import os
import shutil
from pathlib import Path
import pandas as pd
import pytest
import tempfile

import file_mover

# Helper to create a small file with content
def _create_file(path: Path, size: int = 1024):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(os.urandom(size))
    return path

@pytest.fixture(autouse=True)
def cleanup_tmpdir(tmp_path, monkeypatch):
    # Use a temporary working directory for tests
    cwd = Path.cwd()
    try:
        monkeypatch.chdir(tmp_path)
        yield tmp_path
    finally:
        monkeypatch.chdir(cwd)

def test_copy_creates_destination_and_copies(tmp_path, monkeypatch):
    # Prepare source file and destination path (destination folder does not exist)
    src_dir = tmp_path / "src"
    dst_dir = tmp_path / "dst" / "subfolder"
    src_file = src_dir / "file1.bin"
    dst_file = dst_dir / "file1.bin"

    _create_file(src_file, size=512)

    # Prepare pairs and monkeypatch pandas.read_sql to return DataFrame if get_pairs_from_db is used
    pairs = [(str(src_file), str(dst_file))]

    # Run copy (move_flag False)
    summary = file_mover.move_files_from_pairs(
        pairs,
        move_flag=False,
        max_workers=1,
        retry_count=1,
        retry_delay=0.1,
        batch_size=10,
        resume=True,
        disable_tqdm=True,
        log_failed_csv="failed_test.csv",
    )

    assert summary["succeeded"] == 1
    assert summary["skipped"] == 0
    assert summary["failed"] == 0
    assert dst_file.exists()
    assert dst_file.stat().st_size == src_file.stat().st_size

def test_resume_skips_existing(tmp_path):
    src_dir = tmp_path / "src2"
    dst_dir = tmp_path / "dst2"
    src_file = src_dir / "file2.bin"
    dst_file = dst_dir / "file2.bin"

    _create_file(src_file, size=256)

    pairs = [(str(src_file), str(dst_file))]

    # First run: copy
    summary1 = file_mover.move_files_from_pairs(
        pairs,
        move_flag=False,
        max_workers=1,
        retry_count=1,
        retry_delay=0.1,
        batch_size=10,
        resume=True,
        disable_tqdm=True,
        log_failed_csv="failed_test.csv",
    )
    assert summary1["succeeded"] == 1
    assert summary1["skipped"] == 0
    assert summary1["failed"] == 0
    assert dst_file.exists()

    # Second run: should skip because file exists and sizes match
    summary2 = file_mover.move_files_from_pairs(
        pairs,
        move_flag=False,
        max_workers=1,
        retry_count=1,
        retry_delay=0.1,
        batch_size=10,
        resume=True,
        disable_tqdm=True,
        log_failed_csv="failed_test.csv",
    )
    assert summary2["succeeded"] == 0
    assert summary2["skipped"] == 1
    assert summary2["failed"] == 0

def test_overwrite_when_size_diff(tmp_path):
    src_dir = tmp_path / "src3"
    dst_dir = tmp_path / "dst3"
    src_file = src_dir / "file3.bin"
    dst_file = dst_dir / "file3.bin"

    _create_file(src_file, size=128)
    # create a destination file with different size to simulate partial copy
    dst_file.parent.mkdir(parents=True, exist_ok=True)
    with open(dst_file, "wb") as fh:
        fh.write(b"short")

    pairs = [(str(src_file), str(dst_file))]

    # Run with resume=True: since sizes differ, destination should be removed and replaced
    summary = file_mover.move_files_from_pairs(
        pairs,
        move_flag=False,
        max_workers=1,
        retry_count=1,
        retry_delay=0.1,
        batch_size=10,
        resume=True,
        disable_tqdm=True,
        log_failed_csv="failed_test.csv",
    )

    assert summary["succeeded"] == 1
    assert summary["skipped"] == 0
    assert summary["failed"] == 0
    assert dst_file.exists()
    assert dst_file.stat().st_size == src_file.stat().st_size
