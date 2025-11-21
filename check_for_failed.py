from pathlib import Path
import shutil
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def check_and_move(subfolder: Path, dest: Path, min_files: int, dry_run: bool) -> str:
    """Check if subfolder has fewer than min_files and move if needed."""
    file_count = 0
    try:
        for f in subfolder.iterdir():
            if f.is_file():
                file_count += 1
                if file_count >= min_files:
                    break
        if file_count < min_files:
            if dry_run:
                return f"[DRY RUN] Would move '{subfolder}' to '{dest}'."
            else:
                shutil.move(str(subfolder), str(dest / subfolder.name))
                return f"Moved '{subfolder}' to '{dest}'."
        else:
            return f"'{subfolder}' has {file_count} files (OK)."
    except Exception as e:
        return f"Error processing '{subfolder}': {e}"

def move_folder_if_failed_parallel(
    source_folder: str,
    destination_folder: str,
    min_files: int = 2,
    dry_run: bool = False,
    max_workers: int = None
) -> None:
    src = Path(source_folder)
    dest = Path(destination_folder)

    if not src.exists():
        logging.error(f"Source folder '{src}' does not exist.")
        return
    if not dest.exists():
        logging.error(f"Destination folder '{dest}' does not exist.")
        return

    subfolders = [f for f in src.iterdir() if f.is_dir()]
    logging.info(f"Found {len(subfolders)} subfolders to process.")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(check_and_move, subfolder, dest, min_files, dry_run)
            for subfolder in subfolders
        ]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing folders"):
            logging.info(future.result())

if __name__ == "__main__":
    move_folder_if_failed_parallel(
        r"V:\Baddie_2B_anonymised\SCAD",
        r"V:\Baddie_2B_anonymised\failed",
        min_files=2,
        dry_run=False,
        max_workers=16  # Adjust based on your system (e.g., number of logical cores)
    )