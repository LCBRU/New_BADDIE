from pathlib import Path
import shutil
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def move_folder_if_failed(
    source_folder: str,
    destination_folder: str,
    min_files: int = 2,
    dry_run: bool = False
) -> None:
    """
    Move subfolders from source_folder to destination_folder if they contain fewer than min_files.

    Args:
        source_folder (str): Path to the source directory.
        destination_folder (str): Path to the destination directory.
        min_files (int): Minimum number of files required to keep the folder.
        dry_run (bool): If True, only log actions without moving folders.
    """
    src = Path(source_folder)
    dest = Path(destination_folder)

    # Validate paths
    if not src.exists():
        logging.error(f"Source folder '{src}' does not exist.")
        return
    if not dest.exists():
        logging.error(f"Destination folder '{dest}' does not exist.")
        return

    # Iterate through subfolders
    for subfolder in src.iterdir():
        if subfolder.is_dir():
            num_files = sum(1 for f in subfolder.iterdir() if f.is_file())
            logging.info(f"'{subfolder}' has {num_files} files.")

            if num_files < min_files:
                if dry_run:
                    logging.info(f"[DRY RUN] Would move '{subfolder}' to '{dest}'.")
                else:
                    try:
                        shutil.move(str(subfolder), str(dest / subfolder.name))
                        logging.info(f"Moved '{subfolder}' to '{dest}'.")
                    except Exception as e:
                        logging.error(f"Failed to move '{subfolder}': {e}")

# Example usage
move_folder_if_failed(
    r"V:\Baddie_2B_anonymised\SCAD",
    r"V:\Baddie_2B_anonymised\failed",
    min_files=2,
    dry_run=False)

