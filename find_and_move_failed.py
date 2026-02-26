import os
import shutil
import pyodbc

# Define the study name and SQL connection string
research_study_name = 'SCAD'
conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=UHLSQLBRICCS01\\BRICCS01;"
    f"Database=i2b2_app03_{research_study_name};"
    "Trusted_Connection=yes;"
)

# Define the source and destination folders
current_folder = f"V:\\Baddie\{research_study_name}"
rerun_folder = r'V:\Baddie\failed'

# Get all folders in the source directory
all_folders = [f for f in os.listdir(current_folder) if os.path.isdir(os.path.join(current_folder, f))]
total = len(all_folders)
print(f"Checking {total} folders in '{current_folder}'...")

# Connect to SQL and get processed StudyIDs
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
cursor.execute("SELECT DISTINCT StudyID FROM studyID_with_tag_data")
processed_study_ids = {row[0].strip() for row in cursor.fetchall()}
cursor.close()
conn.close()

# Filter out folders that are already processed
all_folders = [f for f in all_folders if f.strip() not in processed_study_ids and f.strip() != 'Review']

# Continue with the rest of your logic
total = len(all_folders)
counter = 0
folders_to_move = []

for folder_name in all_folders:
    counter += 1
    folder_path = os.path.join(current_folder, folder_name)
    print(f"[{counter}/{total}] Checking folder: {folder_name}")

    file_count = len([f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))])
    if file_count < 3:
        folders_to_move.append(folder_name)

print(f"\nFound {len(folders_to_move)} folders with less than 3 files. Moving them to '{rerun_folder}'...")

for folder_name in folders_to_move:
    src = os.path.join(current_folder, folder_name)
    dst = os.path.join(rerun_folder, folder_name)
    shutil.move(src, dst)
    print(f"Moved: {folder_name}")

print("\nDone. All matching folders have been moved.")
