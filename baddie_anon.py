miimport os
import shutil
import subprocess
import timeit
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from pydicom import dcmread
from alive_progress import alive_bar

#research_study_name = 'ECHO'
research_study_name = 'AIMI'
#research_study_name = 'GENVASC'
#research_study_name = 'AIStudy'


def anonymize_file(in_file, file_out, dictionary_loc, patient_id):
    try:
        ds = dcmread(in_file)
        ds.PatientID = str(patient_id)
        ds.PatientName = str(patient_id)
        ds.save_as(in_file)

        di_cmd = f'dicom-anonymizer --dictionary "{dictionary_loc}dictionary.json" "{in_file}" "{file_out}"'
        subprocess.run(di_cmd, shell=True, text=True, capture_output=True, check=True)
        return True
    except Exception as e:
        print(f"Error processing {in_file}: {e}")
        return False

def process_folder(folder_in, folder_out, dictionary_loc, folder_name, patient_id):
    in_path = os.path.join(folder_in, folder_name)
    out_path = os.path.join(folder_out, folder_name)
    os.makedirs(out_path, exist_ok=True)

    filenames = os.listdir(in_path)
    in_files = [os.path.join(in_path, f) for f in filenames]
    out_files = [os.path.join(out_path, f) for f in filenames]
    howmany = len(filenames)
    print(f'working on: {folder_name} (in folder {folder_in}), {howmany} files to process')

    with ProcessPoolExecutor() as executor:
        results = executor.map(anonymize_file, in_files, out_files, [dictionary_loc]*len(filenames), [patient_id]*len(filenames))

    completed = sum(1 for r in results if r)
    failed = len(filenames) - completed

    shutil.rmtree(in_path, ignore_errors=True)
    return completed, failed

def execute_anonymisation(folder_loc_in, folder_loc_out, dictionary_loc, hours_since_start=0):
    print(f"Input: {folder_loc_in}")
    print(f"Output: {folder_loc_out}")
    print(f"Dictionary: {dictionary_loc}")

    tic = timeit.default_timer()
    folders = [f for f in os.listdir(folder_loc_in) if os.path.isdir(os.path.join(folder_loc_in, f))]

    total_completed = 0
    total_failed = 0

    with alive_bar(len(folders)) as bar:
        for i, folder in enumerate(folders):
            patient_id = hours_since_start + i + 1
            #patient_id = folder
            completed, failed = process_folder(folder_loc_in, folder_loc_out, dictionary_loc, folder, patient_id)
            total_completed += completed
            total_failed += failed
            bar()

    toc = timeit.default_timer()
    print(f"Completed: {total_completed}, Failed: {total_failed}")
    print(f"Time taken: {round((toc - tic) / 60, 2)} minutes")

# Entry point for multiprocessing
if __name__ == "__main__":
    folder_loc_in = f'V:\\Baddie_2B_anonymised\\{research_study_name}\\'
    folder_loc_out = f'V:\\Baddie\\{research_study_name}\\'
    dictionary_loc = f'C:\\Baddie\\{research_study_name}\\'

    start_date = datetime(2025, 1, 1)
    current_date = datetime.now()
    hours_since_start = int((current_date - start_date).total_seconds() / 3600)

    execute_anonymisation(folder_loc_in, folder_loc_out, dictionary_loc, hours_since_start)
