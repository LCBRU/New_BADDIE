from concurrent.futures import ProcessPoolExecutor

from pydicom import dcmread

import os
import pyodbc
import multiprocessing
from tqdm import tqdm
import pandas as pd
import time


#print(f"Using {os.cpu_count()} workers")

def extract_dicom_tags(file_path, element_names, study_id, filename):
    try:
        ds = dcmread(file_path)
        values = []
        for en in element_names:
            if en == 'StudyID':
                value = study_id
            elif en == 'FILENAME':
                value = filename
            elif en == 'CMD':
                value = f"COPY {file_path} REVIEW\\{study_id}_{filename}"
            else:
                attr = getattr(ds, en, '')
                value = ', '.join(map(str, attr)) if isinstance(attr, list) else str(attr)
            values.append(value)
        return values
    except Exception as e:
        print(f"Failed to process {file_path}: {e}")
        return None


def process_folder(folder_path, element_names, study_id):
    filenames = os.listdir(folder_path)
    file_paths = [os.path.join(folder_path, f) for f in filenames]

    start_df = time.time ()
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
        results = list(executor.map(
            extract_dicom_tags,
            file_paths,
            [element_names] * len(file_paths),
            [study_id] * len(file_paths),
            filenames
        ))
    end_df = time.time ()
    print(f"[{study_id}] Time to create DataFrame: {end_df - start_df:.2f} seconds")
    # Filter out failed results
    results = [r for r in results if r is not None]
    df = pd.DataFrame(results, columns=element_names)
    return df


def insert_dataframe_to_sql(df, conn_str, SQL_tag_table):
    start_sql = time.time()
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    for _, row in df.iterrows():
        placeholders = ', '.join(['?'] * len(row))
        insert_sql = f"INSERT INTO {SQL_tag_table} ({', '.join(df.columns)}) VALUES ({placeholders})"

        try:
            cursor.execute(insert_sql, tuple(row))
        except Exception as e:
            print(f"Failed to insert row: {e}")
    conn.commit()
    cursor.close()
    conn.close()
    end_sql = time.time()
    print(f"Time to upload to SQL: {end_sql - start_sql:.2f} seconds")



def dicom_tag_retriaval_parallel(folder_loc_in, conn_str, SQL_tag_table, studyID_with_tag_data,element_names):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute(f"SELECT DISTINCT StudyID FROM {studyID_with_tag_data}")
    processed_study_ids = {row[0].strip() for row in cursor.fetchall()}
    cursor.close()
    conn.close()

    folder_list = [
        name for name in os.listdir(folder_loc_in)
        if os.path.isdir(os.path.join(folder_loc_in, name))
           and name.strip() not in processed_study_ids
           and name.strip() != 'Review'
    ]

    for folder in tqdm(folder_list, desc="Processing Folders"):
        folder_path = os.path.join(folder_loc_in, folder)
        df = process_folder(folder_path, element_names, folder)
        insert_dataframe_to_sql(df, conn_str, SQL_tag_table)



research_study_name = 'AIMI'

if __name__ == "__main__":
    multiprocessing.freeze_support ()
    element_names = ['StudyID', 'FILENAME', 'SeriesDescription', 'ImageType', 'SequenceName'
        , 'RequestedProcedureDescription', 'SoftwareVersions', 'SeriesNumber', 'InstanceNumber', 'Rows'
        , 'Columns', 'PixelSpacing', 'PatientName', 'PatientID', 'OtherPatientIDs', 'PatientAddress'
        , 'DocumentTitle', 'MIMETypeOfEncapsulatedDocument', 'ContentLabel', 'BurnedInAnnotation'
        , 'SliceLocation', 'StudyDate']

    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=UHLSQLBRICCS01\\BRICCS01;"
        f"Database=i2b2_app03_{research_study_name};"
        "Trusted_Connection=yes;"
    )

    dicom_tag_retriaval_parallel(
        f'V:\\Baddie\\{research_study_name}',
        conn_str,
        'Tags',
        'studyID_with_tag_data',
        element_names
    )


