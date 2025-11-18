import os
import pandas as pd
import pyodbc
from tqdm import tqdm
from datetime import datetime
from pydicom import dcmread
import timeit

def dicom_tag_retriaval(folder_loc_in: str, conn_str: str, SQL_tag_table: str, studyID_with_tag_data:str):
    element_names = ['StudyID','FILENAME','SeriesDescription','ImageType','SequenceName',
        'RequestedProcedureDescription','SoftwareVersions','SeriesNumber','InstanceNumber','Rows',
        'Columns','PixelSpacing','PatientName','PatientID','OtherPatientIDs','PatientAddress',
        'DocumentTitle','MIMETypeOfEncapsulatedDocument','ContentLabel','BurnedInAnnotation',
        'SliceLocation','StudyDate']

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()




###############

conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=UHLSQLBRICCS01\\BRICCS01;"
    "Database=i2b2_app03_AIMI;"
    "Trusted_Connection=yes;"
)

dicom_tag_retriaval('V:\\Baddie\\AIMI',conn_str,'Tags','studyID_with_tag_data')
