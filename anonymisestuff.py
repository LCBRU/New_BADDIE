import csv
import subprocess
import threading
import bs4
from pydicom import *
from bs4 import BeautifulSoup as bs
import lxml
import pandas as pd
import csv
import re
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import tostring
import warnings
import psutil
import shutil
from itertools import islice
from pathlib import Path
import timeit
import os
#import onedrivesdk
from alive_progress import alive_bar
import time as mytime
from datetime import datetime, time
from dicomanonymizer import *
#import logging
import sys


#folder = f'aimi0012370'


#folder_loc_folder = f'{folder_loc}{folder}\\'
#list = os.listdir(f'{folder_loc_folder}')
#mkcmd = f'mkdir {folder_loc}{folder}\\de'
#subprocess.run ( mkcmd, shell=True, text=True, capture_output=True ).stderr
#print(os.listdir ( folder_loc ))

# Define the start date
start_date = datetime(2025, 1, 1)
# Get the current date and time
current_date = datetime.now()
# Calculate the difference in hours
hours_since_start = int((current_date - start_date).total_seconds() / 3600)
print(f"The number of hours since January 1st, 2025 is {hours_since_start:.2f} hours, we'll use this to start the Patient_id to ensure no overlap of IDs")


def execute_anonymisation(folder_loc_in,folder_loc_out,dictionary_loc):
    print(folder_loc_in)
    print(folder_loc_out)
    print(dictionary_loc)
    tic_a = timeit.default_timer()
    #folder_loc_folder = f'{folder_loc_in}{folder}\\'
    Folder_list = []
    Folder_list = [name for name in os.listdir(folder_loc_in)]
    #Folder_list = ['ScadReg03221', 'ScadReg03147']
    to_process = len(Folder_list)
    print(Folder_list)
    print(to_process)
    filenames = []
    with alive_bar(len(Folder_list)) as bar:
        fol_number_for_patient_id = hours_since_start
        for fol in Folder_list:
            fol_number_for_patient_id += 1
            #print(fol)
            mkcmd = f'mkdir {folder_loc_out}{fol}'
            #print(mkcmd)
            subprocess.run(mkcmd, shell=True, text=True, capture_output=True).stderr
            filenames = []
            for entry in os.listdir(f'{folder_loc_in}{fol}'):
                filenames.append(entry)
                # print(entry)
            completed = 0
            failed = 0
            n = len(filenames)
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            print(f'Starting with Folder {fol} : {n} files. Time started : {dt_string}')
            for filename in filenames:
                tic_a = timeit.default_timer ()
                # print(filename)
                in_file = f"{folder_loc_in}{fol}\\{filename}"
                file_out = f"{folder_loc_out}{fol}\\{filename}"
                # regexp' 'R.+' '{folder} replaced with a simple empty
                ds = dcmread(in_file)
                # Edit the (0010,0020) 'Patient ID' element
                #ds.PatientID = f'{fol}'
                #ds.PatientName = f'{fol}'

                ds.PatientID = f'{fol_number_for_patient_id}'
                ds.PatientName = f'{fol_number_for_patient_id}'
                #ds.OtherPatientIDs= f'########'
                #ds.PatientAddress= f'########'
                #ds.PatientBirthDate = f'19010101'
                #ds.RequestingPhysician = f'########'
                ds.save_as(in_file)


                #di_cmd = f'dicom-anonymizer --dictionary {folder_loc_Processing}dictionary.json {in_file} {file_out}"  # --extra-rules extra_rules.json
                di_cmd = f'dicom-anonymizer --dictionary {dictionary_loc}dictionary.json "{in_file}" "{file_out}" '  # --extra-rules extra_rules.json
                print(di_cmd)
                # ExecuteAnonymisation = \
                try:
                    print(f' working on Folder {fol}...')
                    subprocess.run(di_cmd,shell=True ,text=True ,capture_output=True, check=True).stderr
                    #print(di_cmd)
                    completed += 1

                except subprocess.CalledProcessError as e:
                    failed += 1
                    print(f"Error executing command: {e}")
            bar()
            print(f'Finished with Folder {fol} : {n} files')

    processed = f'completed:{completed} / failed: {failed}'
    toc_a = timeit.default_timer ()
    log = f'{processed} in {round ( (toc_a - tic_a) / 60, 1 )} Minutes.'
    print(log)
    print('started:')
    print(tic_a)
    print('stopped:')
    print(toc_a)
    #logging.info ( log )

def execute_re_anonymisation_acc_num(folder):
    tic_a = timeit.default_timer ()

    to_process = len ( [name for name in os.listdir ( f'{folder_loc}{folder}/de' ) if
                        os.path.isfile ( os.path.join ( f'{folder_loc}{folder}/de', name ) )] )
    filenames = []
    f = 0

    # Create Psudonimised File and open for writing
    # mkcmd = f'mkdir ./{folder_loc}{folder}/de'
    # subprocess.run(mkcmd,shell=True ,text=True ,capture_output=True).stderr
    filenames = []

    for entry in os.listdir ( f'{folder_loc}{folder}/de' ):
        filenames.append ( entry )
        shutil.move ( os.path.join ( f'{folder_loc}{folder}/de', entry ), f'{folder_loc}{folder}' )
        # print(entry)
    for filename in filenames:
        # print(filename)
        in_file = f'{folder_loc}{folder}/{filename}'
        file_out = f'{folder_loc}{folder}/de/{filename}'
        di_cmd = f"dicom-anonymizer -t '(0x0010, 0x0020)' 'regexp' 'R.+' '{folder}' --dictionary dictionary.json {in_file} {file_out}"  # --extra-rules extra_rules.json
        print ( f'{filename}' )
        ExecuteAnonymisation = subprocess.run ( [di_cmd], shell=True, text=True, capture_output=True ).stdout
    processed = len ( [name for name in os.listdir ( f'{folder_loc}{folder}/de/' ) if
                       os.path.isfile ( os.path.join ( f'{folder_loc}{folder}/de/', name ) )] )
    toc_a = timeit.default_timer ()
    log = f'{folder} had {to_process} to Anonymise, {processed} Anonymised in {round ( (toc_a - tic_a) / 60, 1 )} Minutes.'
    logging.info ( log )

def anonymisation_only(folder_loc,fulllist,completed_list):
    #print(fulllist)
    df = pd.read_csv(f'{folder_loc}{fulllist}')
    df_complete = pd.read_csv(f'{folder_loc}{completed_list}')
    #now remove the already completed from the list...
    df = (pd.merge(df_complete,df,on='StudyNumber',how='right', indicator=True).query('_merge == "right_only"').drop('_merge', 1))
    df.reset_index(drop=True, inplace=True)
    finish = len(df)
    #finish = 3
    logging.info(f'Number of participants to process: {finish}')
    i=0
    while i < finish == 1:
        #LastSnumber=''
        folder = df.StudyNumber[i]
        logging.info(f'doing folder:{folder}')
        #StudyInstanceUID = df.StudyInstanceUID[i]
        #print(StudyInstanceUID)
        tic = timeit.default_timer()
        execute_anonymisation(folder)
        #execute_re_anonymisation_acc_num(folder)
        clear_pid_version(folder)
        toc = timeit.default_timer()

        log = f'Participant {i+1}: Download and anonymisation time taken: {round((toc-tic)/60,1)} Minutes'# {subprocess.run(du ./dicoms/Folder -h,shell=True).stdout}' #want to add os. equivilent of subprocess.run(du ./dicoms/Folder -h,shell=True.stdout
        logging.info(log)
        with open(f'{folder_loc}{completed_list}',"a+") as f:
            f.write(f'{folder}\n')
        #print(f'folder:{i+1} folder name:{folder}')
        i = i+1


#folder_loc_Processing = f'C:\\Baddie\\AIMI\\'
#folder_loc_in = f'C:\\AIMI\\'
#folder_loc_out = f'V:\\Baddie\\AIMI\\'
#folder_loc_in = f"N:\\CT test scans\\"

#new locs
folder_loc_in = f"N:\\Baddie_2B_anonymised\\AIMI\\"
folder_loc_out = f"N:\\Baddie\\AIMI\\"
dictionary_loc = f'C:\\Baddie\\AIMI\\'

execute_anonymisation(folder_loc_in,folder_loc_out,dictionary_loc)


#anonymisation_only(f'{folder_loc}{folder}')
#clear_pid_version(folder)