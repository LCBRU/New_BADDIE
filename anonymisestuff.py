import csv
import subprocess
import threading
import bs4
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
import time as mytime
from datetime import datetime, time
from dicomanonymizer import *
#import logging
import sys


folder_loc_in = 'C:\\Baddie\\AIMI\\'
folder_loc_out = 'V:\\1. IT projects\\AIMI\\'
#folder = f'aimi0012370'


#folder_loc_folder = f'{folder_loc}{folder}\\'
#list = os.listdir(f'{folder_loc_folder}')
#mkcmd = f'mkdir {folder_loc}{folder}\\de'
#subprocess.run ( mkcmd, shell=True, text=True, capture_output=True ).stderr
#print(os.listdir ( folder_loc ))


def execute_anonymisation(folder_loc_in,folder_loc_out):
    print({folder_loc_in})
    print({folder_loc_out})
    tic_a = timeit.default_timer()
    folder_loc_folder = f'{folder_loc}{folder}\\'
    to_process = len ( [name for name in os.listdir ( folder_loc_folder ) if
                        os.path.isfile ( os.path.join ( folder_loc_folder, name ) )] )
    filenames = []
    f = 0

    # Create Psudonimised File and open for writing
    mkcmd = f'mkdir {folder_loc}{folder}\\de'
    subprocess.run ( mkcmd, shell=True, text=True, capture_output=True ).stderr
    filenames = []

    for entry in os.listdir ( f'{folder_loc}{folder}' ):
        filenames.append ( entry )
        # print(entry)
    for filename in filenames:
        #print(filename)
        in_file = f'{folder_loc}{folder}\\{filename}'
        file_out = f'{folder_loc}{folder}\\de\\{filename}'
        #regexp' 'R.+' '{folder} replaced with a simple empty
        di_cmd = f"dicom-anonymizer -t '(\"0x0010, 0x0020\")' 'regexp' 'R.+' '{folder}' --dictionary {folder_loc}dictionary.json {in_file} {file_out}"  # --extra-rules extra_rules.json
        # print(di_cmd)
        #ExecuteAnonymisation = \
        try:
            process_output = subprocess.run([di_cmd], shell=True, text=True, capture_output=True).stdout
            print(di_cmd)
            print(process_output)
        except subprocess.CalledProcessError as e:
            print(f"Error executing command: {e}")

    processed = len([name for name in os.listdir ( f'{folder_loc}{folder}\\de\\') if
                       os.path.isfile(os.path.join(f'{folder_loc}{folder}\\de\\', name))])
    toc_a = timeit.default_timer ()
    log = f'{folder} had {to_process} to Anonymise, {processed} Anonymised in {round ( (toc_a - tic_a) / 60, 1 )} Minutes.'
    print(log)
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


execute_anonymisation('aimi0019515')
#anonymisation_only(f'{folder_loc}{folder}')
#clear_pid_version(folder)