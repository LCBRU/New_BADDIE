import csv
import subprocess
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
from itertools import islice
from pathlib import Path
import timeit
import os
import time as mytime
from datetime import datetime, time
from dicomanonymizer import *

warnings.filterwarnings('ignore')
warnings.warn('DelftStack')
warnings.warn('Do not show this message')

#extract_find_results('DICOM_List.csv')
def extract_find_results(filename):
    #inputs
    tic_f = timeit.default_timer()
    print(filename)
    df = pd.read_csv(filename)
    df = df.reset_index()  # make sure indexes pair with number of rows
    finish = len(df)
    #testing
    print("number of participants to process: ", finish)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    print(f'sudo password will be requested in a few minutes after the list to download is built')
    print(f'Time now:{dt_string}')
    
    i=0
    LastSnumber=''
    results = pd.DataFrame([], columns = ['StudyNumber','PatientID', 'AccessionNumber','StudyDate','StudyDescription','StudyInstanceUID'])    
    not_found_results = pd.DataFrame([], columns = ['StudyNumber','Snumber','period_start','period_end'])
    #print(results)

    for index, row in df.iterrows():
        StudyNumber = df.StudyNumber[i]
        Snumber = df.MRN[i]
        Desc = df.StudyDescriptionWanted[i]
        #dates need to be like this: 20200101-20200131
        period_start = df.DateOfWindowStart[i][0:10].replace('-','')
        #print(period_start)
        period_end = df.DateOfWindowEND[i][0:10].replace('-','')

        p = Path('./rsp0001.xml')
        #print(p)
        p.unlink(missing_ok=True) # remove if an XML is alreday there
        cmd_build =f'findscu -P -k 0008,0052=STUDY -aec UHLPACSWFM01 -aet XNAT01 10.194.105.10 104 -k 0010,0020={Snumber} -k 0020,000d -k "0008,1030={Desc}" -k "0008,0020={period_start}-{period_end}" -k 0008,0050 -Xx'
        #print(cmd_build)

        subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr
        #subprocess.run('cat rsp0001.xml',shell=True)
        if os.path.isfile(p): 
            #print(f'rsp0001.xml found...')
            with open("rsp0001.xml", "r") as file:
                # Read each line in the file, readlines() returns a list of lines
                content = file.read()
                #try file.read() rather than file.readlines() then the below may not be needed
                #Combine the lines in the list into a string
                #content = "".join(content)
                
                bs_content = bs(content, 'html.parser')

                PatientID = bs_content.find("element",{"name":"PatientID"}).text
                AccessionNumber = bs_content.find("element",{"name":"AccessionNumber"}).text
                StudyDate = bs_content.find("element",{"name":"StudyDate"}).text
                StudyDescription = bs_content.find("element",{"name":"StudyDescription"}).text
                StudyInstanceUID = bs_content.find("element",{"name":"StudyInstanceUID"}).text
                
                data = [[StudyNumber,PatientID,AccessionNumber,StudyDate,StudyDescription,StudyInstanceUID]]
                #print(data)
                result = pd.DataFrame(data, columns = ['StudyNumber','PatientID', 'AccessionNumber','StudyDate','StudyDescription','StudyInstanceUID'])
                results = results.append(result)
                #print(results)
                #print(result)
                LastSnumber = PatientID
                i += 1
        else:
            #print('File not found!.....')
            data_nf = [[StudyNumber,Snumber,period_start,period_end]]
            not_found = pd.DataFrame(data_nf, columns = ['StudyNumber','Snumber','period_start','period_end'])
            not_found_results = not_found_results.append(not_found)
            i += 1
    results.to_csv('results.csv',index=False)
    not_found_results.to_csv('not_found_results.csv',index=False)

    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    toc_f = timeit.default_timer()
    print(f'Finished, time now is {dt_string}. Time taken: {round((toc_f-tic_f)/60,1)} Minutes')
       
def du(path):
    """disk usage in human readable format (e.g. '2,1GB')"""
    return subprocess.check_output(['du','-sh', path]).split()[0].decode('utf-8')

def enoughDiskSpace(path):
    # if there is not enough disk space stop it!
    total = psutil.disk_usage(path)

    freeSpace = total.free / 1024 / 1024 / 1024 # GB
    if freeSpace > .5: # 500MB (they are about 300MB each)
        return 1
    else:
        print('Remaining disk space = {} GB'.format(freeSpace))
        return 0

def execute_anonymisation(folder):
    tic_a = timeit.default_timer()
    to_process = len([name for name in os.listdir(f'dicoms/{folder}/') if os.path.isfile(os.path.join(f'dicoms/{folder}/', name))])
    filenames = []
    f = 0
   
    #Create Psudonimised File and open for writing
    log_file = open(f'dicoms/Batch_log.csv', 'w')
    mkcmd = f'mkdir ./dicoms/{folder}/de'
    subprocess.run(mkcmd,shell=True ,text=True ,capture_output=True).stderr
    filenames = []

    for entry in os.listdir(f'dicoms/{folder}'):
        filenames.append(entry)
        #print(entry)
    for filename in filenames:
        #print(filename)
        in_file = f'dicoms/{folder}/{filename}'
        file_out = f'dicoms/{folder}/de/{filename}'
        di_cmd = f"dicom-anonymizer -t '(0x0010, 0x0020)' 'regexp' 'R.+' '{folder}' --dictionary dictionary.json {in_file} {file_out}" #--extra-rules extra_rules.json
        #print(di_cmd)      
        ExecuteAnonymisation = subprocess.run([di_cmd],shell=True ,text=True ,capture_output=True).stdout
    processed = len([name for name in os.listdir(f'dicoms/{folder}/de/') if os.path.isfile(os.path.join(f'dicoms/{folder}/de/', name))])
    toc_a = timeit.default_timer()
    log = f'{folder} had {to_process} to Anonymise, {processed} Anonymised in {round((toc_a-tic_a)/60,1)} Minutes.'
    #print(log)
    log_file.write(f'{log} \n')

def clear_pid_version(folder):
    for original_file in os.listdir(f'dicoms/{folder}/'):
        if os.path.isfile(os.path.join(f'dicoms/{folder}/', original_file)):
            os.remove(os.path.join(f'dicoms/{folder}/', original_file))

def download_dicoms(filename):
    print(filename)
    df = pd.read_csv(filename)
    finish = len(df)
    print("number of participants to process: ", finish)    
    i=0
    while i < finish and enoughDiskSpace("/home/danlawday/") == 1:
        LastSnumber=''
        folder = df.StudyNumber[i]
        StudyInstanceUID = df.StudyInstanceUID[i]
        #print(folder)
        #print(StudyInstanceUID)
        mkcmd = f'mkdir ./dicoms/{folder}'
        subprocess.run(mkcmd,shell=True ,text=True ,capture_output=True).stderr
        b1 = 'sudo movescu -v -aet XNAT01 -aem XNAT01 +P 104 -aec AE_ARCH_UHL01 10.194.105.78 104 -S -k QueryRetrieveLevel=STUDY -k StudyInstanceUID='
        #StudyInstanceUID
        b2 = '  -od /home/danlawday/New_BADDIE/dicoms/'
        #Folder
        b3 = '/'
        cmd_build = b1 + StudyInstanceUID + b2 + folder +b3
        #print()
        #print(cmd_build)        
        tic = timeit.default_timer()
        subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr
        execute_anonymisation(folder)
        clear_pid_version(folder)
        toc = timeit.default_timer()
        
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")

        log = f'Participant {i+1}:{dt_string}: Download and anonymisation time taken: {round((toc-tic)/60,1)} Minutes'# {subprocess.run(du ./dicoms/Folder -h,shell=True).stdout}' #want to add os. equivilent of subprocess.run(du ./dicoms/Folder -h,shell=True.stdout
        print(log)
        i = i+1

extract_find_results('DICOM_List.csv') # to results.csv
download_dicoms('results.csv') # using results.csv to output folder

#singlefolder = '3DS000734'
#folder = f'{singlefolder}'
#execute_anonymisation(folder)
#clear_pid_version(folder)
