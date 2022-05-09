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

warnings.filterwarnings('ignore')
warnings.warn('DelftStack')
warnings.warn('Do not show this message')

#extract_find_results('DICOM_List.csv')
def extract_find_results(filename):
    #inputs
    print(filename)
    df = pd.read_csv(filename)
    df = df.reset_index()  # make sure indexes pair with number of rows
    finish = len(df)
    #testing
    print("number of participants to process: ", finish)
    
    i=0
    LastSnumber=''
    results = pd.DataFrame([], columns = ['StudyNumber','PatientID', 'AccessionNumber','StudyDate','StudyDescription','StudyInstanceUID'])        
    print(results)

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
            print(results)
            #print(result)
            LastSnumber = PatientID
            i += 1
    results.to_csv('results.csv',index=False)

       
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
        toc = timeit.default_timer()
        log = f'{i+1} {StudyInstanceUID} time taken: {toc-tic}'# {subprocess.run(du ./dicoms/Folder -h,shell=True).stdout}' #want to add os. equivilent of subprocess.run(du ./dicoms/Folder -h,shell=True.stdout
        print(log)
        i = i+1

#extract_find_results('DICOM_List.csv') # to results.csv

download_dicoms('results.csv') # using results.csv to output folder

