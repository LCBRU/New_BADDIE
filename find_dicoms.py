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
import glob
import time as mytime
from datetime import datetime, time
from dicomanonymizer import *
import logging
import sys
from alive_progress import alive_bar

#need to delete dicoms of ImageType: (0008,0008) DERIVED\SECONDARY\SCREEN SAVE

#mount drives if needed, details in mount_drives.py
#cmd_build = "sudo mount -o username='daniel.lawday',rw,file_mode=0777,dir_mode=0777  '//10.156.254.183/cardiac research archive/BRICC_CTCA/' '/media/CRA'"
#cmd_build = "sudo mount -o username='daniel.lawday',rw,file_mode=0777,dir_mode=0777  '//10.156.254.183/cardiac research archive2/BRICC_CTCA_2/' '/media/CRA2'"
#cmd_build = "sudo mount -o username='daniel.lawday' '//10.147.125.176/Data3/Imaging/Radiology/Imaging Research/IMAGING/AIMI/dicoms/' '/media/IMAGING'"
#cmd_build = "sudo mount -o username='daniel.lawday',rw,file_mode=0777,dir_mode=0777  '//10.161.54.146/Baddie/' '/media/my_pcBaddiefolder'"
#cmd_build = "sudo mount -o username='daniel.lawday' '//10.147.125.176/Data3/Imaging/Radiology/Imaging Research/IMAGING/SCAD/dicoms/' '/media/IMAGING'"

###############################################
#cmd_build = "sudo mount -o username='daniel.lawday',rw,file_mode=0777,dir_mode=0777  '//10.161.54.146/Baddie/' '/media/my_pcBaddiefolder'"
#subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr
###############################################

cmd_build = "sudo adduser danlawday sudo"
print(cmd_build)
subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr

#cmd_build = "sudo visudo"
#print(cmd_build)
#subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr

cmd_build = "sudo ALL=(ALL) NOPASSWD:ALL"
print(cmd_build)
subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr

#processing_folder_loc = '/./media/IMAGING/'
#processing_folder_loc = 'SCAD/dicoms/'
#Storagefolder = 'SCAD/dicoms/'


Storagefolder = 'AIMI/dicoms/'
processing_folder_loc = 'AIMI/'

def run_with_timeout(func, args=(), kwargs={}, timeout=10):
    result = [None]
    def target():
        result[0] = func(*args, **kwargs)
    
    thread = threading.Thread(target=target)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        return None  # or raise an exception, or return a default value
    else:
        return result[0]

logging.basicConfig(
    level=logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f'{processing_folder_loc}/progress.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.info(f'start time')

warnings.filterwarnings('ignore')
warnings.warn('DelftStack')
warnings.warn('Do not show this message')


#extract_find_results('DICOM_List.csv')
def extract_find_results(filename):
    #inputs
    tic_f = timeit.default_timer()
    #cmd_build = "touch DELME_TEMP"
    #subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr
    
    print(filename)
    df = pd.read_csv(filename)
    df = df.reset_index()  # make sure indexes pair with number of rows
    finish = int(len(df))
        
    scan_names_wanted = pd.read_csv('Scan_Names_wanted.csv')
    #will switch to below when it's working, couldn't get it going...
    #scan_names_wanted = pd.read_excel('Scan_Names_wanted.xlsx', engine='openpyxl', sheet_name='list')

    print(f'number of participants to process: {finish}')
    logging.info("number of participants to process: %f ", finish)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    #print(f'sudo password will be requested in a few minutes after the list to download is built')
    print(f'Time now:{dt_string}')
    
    i=0
    LastSnumber=''
    results = pd.DataFrame([], columns = ['StudyNumber','PatientID', 'AccessionNumber','StudyDate','StudyDescription','StudyInstanceUID'])    
    not_found_results = pd.DataFrame([], columns = ['StudyNumber','Snumber','period_start','period_end'])
    #print(results)

    with alive_bar(len(df)) as bar:
        for index, row in df.iterrows():
            StudyNumber = df.StudyNumber[i]
            Snumber = df.MRN[i]
            Desc = df.StudyDescriptionWanted[i]
            #dates need to be like this: 20200101-20200131
            period_start = df.DateOfWindowStart[i][0:10].replace('-','')
            #print(period_start)
            period_end = df.DateOfWindowEND[i][0:10].replace('-','')
            #print(period_end)
            
            p = Path('./rsp0001.xml')
            #print(p)
            #p.unlink(missing_ok=True) # remove if an XML is alreday there
            cmd_build = f'rm r*.xml'
            subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr # remove ALL the XMLs that are there
            #cmd_build =f'findscu -P -k 0008,0052=STUDY -aec UHLPACSWFM01 -aet XNAT01 10.194.105.10 104 -k 0010,0020={Snumber} -k 0020,000d -k "0008,1030={Desc}" -k "0008,0020={period_start}-{period_end}" -k 0008,0050 -Xx'
            #cmd_build =f'findscu -P -k 0008,0052=STUDY -aec UHLPACSWFM01 -aet XNAT01 10.194.105.10 104 -k 0010,0020={Snumber} -k 0020,000d -k "0008,1030={Desc}" -k "0008,0020={period_start}-{period_end}" -k 0008,0050 -Xx '
            cmd_build =f'findscu -P -k 0008,0052=STUDY -aec UHLPACSWFM01 -aet XNAT01 10.194.105.10 104 -k 0010,0020={Snumber} -k 0020,000d -k "0008,1030" -k "0008,0020={period_start}-{period_end}" -k 0008,0050 -Xx '
            #cmd_build =f'findscu -P -k 0008,0052=STUDY -aec UHLPACSWFM01 -aet XNAT01 10.194.105.10 104 -k 0010,0020={Snumber} -k 0020,000d -k "0008,1030" -k "0008,0020" -k "0010,0010" -k "0010,0030" -k 0008,0050 -Xx '
            #print(cmd_build)
            subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr
            
                    #subprocess.run('cat rsp0001.xml',shell=True)
            if os.path.isfile(p): 
                #print(f'rsp0001.xml found...')
                xmlfiles = glob.glob('*.xml')
                #print(xmlfiles)
                for xmlfilename in list(glob.glob('*.xml')):  
                    with open(xmlfilename, "r") as file:
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
                        #PatientName = bs_content.find("element",{"name":"PatientName"}).text
                        #PatientBirthDate = bs_content.find("element",{"name":"PatientBirthDate"}).text
                                            
                        #data = [[StudyNumber,PatientID,AccessionNumber,StudyDate,StudyDescription,StudyInstanceUID,PatientName,PatientBirthDate]]
                        data = [[StudyNumber,PatientID,AccessionNumber,StudyDate,StudyDescription,StudyInstanceUID]]
                        #print(data)
                        #result = pd.DataFrame(data, columns = ['StudyNumber','PatientID', 'AccessionNumber','StudyDate','StudyDescription','StudyInstanceUID','PatientName','PatientBirthDate'])
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
            bar()

        results.to_csv('TBD_results_raw_dump.csv',index=False)

        results_2 = results.merge(scan_names_wanted, how='left', left_on='StudyDescription', right_on='StudyDescription')
        found_but_not_wanted_thus_excluded_list = results_2[results_2['wanted_or_not'] != 'wanted']['StudyDescription'].unique()
        results_2 = results_2[results_2['wanted_or_not'] == 'wanted']
        results_2.to_csv('results.csv',index=False)
        not_found_results.to_csv('not_found_results.csv',index=False)

        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        toc_f = timeit.default_timer()
        log = f'Finished extract_find_results, time now is {dt_string}. Time taken: {round((toc_f-tic_f)/60,1)} Minutes'
        logging.info(log)

def extract_find_viadob_results(filename):
    #inputs
    tic_f = timeit.default_timer()
    #cmd_build = "touch DELME_TEMP"
    #subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr
    
    print(filename)
    df = pd.read_csv(filename)
    df = df.reset_index()  # make sure indexes pair with number of rows
    finish = int(len(df))
        
    scan_names_wanted = pd.read_csv('Scan_Names_wanted.csv')
    #will switch to below when it's working, couldn't get it going...
    #scan_names_wanted = pd.read_excel('Scan_Names_wanted.xlsx', engine='openpyxl', sheet_name='list')

    print(f'number of participants to process: {finish}')
    logging.info("number of participants to process: %f ", finish)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    #print(f'sudo password will be requested in a few minutes after the list to download is built')
    print(f'Time now:{dt_string}')
    
    i=0
    LastSnumber=''
    results = pd.DataFrame([], columns = ['StudyNumber','PatientID', 'AccessionNumber','StudyDate','StudyDescription','StudyInstanceUID'])    
    not_found_results = pd.DataFrame([], columns = ['StudyNumber'])
    #print(results)

    with alive_bar(len(df)) as bar:
        for index, row in df.iterrows():
            Redcap_Name = df.name
            StudyNumber = df.StudyNumber[i]
            dob = df.dob[i]
            #Desc = df.StudyDescriptionWanted[i]
            #dates need to be like this: 20200101-20200131
            #period_start = df.DateOfWindowStart[i][0:10].replace('-','')
            #print(period_start)
            #period_end = df.DateOfWindowEND[i][0:10].replace('-','')
            #print(period_end)
            
            p = Path('./rsp0001.xml')
            #print(p)
            #p.unlink(missing_ok=True) # remove if an XML is alreday there
            cmd_build = f'rm r*.xml'
            subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr # remove ALL the XMLs that are there
            #cmd_build =f'findscu -P -k 0008,0052=STUDY -aec UHLPACSWFM01 -aet XNAT01 10.194.105.10 104 -k 0010,0020={Snumber} -k 0020,000d -k "0008,1030={Desc}" -k "0008,0020={period_start}-{period_end}" -k 0008,0050 -Xx'
            #cmd_build =f'findscu -P -k 0008,0052=STUDY -aec UHLPACSWFM01 -aet XNAT01 10.194.105.10 104 -k 0010,0020={Snumber} -k 0020,000d -k "0008,1030={Desc}" -k "0008,0020={period_start}-{period_end}" -k 0008,0050 -Xx '
            #cmd_build =f'findscu -P -k 0008,0052=STUDY -aec UHLPACSWFM01 -aet XNAT01 10.194.105.10 104 -k 0010,0020={Snumber} -k 0020,000d -k "0008,1030" -k "0008,0020={period_start}-{period_end}" -k 0008,0050 -Xx '
            #cmd_build =f'findscu -P -k 0008,0052=STUDY -aec UHLPACSWFM01 -aet XNAT01 10.194.105.10 104 -k 0010,0020={Snumber} -k 0020,000d -k "0008,1030" -k "0008,0020" -k "0010,0010" -k "0010,0030" -k 0008,0050 -Xx '
            cmd_build =f'findscu -P -k 0008,0052=STUDY -aec UHLPACSWFM01 -aet XNAT01 10.194.105.10 104 -k 0010,0020 -k 0020,000d -k "0008,1030" -k "0008,0020" -k "0010,0010" -k "0010,0030={dob}" -k 0008,0050 -Xx '
            #print(cmd_build)
            subprocess.run(cmd_build,shell=True ,text=True ,capture_output=True).stderr
            
                    #subprocess.run('cat rsp0001.xml',shell=True)
            if os.path.isfile(p): 
                #print(f'rsp0001.xml found...')
                xmlfiles = glob.glob('*.xml')
                #print(xmlfiles)
                for xmlfilename in list(glob.glob('*.xml')):  
                    with open(xmlfilename, "r") as file:
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
                        PatientName = bs_content.find("element",{"name":"PatientName"}).text
                        PatientBirthDate = bs_content.find("element",{"name":"PatientBirthDate"}).text
                                            
                        data = [[StudyNumber,PatientID,AccessionNumber,StudyDate,StudyDescription,StudyInstanceUID,PatientName,Redcap_Name,PatientBirthDate]]
                        #print(data)
                        result = pd.DataFrame(data, columns = ['StudyNumber','PatientID', 'AccessionNumber','StudyDate','StudyDescription','StudyInstanceUID','PatientName','Redcap_Name','PatientBirthDate'])
                        results = results.append(result)
                        #print(results)
                        #print(result)
                        LastSnumber = PatientID
                i += 1
            else:
                #print('File not found!.....')
                data_nf = [[StudyNumber]]
                not_found = pd.DataFrame(data_nf, columns = ['StudyNumber'])
                not_found_results = not_found_results.append(not_found)
                i += 1
            bar()

        results.to_csv('TBD_results_raw_dump.csv',index=False)

        results_2 = results.merge(scan_names_wanted, how='left', left_on='StudyDescription', right_on='StudyDescription')
        found_but_not_wanted_thus_excluded_list = results_2[results_2['wanted_or_not'] != 'wanted']['StudyDescription'].unique()
        results_2 = results_2[results_2['wanted_or_not'] == 'wanted']
        results_2.to_csv('results.csv',index=False)
        not_found_results.to_csv('not_found_results.csv',index=False)

        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
        toc_f = timeit.default_timer()
        log = f'Finished extract_find_results, time now is {dt_string}. Time taken: {round((toc_f-tic_f)/60,1)} Minutes'
        logging.info(log)

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
    Storagefolder_folder = f'{Storagefolder}{folder}/'
    to_process = len([name for name in os.listdir(Storagefolder_folder) if os.path.isfile(os.path.join(Storagefolder_folder, name))])
    filenames = []
    f = 0
   
    #Create Psudonimised File and open for writing    
    mkcmd = f'mkdir {Storagefolder_folder}/de'
    subprocess.run(mkcmd,shell=True ,text=True ,capture_output=True).stderr
    filenames = []

    for entry in os.listdir(f'{Storagefolder_folder}'):
        filenames.append(entry)
        #print(entry)
    for filename in filenames:
        #print(filename)
        in_file = f'{Storagefolder_folder}/{filename}'
        file_out = f'{Storagefolder_folder}/de/{filename}'
        #di_cmd = f"dicom-anonymizer -t '(0x0010, 0x0020)' 'regexp' 'R.+' '{folder}' --dictionary New_BADDIE/dictionary.json {in_file} {file_out}" #--extra-rules extra_rules.json
        di_cmd = f"dicom-anonymizer -t '(0x0010, 0x0020)' 'regexp' 'R.+' --dictionary New_BADDIE/dictionary.json {in_file} {file_out}" #--extra-rules extra_rules.json
        #print(di_cmd)      
        ExecuteAnonymisation = subprocess.run([di_cmd],shell=True ,text=True ,capture_output=True).stdout
    processed = len([name for name in os.listdir(f'{Storagefolder_folder}/de/') if os.path.isfile(os.path.join(f'{Storagefolder_folder}/de/', name))])
    toc_a = timeit.default_timer()
    log = f'{folder} had {to_process} to Anonymise, {processed} Anonymised in {round((toc_a-tic_a)/60,1)} Minutes.'
    logging.info(log)
    

def execute_re_anonymisation_acc_num(folder):
    tic_a = timeit.default_timer()
    
    to_process = len([name for name in os.listdir(f'{processing_folder_loc}{folder}/de') if os.path.isfile(os.path.join(f'{processing_folder_loc}{folder}/de', name))])
    filenames = []
    f = 0
   
    #Create Psudonimised File and open for writing    
    #mkcmd = f'mkdir ./{processing_folder_loc}{folder}/de'
    #subprocess.run(mkcmd,shell=True ,text=True ,capture_output=True).stderr
    filenames = []

    for entry in os.listdir(f'{processing_folder_loc}{folder}/de'):
        filenames.append(entry)
        shutil.move(os.path.join(f'{processing_folder_loc}{folder}/de', entry), f'{processing_folder_loc}{folder}')
        #print(entry)
    for filename in filenames:
        #print(filename)
        in_file = f'{processing_folder_loc}{folder}/{filename}'
        file_out = f'{processing_folder_loc}{folder}/de/{filename}'
        di_cmd = f"dicom-anonymizer -t '(0x0010, 0x0020)' 'regexp' 'R.+' '{folder}' --dictionary dictionary.json {in_file} {file_out}" #--extra-rules extra_rules.json
        print(f'{filename}')      
        ExecuteAnonymisation = subprocess.run([di_cmd],shell=True ,text=True ,capture_output=True).stdout
    processed = len([name for name in os.listdir(f'{processing_folder_loc}{folder}/de/') if os.path.isfile(os.path.join(f'{processing_folder_loc}{folder}/de/', name))])
    toc_a = timeit.default_timer()
    log = f'{folder} had {to_process} to Anonymise, {processed} Anonymised in {round((toc_a-tic_a)/60,1)} Minutes.'
    logging.info(log)
  

def clear_pid_version(folder):
    for original_file in os.listdir(f'{processing_folder_loc}{folder}/'):
        if os.path.isfile(os.path.join(f'{processing_folder_loc}{folder}/', original_file)):
            os.remove(os.path.join(f'{processing_folder_loc}{folder}/', original_file))



def download_dicoms(filename):
    print(filename)
    df = pd.read_csv(filename)
    print(f'Found in input file: {len(df)}')
    df_complete = pd.read_csv('completed_list.csv')
    print(f'Aready completed so need to remove: {len(df_complete)}')
    #now remove the already completed from the list...
    print(df)
    df = pd.merge(df_complete,df,left_on='StudyNumber_complete',right_on='StudyNumber',how='right', indicator=True).query('_merge == "right_only"').drop('_merge', 1)
    df.drop('StudyNumber_complete', axis=1, inplace=True)
    df.reset_index(inplace = True, drop = True)
    #print(df)
    print(f'...removed, leaving the remaining: {len(df)}')
    finish = len(df)
    #finish = 1
    logging.info(f'number of participants to process:{finish}')    
    i=0
    #enoughDiskSpace(f'{Storagefolder}')
    while i < finish and enoughDiskSpace("/home/danlawday/") == 1:
        mytime.sleep(1)# was going too fast!
        LastSnumber=''
        folder = df.StudyNumber[i]
        StudyInstanceUID = df.StudyInstanceUID[i]
        print(folder)
        print(StudyInstanceUID)
        #mkcmd = f'mkdir ./{processing_folder_loc}{folder}'
        mkcmd = f'mkdir {Storagefolder}{folder}'
        #print(mkcmd)
        subprocess.run(mkcmd,shell=True ,text=True ,capture_output=True).stderr
        b1 = 'sudo movescu -v -aet XNAT01 -aem XNAT01 +P 104 -aec AE_ARCH_UHL01 10.194.105.78 104 -S -k QueryRetrieveLevel=STUDY -k StudyInstanceUID='
        #b1 = 'movescu -v -aet XNAT01 -aem XNAT01 +P 104 -aec AE_ARCH_UHL01 10.194.105.78 104 -S -k QueryRetrieveLevel=STUDY -k StudyInstanceUID='
        #StudyInstanceUID
        b2 = f'  -od {Storagefolder}'
        #Folder
        b3 = '/'
        cmd_build = f'{b1}{StudyInstanceUID}{b2}{folder}{b3}'
        #print()        
        tic = timeit.default_timer()
        # added a timeout of 470 (7 minutes) as somtimes it becomes unresponsive
        print("Try next participants download")
        try:
            print(cmd_build)
            process = subprocess.Popen(cmd_build, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            try:
                print("Downloading...")
                stdout, stderr = process.communicate(timeout=470)
                print("...Fin")
            except subprocess.TimeoutExpired:
                print("Terminate!")
                process.terminate()
                try:
                    process.wait(timeout=10)  # Wait for the process to terminate, with a timeout
                except subprocess.TimeoutExpired:
                    print("Kill!")
                    process.kill()  # Force kill if the process does not terminate
                    stdout, stderr = process.communicate()
                print("The movescu call took too long and was terminated.")
                toc = timeit.default_timer()
                log = f'{folder}|Participant {i+1}: Timed out after {round((toc-tic)/60,1)} Minutes'
            else:
                #print("Standard Output:", stdout)
                #print("Standard Error:", stderr)
                #print("Return Code:", process.returncode)
                toc = timeit.default_timer()
                log = f'{folder}|Participant {i+1}: Download time taken: {round((toc-tic)/60,1)} Minutes. This will now need anonymisation.'
        except Exception as e:
            print(f"An error occurred: {e}")
        #execute_anonymisation(folder)
        #clear_pid_version(folder)
        with open("completed_list.csv","a+") as f:
            f.write(f'{folder}\n')
        #log = f'{folder}|Participant {i+1}: Download and anonymisation time taken: {round((toc-tic)/60,1)} Minutes'# {subprocess.run(du ./dicoms/Folder -h,shell=True).stdout}' #want to add os. equivilent of subprocess.run(du ./dicoms/Folder -h,shell=True.stdout
        logging.info(log)
        
        i = i+1

def anonymisation_only(processing_folder_loc,fulllist,completed_list):
    #print(fulllist)
    df = pd.read_csv(f'{processing_folder_loc}{fulllist}')
    df_complete = pd.read_csv(f'{processing_folder_loc}{completed_list}')
    #now remove the already completed from the list...
    df = (pd.merge(df_complete,df,on='StudyNumber',how='right', indicator=True).query('_merge == "right_only"').drop('_merge', 1))
    df.reset_index(drop=True, inplace=True)
    finish = len(df)
    #finish = 3
    logging.info(f'Number of participants to process: {finish}')    
    i=0
    while i < finish and enoughDiskSpace("/home/danlawday/") == 1:
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
        with open(f'{processing_folder_loc}{completed_list}',"a+") as f:
            f.write(f'{folder}\n')
        #print(f'folder:{i+1} folder name:{folder}')
        i = i+1
    print('Finished')



#anonymisation_only('rerun.csv')

#extract_find_results('New_BADDIE/DICOM_List.csv') # to results.csv
#extract_find_results('SCAD_List.csv')
#extract_find_viadob_results('227_dobcheck.csv')

#download_dicoms('results.csv') # using results.csv to output folder
#download_dicoms('SCAD_need_checking.csv')

#processing_folder_loc = '/./media/CRA/'
#processing_folder_loc = 'SCAD/dicoms/'
#singlefolder = 'ScadReg00195'
#singlefolder = 'BPt00005471'
#folder = f'{singlefolder}'
#execute_anonymisation(folder)
#clear_pid_version(folder)

#extract_find_results_NoExtract('folderlist.csv')

#processing_folder_loc = '/./media/CRA2/'
#fulllist = 'folderlist.csv'
#completed_list = 'completed_list.csv'
#anonymisation_only(processing_folder_loc,fulllist,completed_list)


#extract_find_results('AIMI_DICOM_List_set2.csv')
download_dicoms('results.csv') 



