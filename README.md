# New_BADDIE
New version of Baddie search, download and anonomise
How to use:
Create a file named ‘DICOM_List.csv’ for the program to use as it’s search criteria, formatted as below with two example rows:

| UhlSystemNumber | MRN | StudyNumber | DateOfSymptoms | ct_date_time_start | DateOfWindowStart | DateOfWindowEND | StudyDescriptionWanted |
| --- | --- | --- | --- | --- | --- | --- | --- |
| S12345678 | RWES12345678 | 3DS000001 | 2019-07-21 00:00:00.000 | 2019-07-21 00:00:00.000 | 2019-07-21 00:00:00.000 | 2019-07-28 00:00:00.000 | MRI Head |
| S12345679 | RWES12345679 | 3DS000002 | 2015-07-06 00:00:00.000 | 2015-07-06 00:00:00.000 | 2015-07-06 00:00:00.000 | 2015-07-13 00:00:00.000 | MRI Head |

Notes: the date fields are formatted as datetime, these will be read in and reformatted as needed.
The StudyDescriptionWanted will be used to filter to only the type of scan you are searching for.

The program will then create two files: results.csv and not_found_results.csv, the latter is not used further and is for your information only, if the number not being found is too high consider loosening the search criteria.
The next of the program is to iterate though the results.csv file and perform the following tasks for each found set of dicoms:
1.	Create folder for dicoms.
2.	Download of the files into folder.
3.	Create de subfolder
4.	Anonymiser files into de folder
5.	Delete original (none Anonymised) files
6.	Track the completion and log progress.
If the program finishes prior to end of results list the program can be restarted and continue where it left off, reprocessing the dicom it was part way though.

List of files and what they do
| find_dicoms.py | The program |
| DICOM_List.csv | The input file |
| results.csv | As discussed above |
not_found_results.csv | As discussed above |
rsp0001.xml / rsp0002.xml | This is the response from the find command, it’s processed and data extracted into the results.csv |
completed_list.csv | Used to track what’s been completed and remove from the list of dicoms to download, enables picking up from where it stopped on large requests |
dictionary.json  | List all the dicom tags which you want to keep |
Examle_xml.txt | An anonymised version of the a rsp0001.xml for reference (not use by the program) |
progress.log | A progress log, shows timing which can be helpful for predicting how long the request will take |
progress_re.log | Log for anonymization of already downloaded files, which is work in progress |
