import os
import shutil

def MoveFolderIfFailed(folder, outputfolder):
    # Check if the folder exists
    if not os.path.exists(folder):
        print(f"Error: The folder '{folder}' does not exist.")
        return

    # Check if the output folder exists
    if not os.path.exists(outputfolder):
        print(f"Error: The output folder '{outputfolder}' does not exist.")
        return

    # Iterate through each subfolder in the folder
    for subfolder in os.listdir(folder):
        subfolder_path = os.path.join(folder, subfolder)

        # Check if it is a directory
        if os.path.isdir(subfolder_path):
            # Count the number of files in the subfolder
            num_files = len([
                f for f in os.listdir(subfolder_path)
                if os.path.isfile(os.path.join(subfolder_path, f))
            ])
            print(f'{subfolder_path} has {num_files} files')

            # If the subfolder contains fewer than 2 files, move it
            if num_files < 2:
                shutil.move(subfolder_path, os.path.join(outputfolder, subfolder))
                print(f"Moved '{subfolder_path}' to '{outputfolder}'")


MoveFolderIfFailed(r'V:\Baddie_2B_anonymised\AIMI',r'V:\Baddie_2B_anonymised\failed')
