import os
import pydicom
import numpy as np
from PIL import Image

# Define the input folder containing DICOM files
dicom_folder = r'C:\images'

# Loop through all files in the folder
for filename in os.listdir(dicom_folder):
    dicom_path = os.path.join(dicom_folder, filename)

    # Skip if it's not a file
    if not os.path.isfile(dicom_path):
        continue

    try:
        # Attempt to read the file as a DICOM
        ds = pydicom.dcmread(dicom_path)
        pixel_array = ds.pixel_array

        # Normalize pixel values to 0-255
        image_2d = pixel_array.astype(float)
        image_2d_scaled = np.uint8(255 * (image_2d - np.min(image_2d)) / (np.max(image_2d) - np.min(image_2d)))

        # Convert to PIL Image and save as JPG
        image = Image.fromarray(image_2d_scaled)
        jpg_filename = os.path.splitext(filename)[0] + '.jpg'
        jpg_path = os.path.join(dicom_folder, jpg_filename)
        image.save(jpg_path)
        print(f"Converted: {filename} -> {jpg_filename}")
    except Exception as e:
        print(f"Skipped {filename}: {e}")
