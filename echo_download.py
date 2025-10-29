import os
from pydicom.dataset import Dataset
from pynetdicom import AE, evt, AllStoragePresentationContexts
from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelMove

# Define output folder
output_folder = r'V:\1. IT projects\Example_echo'
os.makedirs(output_folder, exist_ok=True)

# Handler to save incoming DICOM files
def handle_store(event):
    ds = event.dataset
    ds.file_meta = event.file_meta
    filename = os.path.join(output_folder, f"{ds.SOPInstanceUID}.dcm")
    ds.save_as(filename, write_like_original=False)
    return 0x0000

# Set up event handlers
handlers = [(evt.EVT_C_STORE, handle_store)]

# Set up AE
ae = AE()
ae.supported_contexts = AllStoragePresentationContexts
ae.add_requested_context(StudyRootQueryRetrieveInformationModelMove)

# Replace with actual PACS AE Title and port
assoc = ae.associate('UHLECP01', 104, ae_title='REMOTE_AET', evt_handlers=handlers)

if assoc.is_established:
    ds = Dataset()
    ds.QueryRetrieveLevel = 'STUDY'
    ds.StudyInstanceUID = '1.2.840.113619.2.391.88025.1751632731.5300.291'  # Replace with actual UID

    responses = assoc.send_c_move(ds, 'YOUR_LOCAL_AET', StudyRootQueryRetrieveInformationModelMove)
    for (status, identifier) in responses:
        print('C-MOVE response:', status)

    assoc.release()
else:
    print("Association failed")
