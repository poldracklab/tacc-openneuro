import pandas as pd
import gspread as gs
import glob
import os.path
import subprocess

# Compares datasets on Status sheet with status "s3" to those listed on the OpenNeuroDerivatives superdataset

service_account_json = '/home1/03201/jbwexler/thermal-shuttle-364720-d11c29ce61ef.json'
gc = gs.service_account(filename=service_account_json)
sh = gc.open_by_url('https://docs.google.com/spreadsheets/d/1pznoUWMFdgUELjj5P-h8kshuP8_aXCYsG15BimnhA38')
sheet = pd.DataFrame(sh.worksheet('Status').get_all_records())
sheet.set_index('dataset_number',inplace=True)

fmriprep_s3 = set(sheet[sheet.fmriprep == 's3'].index)
mriqc_s3 = set(sheet[sheet.mriqc == 's3'].index)
fmriprep_nots3 = set(sheet[sheet.fmriprep != 's3'].index)
mriqc_nots3 = set(sheet[sheet.mriqc != 's3'].index)

OpenNeuroDerivatives_path = '/work2/03201/jbwexler/frontera/OpenNeuroDerivatives'
subprocess.run(f'datalad update -d {OpenNeuroDerivatives_path} --merge',shell=True)
dirs = glob.glob(os.path.join(OpenNeuroDerivatives_path, 'ds*'))
dirs_split = [os.path.basename(dir).split('-') for dir in dirs]
super_ds = pd.DataFrame(dirs_split,columns=['dataset','software'])
super_ds.set_index('dataset',inplace=True)
fmriprep_super = set(super_ds[super_ds.software == 'fmriprep'].index)
mriqc_super = set(super_ds[super_ds.software == 'mriqc'].index)

print('fmriprep on sheet but not github: ',','.join(fmriprep_s3 - fmriprep_super))
print('mriqc on sheet but not github: ',','.join(mriqc_s3 - mriqc_super))
print('fmriprep on github but not on sheet: ',','.join(fmriprep_nots3.intersection(fmriprep_super)))
print('mriqc on github but not on sheet: ',','.join(mriqc_nots3.intersection(mriqc_super)))
