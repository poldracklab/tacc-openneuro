import pandas as pd
import gspread as gs
import glob
import os.path
import subprocess
import sys

"""Compares datasets on Status sheet with status "s3" to those listed on the
OpenNeuroDerivatives superdataset"""

gc = gs.oauth()
sh = gc.open_by_url(
'https://docs.google.com/spreadsheets/d/1pznoUWMFdgUELjj5P-h8kshuP8_aXCYsG15BimnhA38'
)
sheet = sh.worksheet('Status')
sheet_df = pd.DataFrame(sheet.get_all_records())
sheet_df = sheet_df.set_index('dataset_number')

fmriprep_s3 = set(sheet_df[sheet_df.fmriprep == 's3'].index)
mriqc_s3 = set(sheet_df[sheet_df.mriqc == 's3'].index)
fmriprep_nots3 = set(sheet_df[sheet_df.fmriprep != 's3'].index)
mriqc_nots3 = set(sheet_df[sheet_df.mriqc != 's3'].index)

OpenNeuroDerivatives_path = '/work2/03201/jbwexler/frontera/OpenNeuroDerivatives'
subprocess.run(f'datalad update -d {OpenNeuroDerivatives_path} --merge',shell=True)
dirs = glob.glob(os.path.join(OpenNeuroDerivatives_path, 'ds*'))
dirs_split = [os.path.basename(dir).split('-') for dir in dirs]
super_ds = pd.DataFrame(dirs_split,columns=['dataset','software'])
super_ds = super_ds.set_index('dataset')
fmriprep_super = set(super_ds[super_ds.software == 'fmriprep'].index)
mriqc_super = set(super_ds[super_ds.software == 'mriqc'].index)

fmriprep_col = sheet.find('fmriprep', in_row=1).col
mriqc_col = sheet.find('mriqc', in_row=1).col

def ask_user_fix():
    reply = input('Do you want to update the sheet to fix the discrepancy? (y/n) ')
    if reply == 'y':
        return True
    else:
        return False

def get_dataset_row(dataset):
    cell_list = sheet.findall(dataset, in_column=1)
    if len(cell_list) > 1:
        print(f"Error: {dataset} appeared multiple times in column A")
        sys.exit(1) 
    if not cell_list:
        print(f"Error: {dataset} was not found in column A")
        sys.exit(1)
    return cell_list[0].row

f_sheet_not_gh = fmriprep_s3 - fmriprep_super
if f_sheet_not_gh:
    print(f"fmriprep on sheet but not github: {','.join(f_sheet_not_gh)}")
    if ask_user_fix():
       for ds in f_sheet_not_gh:
           cell = sheet.cell(get_dataset_row(ds), fmriprep_col)
           if cell.value == 's3':
               sheet.update_acell(cell.address, 'corral')

m_sheet_not_gh = mriqc_s3 - mriqc_super
if m_sheet_not_gh:
    print(f"mriqc on sheet but not github: {','.join(m_sheet_not_gh)}")
    if ask_user_fix():
       for ds in m_sheet_not_gh:
           cell = sheet.cell(get_dataset_row(ds), mriqc_col)
           if cell.value == 's3':
               sheet.update_acell(cell.address, 'corral') 

f_gh_not_sheet = fmriprep_nots3.intersection(fmriprep_super)
if f_gh_not_sheet:
    print(f"fmriprep on github but not on sheet: {','.join(f_gh_not_sheet)}")
    if ask_user_fix():
        for ds in f_gh_not_sheet:
            cell = sheet.cell(get_dataset_row(ds), fmriprep_col)
            if cell.value == 'corral':
                sheet.update_acell(cell.address, 's3')

m_gh_not_sheet = mriqc_nots3.intersection(mriqc_super)
if m_gh_not_sheet:
    print(f"mriqc on github but not on sheet: {','.join(m_gh_not_sheet)}")
    if ask_user_fix():
       for ds in m_gh_not_sheet:
           cell = sheet.cell(get_dataset_row(ds), mriqc_col)
           if cell.value == 'corral':
               sheet.update_acell(cell.address, 's3')
