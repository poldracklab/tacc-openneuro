import nibabel as nb
from nilearn import plotting as nlp
import os
import os.path
import glob
import shutil
import subprocess
from fpdf import FPDF
import argparse
import pandas as pd
from datetime import datetime, timedelta
import datalad.api as dl
import requests
import sys

# first argument is comma-separated list of datasets to iterate through

RAW_PATH = "/corral-repl/utexas/poldracklab/data/OpenNeuro/raw/"
MOSAICS_PATH = "/corral-repl/utexas/poldracklab/data/OpenNeuro/mosaics/"
CSV_URL = "https://raw.githubusercontent.com/jbwexler/openneuro_metadata/main/metadata.csv"
DATE_FORMAT = '%Y-%m-%d'

def make_mosaic(ds, ds_path, out_path):
    tmp_dir = "tmp_" + ds
    if not os.path.exists(tmp_dir):
        os.mkdir(tmp_dir)

    for file in glob.glob(os.path.join(ds_path,"sub-*/**/anat/*.nii*"), recursive=True):
        out_file = os.path.basename(file).split('.')[0] + ".png"
        if os.path.exists(os.path.join(tmp_dir,out_file)):
            print("Skipping " + out_file)
            continue
        try:
            t1w = nb.load(file)
            fig = nlp.plot_anat(t1w,dim=-1)
            print(out_file)
            fig.savefig(os.path.join(tmp_dir,out_file))
            fig.close()
        except:
            print(file + " not found")
    
    pdf = FPDF()
    pdf.set_auto_page_break(0)
    pdf.add_page()
    pdf.set_font('Arial', 'B', 5)
    x=0
    y=0
    x_size=66
    y_size=26
    count=0

    for img in sorted(glob.glob(tmp_dir + '/*')):
        pdf.image(img,x=x,y=y,w=x_size,h=y_size)
        pdf.set_xy(x,y+y_size/2+1)
        pdf.cell(x_size,y_size,os.path.basename(img))
    
        x+=x_size+2
        if x > 3*x_size:
            x=0
            y+=y_size+2
        count+=1
        if count >= 30:
            pdf.add_page()
            count = 0
            x=0
            y=0
    
    pdf.output(os.path.join(MOSAICS_PATH, 'mosaics', ds + "_mosaic.pdf"))
    shutil.rmtree(tmp_dir)
    
def install_datasets(ds_list):
    ds_list_dl = ds_list
    for ds in ds_list:
        ds_source = "https://github.com/OpenNeuroDatasets/%s.git" % ds
        ds_path = os.path.join(RAW_PATH, ds)
        today_str = datetime.today().strftime(DATE_FORMAT)
        failed_install_list = []
        failed_get_list = []
        
        if requests.get(ds_source).status_code == 404:
            failed_install_list.append(ds)
            ds_list_dl.remove(ds)
            continue  
        elif os.path.isdir(ds_path):
            dl.update(dataset=ds_path, sibling="origin")
        else:
            try:
                dl.install(path=ds_path, source=ds_source)
            except:
                failed_install_list.append(ds)
                ds_list_dl.remove(ds)
                continue
                                
        try:
            for nii_path in glob.glob(os.path.join(ds_path,"sub-*/**/anat/*.nii*"), recursive=True):
                dl.get(nii_path, dataset=ds_path)
        except:
            failed_get_list.append(ds)
            ds_list_dl.remove(ds)

    if failed_install_list:
        failed_install_log = os.path.join(MOSAICS_PATH, "logs", "download_get", "failed_install_" + today_str)
        with open(failed_install_log, "w") as outfile:
            outfile.write("\n".join(str(i) for i in failed_install_list))
    if failed_get_list:
        failed_get_log = os.path.join(MOSAICS_PATH, "logs", "download_get", "failed_get_" + today_str)
        with open(failed_get_log, "w") as outfile:
            outfile.write("\n".join(str(i) for i in failed_get_list))
    return ds_list_dl

def ds_list_from_csv(since_date):
    df = pd.read_csv(CSV_URL)
    df.most_recent_snapshot = pd.to_datetime(df.most_recent_snapshot, format=DATE_FORMAT)
    since_df = df.loc[df.most_recent_snapshot >= since_date]
    ds_list = since_df.accession_number.to_list()
    return ds_list

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dataset-list', type=str)
    parser.add_argument('-f', '--dataset-list-file', type=str)
    parser.add_argument('-r', '--recent-snapshots', type=int, help="Includes datasets modified/published in the past provided # of days.")
    parser.add_argument('-s', '--snapshots-since', type=str, help="Includes datasets since provided date (YYYY-MM-DD).")
    parser.add_argument('-l', '--local', action='store_true')
    parser.add_argument('-j', '--job', action='store_true')
    parser.add_argument('-x', '--skip-download', action='store_true')   
    args = parser.parse_args()
    today_str = datetime.today().strftime(DATE_FORMAT)
        
    if args.dataset_list is not None:
        ds_list = args.dataset_list.split(',')
    elif args.dataset_list_file is not None:
        with open(args.dataset_list_file) as file:
            ds_list = file.read().splitlines()
    elif args.recent_snapshots is not None:
        since_date = datetime.today() - timedelta(days=args.recent_snapshots)
        ds_list = ds_list_from_csv(since_date)
    elif args.snapshots_since is not None:
        since_date = datetime.strptime(args.snapshots_since, DATE_FORMAT)
        ds_list = ds_list_from_csv(since_date)
    
    if not ds_list:
        print("No datasets found")
        sys.exit(0)
    
    if not args.skip_download:
        ds_list = install_datasets(ds_list)

    if args.local:
        for ds in ds_list:
            ds_path = os.path.join(RAW_PATH, ds)
            out_path = os.path.join(MOSAICS_PATH, "mosaics", ds)
            make_mosaic(ds, ds_path, out_path)
    elif args.job:
        file_path = os.path.realpath(__file__)
        file_dir = os.path.dirname(file_path)
        launcher_list = []
        for ds in ds_list:
            line = "python %s -ld %s" % (file_path, ds)
            launcher_list.append(line)
        with open(os.path.join(MOSAICS_PATH, "mosaics_launcher"), "w") as outfile:
            print(*launcher_list, sep="\n", file=outfile)
        job_name = "mosaics_" + today_str
        slurm_path = os.path.join(file_dir, "mosaics.slurm")
        command = "sbatch -J %s %s" % (job_name, slurm_path)
        subprocess.run(command, shell=True)
        
    with open(os.path.join(MOSAICS_PATH, "logs", "ds_list", "ds_list_" + today_str), "w") as outfile:
        print(*ds_list, sep="\n", file=outfile)
    
    
if __name__ == "__main__":
    main()
