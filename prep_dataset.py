import json
import os
import subprocess
from os.path import dirname
from typing import Dict, Tuple, List
import progressbar
import click
import pandas as pd
from ngs_utils.file_utils import safe_mkdir


# input paths:
xlsx_url = 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20130606_sample_info/20130606_sample_info.xlsx'
xlsx_fpath = f'data/{os.path.basename(xlsx_url)}'  # spreadsheet with 1kg metadata
warp_references_json_fpath = 'data/warp_references_inputs.json'
gs_data_base_url = 'gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/phase3/data'

# output paths:
samples_with_fastq_csv_fpath = 'workflow/intermediate/samples_with_fastq.csv'
samples_with_fastq_selected_csv_fpath = 'workflow/intermediate/samples_with_fastq_selected.csv'
warp_inputs_dirpath = 'workflow/inputs'

# Including two trios for testing the relatedness checks
DEFAULT_INCLUDE = [
    'NA12878', 'NA12891', 'NA12892',
    'NA19238', 'NA19239', 'NA19240',
]


@click.command()
@click.option('-n', 'n', help='the number of samples to select', default=50)
def main(n: int):
    if not os.path.isfile(xlsx_fpath):
        os.system(f'wget {xlsx_url} -O {xlsx_fpath}')

    if not os.path.isfile(samples_with_fastq_csv_fpath):
        safe_mkdir(dirname(samples_with_fastq_csv_fpath))
        print('Reading the list of samples with the available fastq data in the google genomics'
            ' public bucket. There are 2950 samples there, while the metadata in excel from the FTP has'
            ' got 3500 samples, so we need to overlap two lists first.')
        print('Reading the list of samples...')
        cmd = f'gsutil ls "{gs_data_base_url}"'
        sample_dirs = subprocess.check_output(cmd, shell=True).decode().split('\n')
        sample_names_with_fastq = []
        print('Checking which samples have the FASTQ data...')
        for i in progressbar.progressbar(range(len(sample_dirs))):
            sd = sample_dirs[i]
            cmd = f'gsutil ls {sd}'
            subdirs = subprocess.check_output(cmd, shell=True).decode().split('\n')
            if any(subdir.endswith('sequence_read/') for subdir in subdirs):
                sample_names_with_fastq.append(sample_dirs[i].split('/')[-2])

        df = pd.read_excel(xlsx_fpath, sheet_name="Sample Info")
        df = df[['Sample', 'Gender', 'Population', 'Family ID']]
        df = df[df['Sample'].isin(sample_names_with_fastq)]
        df.to_csv(samples_with_fastq_csv_fpath)

    if not os.path.isfile(samples_with_fastq_selected_csv_fpath):
        print(f'Selecting {n} samples...')
        safe_mkdir(dirname(samples_with_fastq_selected_csv_fpath))
        df = pd.read_csv(samples_with_fastq_csv_fpath)
        ceu_family_cond = df['Sample'].isin(DEFAULT_INCLUDE)
        df = pd.concat([
            df[ceu_family_cond],
            df[~ceu_family_cond].sample(n - 3, random_state=1)
        ])
        df.to_csv(samples_with_fastq_selected_csv_fpath)

    if not os.path.isdir(warp_inputs_dirpath):
        print(f'Finding FASTQs and generating WARP input files...')
        safe_mkdir(warp_inputs_dirpath)
        with open(warp_references_json_fpath) as fh:
            refs_data = json.load(fh)

        df = pd.read_csv(samples_with_fastq_selected_csv_fpath)

        for (_, row), _ in zip(df.iterrows(), progressbar.progressbar(range(len(df)))):
            sample = row['Sample']

            print(f'Finding fastqs for {sample}')
            cmd = f'gsutil ls "{gs_data_base_url}/{sample}/sequence_read/*_*.filt.fastq.gz"'
            fastq_fpaths = subprocess.check_output(cmd, shell=True).decode().split('\n')
            r1_fpaths = sorted([fp for fp in fastq_fpaths if fp.endswith('1.filt.fastq.gz')])
            r2_fpaths = sorted([fp for fp in fastq_fpaths if fp.endswith('2.filt.fastq.gz')])
            fastq_pairs = list(zip(r1_fpaths, r2_fpaths))

            data = dict()
            data['WGSFromFastq.sample_and_fastqs'] = dict(
                sample_name=sample,
                base_file_name=sample,
                final_gvcf_base_name=sample,
                fastqs=fastq_pairs,
            )
            data.update(refs_data)
            json_fpath = os.path.join(warp_inputs_dirpath, f'{sample}.json')
            with open(json_fpath, 'w') as out:
                json.dump(data, out, indent=4)


if __name__ == '__main__':
    main()
