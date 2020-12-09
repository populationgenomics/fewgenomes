import json
import os
import subprocess
from typing import Dict, Tuple, List
import click
import pandas as pd
from google.cloud import storage
from ngs_utils.file_utils import safe_mkdir


# input paths:
xlsx_url = 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20130606_sample_info/20130606_sample_info.xlsx'
xlsx_fpath = f'data/{os.path.basename(xlsx_url)}'  # spreadsheet with 1kg metadata
warp_references_json_fpath = 'data/warp_references_inputs.json'
gs_data_base_url = 'gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/phase3/data'

# output paths:
samples_csv_fpath = 'workflow/samples.csv'
warp_inputs_dirpath = 'workflow/inputs'


def file_exists(path: str):
    if path.startswith('gs://'):
        bucket = path.replace('gs://', '').split('/')[0]
        path = path.replace('gs://', '').split('/', maxsplit=1)[1]
        gs = storage.Client()
        return gs.get_bucket(bucket).get_blob(path)
    return os.path.exists(path)


@click.command()
@click.option('-n', 'n', help='the number of samples to select', default=50)
def main(n: int):
    if not os.path.isfile(xlsx_fpath):
        os.system(f'wget {xlsx_url} -O {xlsx_fpath}')

    if not os.path.isfile(samples_csv_fpath):
        # First we are reading the list of samples with the available fastq data in the google genomics
        # public bucket. There are 2950 samples there, while the metadata in excel from the FTP has
        # got 3500 samples, so we need to overlap two lists first.
        cmd = f'gsutil ls "{gs_data_base_url}"'
        sample_dirs = subprocess.check_output(cmd, shell=True).decode().split('\n')
        sample_names_with_fastq = []
        for sd in sample_dirs:
            if file_exists(sd + 'sequence_read'):
                sample_names_with_fastq.append(sd.split('/')[-2])

        df = pd.read_excel(xlsx_fpath, sheet_name="Sample Info")
        df = df[['Sample', 'Gender', 'Population', 'Family ID']]
        df = df[df['Sample'].isin(sample_names_with_fastq)]
        df = pd.concat([
            df[df['Sample'] == 'NA12878'],
            df[df['Sample'] == 'NA12891'],
            df[df['Sample'] == 'NA12892'],
            df.sample(n - 3, random_state=1)
        ])
        df.to_csv(samples_csv_fpath)

    if not os.path.isdir(warp_inputs_dirpath):
        safe_mkdir(warp_inputs_dirpath)
        with open(warp_references_json_fpath) as fh:
            refs_data = json.load(fh)

        df = pd.read_csv(samples_csv_fpath)

        for i, row in df.iterrows():
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
