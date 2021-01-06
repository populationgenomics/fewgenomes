import json
import os
import subprocess
from os.path import dirname
from typing import Dict, Tuple, List
import progressbar
import pandas as pd


# spreadsheet with 1kg metadata
XLSX_URL = 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20130606_sample_info/20130606_sample_info.xlsx'
PED_URL = 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20121016_updated_pedigree/G1K_samples_20111130.ped'
GS_1GK_DATA_BASE_URL = 'gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/phase3/data'
WARP_REF_INPUT_JSON = 'resources/warp_references_inputs.json'

# Including two trios for testing the relatedness checks
DEFAULT_INCLUDE = [
    'NA12878', 'NA12891', 'NA12892',
    'NA19238', 'NA19239', 'NA19240',
]

WARP_INPUTS_DIR = 'warp_inputs/single_sample_jsons/'

n = config.get('n', 50)  # the number of samples to select
assert n >= len(DEFAULT_INCLUDE)


rule all:
    input:
        expand(os.path.join(WARP_INPUTS_DIR, '{sample}.json'), sample=DEFAULT_INCLUDE)


rule get_ped:
    output:
        ped = 'resources/G1K_samples.ped'
    params:
        url = PED_URL
    shell:
        'wget {params.url} -O {output.ped}'

rule get_xlxs:
    output:
        'resources/G1K_sample_info.xlsx'
    params:
        url = XLSX_URL
    shell:
        'wget {params.url} -O {output}'

rule overlap_with_available_data:
    input:
        ped = rules.get_ped.output[0]
    output:
        ped = 'resources/G1K_samples.with_fastq.ped'
    params:
        gs_data_base_url = GS_1GK_DATA_BASE_URL
    run:
        print('Reading the list of samples with the available fastq data in the google genomics'
            ' public bucket. There are 2950 samples there, while the metadata in excel from the FTP has'
            ' got 3500 samples, so we need to overlap two lists first.')
        print('Reading the list of samples...')
        cmd = f'gsutil ls "{params.gs_data_base_url}"'
        sample_dirs = subprocess.check_output(cmd, shell=True).decode().split('\n')
        sample_names_with_fastq = []
        print('Checking which samples have the FASTQ data...')
        for i in progressbar.progressbar(range(len(sample_dirs))):
            sd = sample_dirs[i]
            cmd = f'gsutil ls {sd}'
            # subdirs = subprocess.check_output(cmd, shell=True).decode().split('\n')
            # if any(subdir.endswith('sequence_read/') for subdir in subdirs):
            sample_names_with_fastq.append(sample_dirs[i].split('/')[-2])

        df = pd.read_csv(input.ped, sep='\t')
        print(df)
        df = df[df['Individual.ID'].isin(sample_names_with_fastq)]
        df.to_csv(output.ped, sep='\t', index=False)

rule select_few_samples:
    input:
        ped = rules.overlap_with_available_data.output.ped
    output:
        ped = f'resources/G1K_samples.with_fastq.selected{n}.ped'
    run:
        print(f'Selecting {n} samples...')
        df = pd.read_csv(input.ped, sep='\t')
        ceu_family_cond = df['Individual.ID'].isin(DEFAULT_INCLUDE)
        df = pd.concat([
            df[ceu_family_cond],
            df[~ceu_family_cond].sample(n - 3, random_state=1)
        ])
        df.to_csv(output.ped, sep='\t', index=False)

rule make_warp_inputs:
    input:
        ped = rules.select_few_samples.output.ped,
        json = WARP_REF_INPUT_JSON,
    output:
        expand(os.path.join(WARP_INPUTS_DIR, '{sample}.json'), sample=DEFAULT_INCLUDE)
    params:
        gs_data_base_url = GS_1GK_DATA_BASE_URL,
        warp_inputs_dir = WARP_INPUTS_DIR,
    run:
        print(f'Finding FASTQs and generating WARP input files...')
        with open(input.json) as fh:
            refs_data = json.load(fh)

        df = pd.read_csv(input.ped, sep='\t')

        for (_, row), _ in zip(df.iterrows(), progressbar.progressbar(range(len(df)))):
            sample = row['Individual.ID']

            print(f'Finding fastqs for {sample}')
            cmd = f'gsutil ls "{params.gs_data_base_url}/{sample}/sequence_read/*_*.filt.fastq.gz"'
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
            json_fpath = os.path.join(params.warp_inputs_dir, f'{sample}.json')
            with open(json_fpath, 'w') as out:
                json.dump(data, out, indent=4)

