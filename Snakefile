import json
import os
import subprocess
from collections import defaultdict
from os.path import dirname
from typing import Dict, Tuple, List
import progressbar
import pandas as pd


# spreadsheet with 1kg metadata
XLSX_URL = 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/' \
           'working/20130606_sample_info/20130606_sample_info.xlsx'
PED_URL = 'ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/' \
          'working/20121016_updated_pedigree/G1K_samples_20111130.ped'
GS_1GK_DATA_BASE_URL = 'gs://genomics-public-data/' \
                       'ftp-trace.ncbi.nih.gov/1000genomes/ftp/phase3/data'
REFS_INPUT_WGSFROMBAM_JSON = 'resources/warp_refs_wgsfrombam.json'
REFS_INPUT_WGSFROMFASTQ_JSON = 'resources/warp_refs_wgsfromfastq.json'
REFS_INPUT_EXOMEFROMBAM_JSON = 'resources/warp_refs_exomefrombam.json'

# Including a platinum genome NA12878, and one full trio for testing
# the relatedness checks
DEFAULT_INCLUDE = [
    'NA12878', 'NA19238', 'NA19239', 'NA19240',
]

DATASETS_DIR = 'datasets/'

n = config.get('n', 50)  # the number of samples to select
assert n >= len(DEFAULT_INCLUDE)

INPUT_TYPES_TO_FOLDER_NAME = {
    'wgs_fastq': 'sequence_read',
    'wgs_bam': 'alignment',
    'exome_bam': 'exome_alignment',
    'wgs_bam_highcov': 'high_coverage_alignment',
}
input_type = config.get('input_type')
assert input_type in INPUT_TYPES_TO_FOLDER_NAME

assert 'dataset_name' in config, \
    'Specify dataset_name with snakemake --config dataset_name=NAME'
dataset_name = config['dataset_name']


rule all:
    input:
        dynamic(os.path.join(DATASETS_DIR, dataset_name,
            input_type,'{sample}.json'))


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

rule save_gs_ls:
    output:
        'resources/gs_phase3_data_ls.txt'
    params:
        gs_data_base_url = GS_1GK_DATA_BASE_URL
    shell:
        'gsutil ls "{params.gs_data_base_url}/*/" > {output}'

rule gs_ls_to_table:
    input:
        rules.save_gs_ls.output[0]
    output:
        tsv = 'work/gs_phase3_data.tsv'
    params:
        gs_data_base_url = GS_1GK_DATA_BASE_URL
    run:
        input_types_by_sample = defaultdict(list)
        with open(input[0]) as ls_inp:
            for line in ls_inp:
                line = line.strip()
                if line.startswith(params.gs_data_base_url) and line.endswith('/'):
                    # .../data/HG00096/exome_alignment/
                    tokens = line.split('/')
                    input_type = tokens[-2]
                    sample = tokens[-3]
                    input_types_by_sample[sample].append(input_type)
        with open(output.tsv, 'w') as out:
            for sample, input_types in input_types_by_sample.items():
                out.write(sample + '\t' + ','.join(input_types) + '\n')

rule overlap_with_available_data:
    input:
        ped = rules.get_ped.output[0],
        gs_tsv = rules.gs_ls_to_table.output.tsv,
    output:
        ped = 'work/G1K_samples.with_gs_data.ped'
    run:
        df = pd.read_csv(input.ped, sep='\t')
        for fold_n in INPUT_TYPES_TO_FOLDER_NAME.values():
            df[fold_n] = False

        with open(input.gs_tsv) as gs_tsv:
            for line in gs_tsv:
                line = line.strip()
                sample = line.split('\t')[0]
                folder_names = line.split('\t')[1].split(',')
                for fold_n in folder_names:
                    if fold_n in INPUT_TYPES_TO_FOLDER_NAME.values():
                        df.loc[df['Individual.ID'] == sample, fold_n] = True
        df.to_csv(output.ped, sep='\t', index=False)

rule select_few_samples:
    input:
        ped = rules.overlap_with_available_data.output.ped
    output:
        ped = os.path.join(DATASETS_DIR, dataset_name, 'samples.ped')
    params:
        input_type = input_type
    run:
        print(f'Selecting {n} samples...')
        df = pd.read_csv(input.ped, sep='\t')
        df = df[df[INPUT_TYPES_TO_FOLDER_NAME[params.input_type]] == True]
        default_sample_cond = df['Individual.ID'].isin(DEFAULT_INCLUDE)
        df = pd.concat([
            df[default_sample_cond],
            df[~default_sample_cond].sample(n - len(DEFAULT_INCLUDE), random_state=1)
        ])
        df.to_csv(output.ped, sep='\t', index=False)

rule make_warp_inputs:
    input:
        ped = rules.select_few_samples.output.ped,
        refs_wgsfrombam_json   = REFS_INPUT_WGSFROMBAM_JSON,
        refs_wgsfromfastq_json = REFS_INPUT_WGSFROMFASTQ_JSON,
        refs_exomefrombam_json = REFS_INPUT_EXOMEFROMBAM_JSON,
    output:
        dynamic(os.path.join(DATASETS_DIR, dataset_name, input_type,'{sample}.json'))
    params:
        gs_data_base_url = GS_1GK_DATA_BASE_URL,
        dataset_dir = os.path.join(DATASETS_DIR, dataset_name),
        input_type = input_type
    run:
        print(f'Finding inputs and generating WARP input files...')
        df = pd.read_csv(input.ped, sep='\t')
        with open(input.refs_wgsfrombam_json) as fh:
            refs_wgsfrombam_data = json.load(fh)
        with open(input.refs_wgsfromfastq_json) as fh:
            refs_wgsfromfastq_data = json.load(fh)
        with open(input.refs_exomefrombam_json) as fh:
            refs_exomefrombam_data = json.load(fh)

        for (_, row), _ in zip(df.iterrows(), progressbar.progressbar(range(len(df)))):
            sample = row['Individual.ID']
            data = dict()

            if input_type in ['exome_bam', 'wgs_bam', 'wgs_bam_highcov']:
                print(f'Finding BAMs for {sample}')
                cmd = f'gsutil ls "{params.gs_data_base_url}/{sample}/' \
                      f'{INPUT_TYPES_TO_FOLDER_NAME[input_type]}/{sample}.mapped.*.bam"'
                bam_fpaths = subprocess.check_output(cmd, shell=True).decode().split('\n')
                bam_fpath = bam_fpaths[0]
                bai_fpath = bam_fpath + '.bai'

                if input_type in ['exome_bam']:
                    wfl_name = 'ExomeFromBam'
                else:
                    wfl_name = 'WGSFromBam'

                data[f'{wfl_name}.sample_name'] = sample
                data[f'{wfl_name}.base_file_name'] = sample
                data[f'{wfl_name}.final_gvcf_base_name'] = sample
                data[f'{wfl_name}.input_bam'] = bam_fpath
                if input_type in ['exome_bam']:
                    data.update(refs_exomefrombam_data)
                else:
                    data.update(refs_wgsfrombam_data)

            elif input_type in ['wgs_fastq']:
                print(f'Finding fastqs for {sample}')
                cmd = f'gsutil ls "{params.gs_data_base_url}/{sample}/' \
                      f'sequence_read/*_*.filt.fastq.gz"'
                fastq_fpaths = subprocess.check_output(cmd, shell=True).decode().split('\n')
                r1_fpaths = sorted([fp for fp in fastq_fpaths if
                                    fp.endswith('1.filt.fastq.gz')])
                r2_fpaths = sorted([fp for fp in fastq_fpaths if
                                    fp.endswith('2.filt.fastq.gz')])
                fastq_pairs = list(zip(r1_fpaths, r2_fpaths))

                data = dict()
                data['WGSFromFastq.sample_and_fastqs'] = dict(
                    sample_name=sample,
                    base_file_name=sample,
                    final_gvcf_base_name=sample,
                    fastqs=fastq_pairs,
                )
                data.update(refs_wgsfromfastq_data)

            json_fpath = os.path.join(params.dataset_dir, input_type, f'{sample}.json')
            with open(json_fpath, 'w') as out:
                json.dump(data, out, indent=4)
