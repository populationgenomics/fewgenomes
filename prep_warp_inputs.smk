"""
Generates JSON inputs for WARP WGS pipelines
"""

import json
import os
import subprocess
from collections import defaultdict
from os.path import basename
import numpy as np
import pandas as pd
import progressbar


# spreadsheet with 1kg metadata
XLSX_URL = (
    'ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/'
    '20130606_sample_info/20130606_sample_info.xlsx'
)
PED_URL = (
    'ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/'
    '20121016_updated_pedigree/G1K_samples_20111130.ped'
)
GS_1GK_DATA_BUCKET = (
    'gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/phase3/data'
)
GATKSV_SAMPLES_JSON_URL = (
    'https://raw.githubusercontent.com/broadinstitute/gatk-sv/master/input_values/'
    'ref_panel_1kg.json'
)

# Including a platinum genome NA12878, and one full trio for testing
# the relatedness checks
DEFAULT_INCLUDE = config.get('default_samples', 
    'NA12878,NA19238,NA19239,NA19240').split(',')
DATASETS_DIR = 'datasets/'

SAMPLE_N = config.get('n', len(DEFAULT_INCLUDE))  # the number of samples to select
if SAMPLE_N:
    DEFAULT_INCLUDE = DEFAULT_INCLUDE[:SAMPLE_N]

INPUT_TYPES_TO_FOLDER_NAME = {
    'wgs_fastq': 'sequence_read',
    'wgs_bam': 'alignment',
    'wgs_bam_highcov': 'high_coverage_alignment',
    'exome_bam': 'exome_alignment',
}
INPUT_TYPES_TO_WORKFLOW_NAME = {
   'wgs_fastq': 'WGSFromFastq',
   'wgs_bam': 'WGSFromBam',
   'wgs_bam_highcov': 'WGSFromBam',
   'exome_bam': 'ExomeFromBam'
}
INPUT_TYPE = config.get('input_type')
assert INPUT_TYPE in INPUT_TYPES_TO_FOLDER_NAME

assert 'dataset_name' in config, \
    'Specify dataset_name with snakemake --config dataset_name=NAME'
DATASET = config['dataset_name']


rule all:
    input:
        singlesample_warp_inputs = \
            dynamic(os.path.join(DATASETS_DIR, DATASET, INPUT_TYPE, '{sample}.json')),
        multisample_warp_input = \
            os.path.join(DATASETS_DIR, DATASET, f'{DATASET}-{INPUT_TYPE}.json')

def get_warp_input_json_url(wfl_name):
    return (
        f'https://raw.githubusercontent.com/populationgenomics/cromwell-configs/'
        f'main/warp-input-templates/{wfl_name}-inputs.json'
    )

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
        gs_data_base_url = GS_1GK_DATA_BUCKET
    shell:
        'gsutil -u fewgenomes ls "{params.gs_data_base_url}/*/" > {output}'

rule gs_ls_to_table:
    input:
        rules.save_gs_ls.output[0]
    output:
        tsv = 'work/gs_phase3_data.tsv'
    params:
        gs_data_base_url = GS_1GK_DATA_BUCKET
    run:
        input_types_by_sample = defaultdict(list)
        with open(input[0]) as ls_inp:
            for line in ls_inp:
                line = line.strip()
                if line.startswith(params.gs_data_base_url) and line.endswith('/'):
                    # .../data/HG00096/exome_alignment/
                    tokens = line.split('/')
                    INPUT_TYPE = tokens[-2]
                    sample = tokens[-3]
                    input_types_by_sample[sample].append(INPUT_TYPE)
        with open(output.tsv, 'w') as out:
            for sample, input_types in input_types_by_sample.items():
                out.write(sample + '\t' + ','.join(input_types) + '\n')

rule gatksv_to_table:
    output:
        tsv = 'work/gatksv_data.txt'
    params:
        url = GATKSV_SAMPLES_JSON_URL
    run:
        shell(f'wget {params.url} -O work/{basename(params.url)}')
        with open(f'work/{basename(params.url)}') as fh:
            data = json.load(fh)
        cram_urls = data['bam_or_cram_files']
        with open(output.tsv, 'w') as out:
            for cram_url in cram_urls:
                out.write(cram_url + '\n')

rule overlap_with_available_data:
    input:
        ped = rules.get_ped.output[0],
        gs_tsv = rules.gs_ls_to_table.output.tsv,
        gatksv_tsv = rules.gatksv_to_table.output.tsv,
    output:
        ped = 'work/G1K_samples.with_gs_data.ped'
    run:
        df = pd.read_csv(input.ped, sep='\t')
        for folder_name in INPUT_TYPES_TO_FOLDER_NAME.values():
            df[folder_name] = False

        with open(input.gs_tsv) as gs_tsv:
            for line in gs_tsv:
                line = line.strip()
                sample = line.split('\t')[0]
                folder_names = line.split('\t')[1].split(',')
                for folder_name in folder_names:
                    if folder_name in INPUT_TYPES_TO_FOLDER_NAME.values():
                        df.loc[df['Individual.ID'] == sample, folder_name] = True
        with open(input.gatksv_tsv) as gatksv_tsv:
            for line in gatksv_tsv:
                cram_path = line.strip()
                sample = basename(cram_path).replace('.final.cram', '')
                df.loc[df['Individual.ID'] == sample, 'gatksv_cram'] = cram_path

        df.to_csv(output.ped, sep='\t', index=False)

rule select_few_samples:
    input:
        ped = rules.overlap_with_available_data.output.ped
    output:
        ped = os.path.join(DATASETS_DIR, DATASET, 'samples.ped')
    params:
        input_type = INPUT_TYPE
    run:
        df = pd.read_csv(input.ped, sep='\t')
        df = df[~df['gatksv_cram'].isnull() | df['high_coverage_alignment']]
        default_sample_cond = df['Individual.ID'].isin(DEFAULT_INCLUDE)
        print(f'Selecting {SAMPLE_N} samples...')
        df = pd.concat([
            df[default_sample_cond],
            df[~default_sample_cond].sample(SAMPLE_N - len(DEFAULT_INCLUDE), 
                random_state=1)
        ])
        df.to_csv(output.ped, sep='\t', index=False)

rule make_sample_map:
    input:
        ped = rules.select_few_samples.output.ped
    output:
        sample_map = os.path.join(DATASETS_DIR, DATASET, f'{DATASET}-{INPUT_TYPE}.tsv'),
    params:
        gs_data_base_url = GS_1GK_DATA_BUCKET,
    run:
        print(f'Finding inputs and generating WARP input files...')
        df = pd.read_csv(input.ped, sep='\t')
        
        input_files_by_sample = dict()
        for (_, row), _ in zip(df.iterrows(), progressbar.progressbar(range(len(df)))):
            sample = row['Individual.ID']

            if INPUT_TYPE in ['exome_bam', 'wgs_bam', 'wgs_bam_highcov']:
                if row[INPUT_TYPES_TO_FOLDER_NAME[INPUT_TYPE]]:
                    print(f'Finding BAMs for {sample}')
                    cmd = f'gsutil -u fewgenomes ls "{params.gs_data_base_url}/{sample}/' \
                          f'{INPUT_TYPES_TO_FOLDER_NAME[INPUT_TYPE]}/{sample}.*.bam"'
                    bam_fpaths = subprocess.check_output(cmd, shell=True).decode().split('\n')
                    bam_fpath = bam_fpaths[0]
                    input_files_by_sample[sample] = bam_fpath
                if row['gatksv_cram'] and isinstance(row['gatksv_cram'], str):
                    input_files_by_sample[sample] = row['gatksv_cram']

            elif INPUT_TYPE in ['wgs_fastq']:
                print(f'Finding fastqs for {sample}')
                cmd = f'gsutil -u fewgenomes ls "{params.gs_data_base_url}/{sample}/' \
                      f'sequence_read/*_*.filt.fastq.gz"'
                fastq_fpaths = subprocess.check_output(cmd, shell=True).decode().split('\n')
                r1_fpaths = sorted([fp for fp in fastq_fpaths if
                                    fp.endswith('1.filt.fastq.gz')])
                r2_fpaths = sorted([fp for fp in fastq_fpaths if
                                    fp.endswith('2.filt.fastq.gz')])
                fastq_pairs = list(zip(r1_fpaths, r2_fpaths))
                input_files_by_sample[sample] = \
                    ','.join(['|'.join(fp) for fp in fastq_pairs])

        with open(output.sample_map, 'w') as out:
            for sample, input_files in input_files_by_sample.items():
                out.write('\t'.join([sample, input_files]) + '\n')

rule make_multisample_warp_input:
    input:
        sample_map = rules.make_sample_map.output.sample_map,
    output:
        os.path.join(DATASETS_DIR, DATASET, f'{DATASET}-{INPUT_TYPE}.json')
    params:
        wfl_name = INPUT_TYPES_TO_WORKFLOW_NAME.get(INPUT_TYPE),
    run:
        multi_wfl_name = params.wfl_name.replace('From', 'MultipleSamplesFrom')        
        wfl_tmpl_url = get_warp_input_json_url(multi_wfl_name)
        wfl_tmpl_path = 'work/warp-input-multi.json'
        shell(f'wget {wfl_tmpl_url} -O {wfl_tmpl_path}')
        
        with open(wfl_tmpl_path) as fh:
            data = json.load(fh)
            data[f'{multi_wfl_name}.sample_map'] = os.path.abspath(input.sample_map)
        with open(output[0], 'w') as out:
            json.dump(data, out, indent=4)

rule make_singlesample_warp_inputs:
    input:
        sample_map = rules.make_sample_map.output.sample_map,
    output:
        dynamic(os.path.join(DATASETS_DIR, DATASET, INPUT_TYPE, '{sample}.json')),
    params:
        dataset_dir = os.path.join(DATASETS_DIR, DATASET),
        input_type = INPUT_TYPE,
        wfl_name = INPUT_TYPES_TO_WORKFLOW_NAME.get(INPUT_TYPE)
    run:
        print(f'Finding inputs and generating WARP input files...')
        wfl_tmpl_url = get_warp_input_json_url(params.wfl_name)
        wfl_tmpl_path = 'work/warp-input.json'
        shell(f'wget {wfl_tmpl_url} -O {wfl_tmpl_path}')
        with open(wfl_tmpl_path) as fh:
            data_tmpl = json.load(fh)

        with open(input.sample_map) as fh:
            for line in fh:
                sample, input_files = line.strip().split()
                
                data = data_tmpl.copy()

                if INPUT_TYPE in ['exome_bam', 'wgs_bam', 'wgs_bam_highcov']:
                    bam_fpath = input_files
                    data[f'{params.wfl_name}.sample_name'] = sample
                    data[f'{params.wfl_name}.base_file_name'] = sample
                    data[f'{params.wfl_name}.final_gvcf_base_name'] = sample
                    data[f'{params.wfl_name}.input_bam'] = bam_fpath
    
                elif INPUT_TYPE in ['wgs_fastq']:
                    fastq_pairs = [fp.split('|') for fp in input_files.split(',')]
    
                    data['WGSFromFastq.sample_and_fastqs'] = dict(
                        sample_name=sample,
                        base_file_name=sample,
                        final_gvcf_base_name=sample,
                        fastqs=fastq_pairs,
                    )
    
                json_fpath = os.path.join(params.dataset_dir, INPUT_TYPE, f'{sample}.json')
                with open(json_fpath, 'w') as out:
                    json.dump(data, out, indent=4)
