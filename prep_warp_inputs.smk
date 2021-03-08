"""
Generates JSON inputs for WARP WGS pipelines
"""

import json
import os
import subprocess
from collections import defaultdict
from os.path import basename
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
GVCF_1KG_BUCKET_PATTERN = (
    'gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_14151/'
    'Project_CCDG_14151_B01_GRM_WGS.gVCF.2020-02-12/'
    'Project_CCDG_14151_B01_GRM_WGS.gVCF.2020-02-12/'
    'Sample_*/analysis/*.haplotypeCalls.er.raw.vcf.gz'
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
    'gvcf': 'gvcf',
}
INPUT_TYPES_TO_WORKFLOW_NAME = {
   'wgs_fastq': 'WGSFromFastq',
   'wgs_bam': 'WGSFromBam',
   'wgs_bam_highcov': 'WGSFromBam',
   'exome_bam': 'ExomeFromBam',
   'gvcf': 'PrepareGvcfsWf',
}
INPUT_TYPE = config.get('input_type')
assert INPUT_TYPE in INPUT_TYPES_TO_FOLDER_NAME

assert 'dataset_name' in config, \
    'Specify dataset_name with snakemake --config dataset_name=NAME'
DATASET = config['dataset_name']

# Base bucket to copy files to
COPY_LOCALY_BUCKET = config.get('copy_localy_bucket')


if INPUT_TYPE == 'gvcf':
    rule all:
        input:
            os.path.join(DATASETS_DIR, DATASET, f'{DATASET}-gvcf.json')
else:
    rule all:
        input:
            singlesample_warp_inputs = \
                dynamic(os.path.join(DATASETS_DIR, DATASET, INPUT_TYPE, '{sample}-warp.json')),
            multisample_warp_input = \
                os.path.join(DATASETS_DIR, DATASET, f'{DATASET}-warp-{INPUT_TYPE}.json')
    
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
        'resources/gs-phase3-data-ls.txt'
    params:
        gs_data_base_url = GS_1GK_DATA_BUCKET
    shell:
        'gsutil -u fewgenomes ls "{params.gs_data_base_url}/*/" > {output}'

rule save_gvcf_ls:
    output:
        'resources/gs-gvcfs.txt'
    params:
        url_pattern = GVCF_1KG_BUCKET_PATTERN
    shell:
        'gsutil -u fewgenomes ls "{params.url_pattern}" > {output}'

rule gs_ls_to_table:
    input:
        gs_ls_output = rules.save_gs_ls.output[0],
        gvcf_ls_output = rules.save_gvcf_ls.output[0],
    output:
        tsv = 'work/gs-ls-data.tsv'
    params:
        gs_data_base_url = GS_1GK_DATA_BUCKET
    run:
        input_types_by_sample = defaultdict(list)
        with open(input.gs_ls_output) as ls_inp:
            for line in ls_inp:
                line = line.strip()
                if line.startswith(params.gs_data_base_url) and line.endswith('/'):
                    # .../data/HG00096/exome_alignment/
                    tokens = line.split('/')
                    INPUT_TYPE = tokens[-2]
                    sample = tokens[-3]
                    input_types_by_sample[sample].append(INPUT_TYPE)
        with open(input.gvcf_ls_output) as ls_inp:
            for line in ls_inp:
                line = line.strip()
                # HG00405.haplotypeCalls.er.raw.vcf.gz -> HG00405
                sample = line.split('/')[-1].split('.')[0]
                input_types_by_sample[sample].append('gvcf')
        with open(output.tsv, 'w') as out:
            for sample, input_types in input_types_by_sample.items():
                out.write(sample + '\t' + ','.join(input_types) + '\n')

rule gatksv_to_table:
    output:
        tsv = 'work/gatksv-data.txt'
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
        ped = 'work/g1k-samples-with-gs-data.ped'
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
        if INPUT_TYPE in ['wgs_bam_highcov']:
            df = df[~df['gatksv_cram'].isnull() | df['high_coverage_alignment']]
        elif INPUT_TYPE in ['wgs_bam', 'exome_bam', 'gvcf']:
            df = df[df[INPUT_TYPES_TO_FOLDER_NAME[INPUT_TYPE]]]
        
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
        gs_data_bucket = GS_1GK_DATA_BUCKET,
        gvcf_bucket_ptrn = GVCF_1KG_BUCKET_PATTERN,
    run:
        print(f'Finding inputs and generating WARP input files...')
        df = pd.read_csv(input.ped, sep='\t')
        
        input_files_by_sample = dict()
        for (_, row), _ in zip(df.iterrows(), progressbar.progressbar(range(len(df)))):
            sample = row['Individual.ID']

            if INPUT_TYPE in ['exome_bam', 'wgs_bam', 'wgs_bam_highcov']:
                if row[INPUT_TYPES_TO_FOLDER_NAME[INPUT_TYPE]]:
                    print(f'Finding BAMs for {sample}')
                    cmd = f'gsutil -u fewgenomes ls "{params.gs_data_bucket}/{sample}/' \
                          f'{INPUT_TYPES_TO_FOLDER_NAME[INPUT_TYPE]}/{sample}.*.bam"'
                    bam_fpaths = subprocess.check_output(cmd, shell=True).decode().split('\n')
                    bam_fpath = bam_fpaths[0]
                    input_files_by_sample[sample] = bam_fpath
                if row['gatksv_cram'] and isinstance(row['gatksv_cram'], str):
                    input_files_by_sample[sample] = row['gatksv_cram']

            elif INPUT_TYPE in ['wgs_fastq']:
                print(f'Finding fastqs for {sample}')
                cmd = f'gsutil -u fewgenomes ls "{params.gs_data_bucket}/{sample}/' \
                      f'sequence_read/*_*.filt.fastq.gz"'
                fastq_fpaths = subprocess.check_output(cmd, shell=True).decode().split('\n')
                r1_fpaths = sorted([fp for fp in fastq_fpaths if
                                    fp.endswith('1.filt.fastq.gz')])
                r2_fpaths = sorted([fp for fp in fastq_fpaths if
                                    fp.endswith('2.filt.fastq.gz')])
                fastq_pairs = list(zip(r1_fpaths, r2_fpaths))
                input_files_by_sample[sample] = \
                    ','.join(['|'.join(fp) for fp in fastq_pairs])
                
            elif INPUT_TYPE in ['gvcf']:
                print(row)
                if row[INPUT_TYPES_TO_FOLDER_NAME[INPUT_TYPE]]:
                    print(f'Finding GVCFs for {sample}')
                    path = params.gvcf_bucket_ptrn.replace('*', sample)
                    input_files_by_sample[sample] = path

        with open(output.sample_map, 'w') as out:
            for sample, input_files in input_files_by_sample.items():
                out.write('\t'.join([sample, input_files]) + '\n')

sample_map = rules.make_sample_map.output.sample_map

if COPY_LOCALY_BUCKET:
    rule copy_gvcf:
        input:
            sample_map = rules.make_sample_map.output.sample_map,
        output:
            sample_map = os.path.join(DATASETS_DIR, DATASET, f'{DATASET}-{INPUT_TYPE}-local.tsv'),
        params:
            bucket = COPY_LOCALY_BUCKET,
            dataset = DATASET,
        run:
            with open(input.sample_map) as f,\
                 open(output.sample_map, 'w') as out:
                for line in f:
                    sample, gvcf = line.strip().split()
                    out_gvcf_name = f'{sample}.g.vcf.gz'
                    target_path = f'{params.bucket}/original-gvcf/{sample}/gvcf/{out_gvcf_name}'
                    shell(f'gsutil ls {target_path} || gsutil -u fewgenomes cp {gvcf} {target_path}')
                    shell(f'gsutil ls {target_path}.tbi || gsutil -u fewgenomes cp {gvcf}.tbi {target_path}.tbi')
                    out.write('\t'.join([sample, target_path]) + '\n')
       
    sample_map = rules.copy_gvcf.output.sample_map
        
rule make_prepare_gvcf_input:
    input:
        sample_map = sample_map,
    output:
        os.path.join(DATASETS_DIR, DATASET, f'{DATASET}-gvcf.json')
    params:
        wfl_name = INPUT_TYPES_TO_WORKFLOW_NAME.get(INPUT_TYPE),
    run:
        multi_wfl_name = params.wfl_name
        wfl_tmpl_path = 'wdl/prepare-gvcfs-inputs.json'
        with open(wfl_tmpl_path) as fh:
            data = json.load(fh)
        
        samples = []
        gvcfs = []
        with open(input.sample_map) as f:
            for line in f:
                sample, gvcf = line.strip().split() 
                samples.append(sample)
                gvcfs.append(gvcf)
        data[f'{multi_wfl_name}.samples'] = samples
        data[f'{multi_wfl_name}.gvcfs'] = gvcfs
        with open(output[0], 'w') as out:
            json.dump(data, out, indent=4)

rule make_multisample_warp_input:
    input:
        sample_map = rules.make_sample_map.output.sample_map,
    output:
        os.path.join(DATASETS_DIR, DATASET, f'{DATASET}-warp-{INPUT_TYPE}.json')
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
        dynamic(os.path.join(DATASETS_DIR, DATASET, INPUT_TYPE, '{sample}-warp.json')),
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
    
                json_fpath = os.path.join(params.dataset_dir, INPUT_TYPE, f'{sample}-warp.json')
                with open(json_fpath, 'w') as out:
                    json.dump(data, out, indent=4)
