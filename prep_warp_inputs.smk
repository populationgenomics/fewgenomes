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
GVCF_1KG_BUCKET_PATTERNS = [
    'gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_14151/'
    'Project_CCDG_14151_B01_GRM_WGS.gVCF.2020-02-12/'
    'Project_CCDG_14151_B01_GRM_WGS.gVCF.2020-02-12/'
    'Sample_*/analysis/*.haplotypeCalls.er.raw.vcf.gz',
    'gs://fc-56ac46ea-efc4-4683-b6d5-6d95bed41c5e/CCDG_13607/'
    'Project_CCDG_13607_B01_GRM_WGS.gVCF.2019-02-06/'
    'Sample_*/analysis/*.haplotypeCalls.er.raw.g.vcf.gz'
]


DATASETS_DIR = 'datasets/'

# E.g. including a platinum genome NA12878 trio for testing the relatedness checks
DEFAULT_INCLUDE = config.get('default_include', '').split(',')
SAMPLE_N = config.get('n')  # the number of samples to select
FAMILIES_N = config.get('families', 0)  # the number of families to select
assert not (SAMPLE_N and FAMILIES_N), 'Only one of -n and --families can be defined'
if DEFAULT_INCLUDE:
    if SAMPLE_N:
        DEFAULT_INCLUDE = DEFAULT_INCLUDE[:SAMPLE_N]
    if FAMILIES_N:
        DEFAULT_INCLUDE = DEFAULT_INCLUDE[:FAMILIES_N]

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
INPUT_TYPES = config.get('input_type', '').split(',')
assert all(it in INPUT_TYPES_TO_FOLDER_NAME for it in INPUT_TYPES)

assert 'dataset_name' in config, \
    'Specify dataset_name with snakemake --config dataset_name=NAME'
DATASET = config['dataset_name']

ANCESTRY = config.get('ancestry')

# Base bucket to copy files to
COPY_LOCALY_BUCKET = 'gs://cpg-fewgenomes-main'

OUT_SAMPLE_MAP_TSV = os.path.join(DATASETS_DIR, DATASET, f'{DATASET}-{"-".join(INPUT_TYPES)}-local.tsv')


rule all:
    input:
        OUT_SAMPLE_MAP_TSV

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
        url_patterns = GVCF_1KG_BUCKET_PATTERNS
    run:
        shell('touch {output}')
        for ptn in params.url_patterns:
            shell('gsutil -u fewgenomes ls "{ptn}" >> {output}')

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
                    components = line.split('/')
                    it = components[-2]
                    sample = components[-3]
                    input_types_by_sample[sample].append(it)
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

rule select_samples_or_families:
    input:
        ped = rules.overlap_with_available_data.output.ped
    output:
        ped = os.path.join(DATASETS_DIR, DATASET, 'samples.ped')
    run:
        df = pd.read_csv(input.ped, sep='\t')
        if set(INPUT_TYPES) & {'wgs_bam_highcov'}:
            df = df[~df['gatksv_cram'].isnull() | df['high_coverage_alignment']]
        other_its = [it for it in INPUT_TYPES if it in ['wgs_bam', 'exome_bam', 'gvcf']]
        for it in other_its:
            df = df[df[INPUT_TYPES_TO_FOLDER_NAME[it]]]
        
        if ANCESTRY:
            df = df[df['Population'] == ANCESTRY]

        if SAMPLE_N:
            print(f'Selecting {SAMPLE_N} samples')
        if FAMILIES_N:
            print(f'Selecting {FAMILIES_N} families')
            
        from peddy import Ped
        ped = Ped(input.ped)
        all_samples = set(df['Individual.ID'])
        related_pairs_per_family = dict()
        for fam_id, fam in ped.families.items():
            relation_by_pair = dict()  # pair of str -> relation str
            for s1 in fam.samples:
                for s2 in fam.samples:
                    if (s1.sample_id != s2.sample_id and 
                        s1.sample_id in all_samples and 
                        s2.sample_id in all_samples
                    ):
                        pair = tuple(sorted([s1.sample_id, s2.sample_id]))
                        if pair not in relation_by_pair:
                            relation = ped.relation(s1, s2)
                            if relation not in [
                                'unrelated', 
                                'unknown', 
                                'related at unknown level',
                                'mom-dad',
                            ]:
                                relation_by_pair[pair] = relation
            if len(relation_by_pair) > 0:
                related_pairs_per_family[fam_id] = relation_by_pair

        # Selecting families with >2 related pairs
        big_fam_ids = [fam_id for fam_id, relation_by_pair in related_pairs_per_family.items()
                       if len(relation_by_pair) >= 2]
        # Selecting smallest families first, as it would prioritize nice trios
        big_fam_ids = sorted(big_fam_ids,
            key=lambda id: len([s for s in ped.families[id].samples if s.sample_id in all_samples]))
        for id in big_fam_ids:
            relation_by_pair = related_pairs_per_family[id]
            print(f'Family {id}: pairs of samples {relation_by_pair}')
        print(f'Found {len(big_fam_ids)} candidate families with >=2 related pairs')
        big_fam_ids = big_fam_ids[:FAMILIES_N]

        default_sample_cond = df['Individual.ID'].isin(DEFAULT_INCLUDE)
        families_cond = df['Family.ID'].isin(big_fam_ids)
        
        if FAMILIES_N:
            df = pd.concat([df[families_cond]])

        if SAMPLE_N:
            df = pd.concat([
                df[default_sample_cond],
                df[~default_sample_cond].sample(SAMPLE_N - len(DEFAULT_INCLUDE), 
                    random_state=1)
            ])

        df.to_csv(output.ped, sep='\t', index=False)

rule make_sample_map:
    input:
        ped = rules.select_samples_or_families.output.ped
    output:
        sample_map = os.path.join(DATASETS_DIR, DATASET, f'{DATASET}-{"-".join(INPUT_TYPES)}.tsv'),
    params:
        gs_data_bucket = GS_1GK_DATA_BUCKET,
        gvcf_bucket_ptrns = GVCF_1KG_BUCKET_PATTERNS,
    run:
        print(f'Finding inputs and generating WARP input files...')
        df = pd.read_csv(input.ped, sep='\t')
        
        input_files_by_sample = defaultdict(list)
        for (_, row), _ in zip(df.iterrows(), progressbar.progressbar(range(len(df)))):
            sample = row['Individual.ID']

            its = [it for it in INPUT_TYPES if it in ['exome_bam', 'wgs_bam', 'wgs_bam_highcov']]
            for it in its:
                if row[INPUT_TYPES_TO_FOLDER_NAME[it]]:
                    print(f'Finding BAMs for {sample}')
                    cmd = f'gsutil -u fewgenomes ls "{params.gs_data_bucket}/{sample}/' \
                          f'{INPUT_TYPES_TO_FOLDER_NAME[it]}/{sample}.*.bam"'
                    bam_fpaths = subprocess.check_output(cmd, shell=True).decode().split('\n')
                    bam_fpath = bam_fpaths[0]
                    input_files_by_sample[sample].append(bam_fpath)
                if row['gatksv_cram'] and isinstance(row['gatksv_cram'], str):
                    input_files_by_sample[sample].append(row['gatksv_cram'])

            its = [it for it in INPUT_TYPES if it == 'wgs_fastq']
            for it in its:
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

            its = [it for it in INPUT_TYPES if it == 'gvcf']
            for it in its:
                if row[INPUT_TYPES_TO_FOLDER_NAME[it]]:
                    print(f'Finding GVCFs for {sample}')
                    for ptn in params.gvcf_bucket_ptrns:
                        path = ptn.replace('*', sample)
                        cmd = f'gsutil -u fewgenomes ls "{path}"'
                        print(cmd)
                        try:
                            subprocess.check_output(cmd, shell=True).decode().split('\n')
                        except subprocess.CalledProcessError:
                            pass
                        else:
                            input_files_by_sample[sample] = path
                            break

        with open(output.sample_map, 'w') as out:
            for sample, input_files in input_files_by_sample.items():
                out.write('\t'.join([sample, input_files]) + '\n')

sample_map = rules.make_sample_map.output.sample_map

if COPY_LOCALY_BUCKET:
    rule copy_gvcf:
        input:
            sample_map = rules.make_sample_map.output.sample_map,
        output:
            sample_map = OUT_SAMPLE_MAP_TSV,
        params:
            bucket = COPY_LOCALY_BUCKET,
            dataset = DATASET,
        run:
            with open(input.sample_map) as f,\
                 open(output.sample_map, 'w') as out:
                for line in f:
                    sample, gvcf = line.strip().split()
                    out_gvcf_name = f'{sample}.g.vcf.gz'
                    target_path = f'{params.bucket}/gvcf/batch1/{out_gvcf_name}'
                    shell(f'gsutil ls {target_path} || gsutil -u fewgenomes cp {gvcf} {target_path}')
                    shell(f'gsutil ls {target_path}.tbi || gsutil -u fewgenomes cp {gvcf}.tbi {target_path}.tbi')
                    out.write('\t'.join([sample, target_path]) + '\n')

    sample_map = rules.copy_gvcf.output.sample_map
