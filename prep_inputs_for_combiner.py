#!/usr/bin/env python

"""
Uploads GVCFs and QC files to CPG and generates sample maps for the GVCF combiner
"""

import sys
import subprocess
import os
import time
from collections import defaultdict
import csv
from os.path import isdir, isfile, exists, basename, dirname, join
import click
import pandas as pd
from google.cloud import storage
import logging

logger = logging.getLogger('prep_cpg_qc_inputs')
logger.setLevel('INFO')


def run_cmd(cmd):
    print(cmd)
    return subprocess.run(cmd, shell=True)


def run_check_output(cmd, silent=False):
    if not silent:
        print(cmd)
    return subprocess.check_output(cmd, shell=True).decode()


PICARD_SUFFIX_D = {
    'contamination': 'selfSM',
    'alignment_summary_metrics': 'alignment_summary_metrics',
    'duplicate_metrics': 'duplicate_metrics',
    'insert_size_metrics': 'insert_size_metrics',
    'wgs_metrics': 'wgs_metrics',
}


def _find_gcs_files(warp_executions_bucket, work_dir, file_suffix, file_key):
    safe_mkdir(work_dir)
    found_file_fpath = join(work_dir, f'found_{file_key}.txt')
    if not isfile(found_file_fpath):
        path_tmpl = join(warp_executions_bucket, '**', f'*.{file_suffix}')
        run_cmd(f'gsutil ls \'{path_tmpl}\' > {found_file_fpath}')
    with open(found_file_fpath) as f:
        found_files = [l.strip() for l in f]
    found_path_by_sname = {
        os.path.basename(fp).replace(f'.{file_suffix}', ''): fp
        for fp in found_files
    }
    return found_path_by_sname


def _randomise_pop_labels(sample_df):
    # Adding population labels for 2/3 of the samples in a group;
    # the rest will be used to test the ancestry inferring methods
    return list(
        sample_df\
            .groupby(['Population']) \
            .apply(lambda x: x.iloc[:int(x['Population'].size / 1.5)]) \
            ['Individual.ID']
    )


def _move_locally(gvcf_by_sample, dataset, picard_file_by_sname_by_key):
    local_gvcf_by_sample = dict()
    local_picard_file_by_sname_by_key = defaultdict(dict)
    for sample, gvcf_path in gvcf_by_sample.items():
        local_path = f'gs://cpg-fewgenomes-upload/{sample}/gvcf/{basename(gvcf_path)}'
        if not file_exists(local_path):
            run_cmd(f'gsutil cp {gvcf_path} {local_path}')
        if not file_exists(local_path + '.tbi'):
            run_cmd(f'gsutil cp {gvcf_path}.tbi {local_path}.tbi')
        local_gvcf_by_sample[sample] = local_path

        for picard_key, picard_path_by_sname in picard_file_by_sname_by_key.items():
            picard_path = picard_path_by_sname.get(sample)
            if picard_path:
                local_path = f'gs://cpg-fewgenomes-upload/{sample}/picard_files/' \
                             f'{basename(picard_path)}'
                if not file_exists(local_path):
                    run_cmd(f'gsutil cp {picard_path} {local_path}')
                local_picard_file_by_sname_by_key[picard_key][sample] = local_path
    return local_gvcf_by_sample, local_picard_file_by_sname_by_key


@click.command()
@click.option(
    '--dataset',
    'dataset_name',
    required=True,
    help='Dataset name, e.g. "50genomes". Assumes that '
         '`{datasets_dir}/{datasets_name}/samples.ped` exists, unless --ped file is '
         'specified explicitly.'
)
@click.option(
    '--ped',
    'samples_ped',
    help='Ped file with input sample names. If not provided, '
         '`{datasets_dir}/{datasets_name}/samples.ped` is read.'
)
@click.option(
    '--warp-executions-bucket',
    'warp_executions_bucket',
    default=('gs://playground-us-central1/cromwell/executions/'
             'WGSMultipleSamplesFromBam'),
    help='Bucket with WARP workflow outputs.'
)
@click.option(
    '--datasets-dir',
    'datasets_dir',
    default='datasets',
    help='Output folder. Default is "datasets/".'
)
@click.option(
    '--work-dir',
    'work_dir',
    help='Directory to store temporary files.'
)
@click.option(
    '--split-rounds',
    'split_rounds',
    is_flag=True,
    help='Break samples into 2 groups to produce tests for the GVCF combiner.'
)
@click.option(
    '--randomise-pop-labels',
    'randomise_pop_labels',
    is_flag=True,
    help='Remove population labels for 1/3 of the samples to test sample-qc '
         'ancestry inference.'
)
@click.option(
    '--move-locally',
    'move_locally',
    is_flag=True,
    help='Move GVCFs and picard files to the gs://cpg-fewgenomes-upload bucket'
)
def main(
    dataset_name: str,
    samples_ped: str,
    datasets_dir: str = None,
    warp_executions_bucket: str = None,
    work_dir: str = None,
    split_rounds: bool = False,
    randomise_pop_labels: bool = False,
    move_locally: bool = False,
):
    """
    Generate test inputs for the combine_gvcfs.py script
    """

    work_dir = safe_mkdir(work_dir or f'work/{dataset_name}/prep_inputs_for_combiner')

    if not samples_ped:
        samples_ped = os.path.join(datasets_dir, dataset_name, 'samples.ped')
    try:
        sample_df = pd.read_csv(samples_ped, sep='\t')
    except FileNotFoundError:
        logger.error(f'Could not read file {samples_ped}')
        sys.exit(1)

    samples_with_pop_labels = None
    if randomise_pop_labels:
        samples_with_pop_labels = _randomise_pop_labels(sample_df)

    found_gvcf_path_by_sname = _find_gcs_files(
        warp_executions_bucket, work_dir, 'g.vcf.gz', 'gvcfs')
    found_picard_file_by_sname_by_key = defaultdict(dict)
    for picard_key, picard_suffix in PICARD_SUFFIX_D.items():
        found_picard_file_by_sname_by_key[picard_key] = _find_gcs_files(
            warp_executions_bucket, work_dir, picard_suffix, picard_key)

    gvcf_by_sample = dict()
    picard_file_by_sname_by_key = defaultdict(dict)
    for sample in sample_df['Individual.ID']:
        fpath = found_gvcf_path_by_sname.get(sample)
        if not fpath:
            logger.error(f'Could not find {sample}.g.vcf.gz in '
                         f'{warp_executions_bucket}')
            continue
        gvcf_by_sample[sample] = fpath

        for picard_key, picard_suffix in PICARD_SUFFIX_D.items():
            fpath = found_picard_file_by_sname_by_key[picard_key].get(sample)
            if not fpath:
                logger.error(f'Could not find {sample}.{picard_suffix} in '
                             f'{warp_executions_bucket}')
            else:
                picard_file_by_sname_by_key[picard_key][sample] = fpath
    # if any(fpath is None for fpath in gvcf_by_sample.values()):
    #     logger.error(f'ERROR: could not find a GVCF for some samples')
    #     sys.exit(1)

    if move_locally:
        gvcf_by_sample, picard_file_by_sname_by_key = \
            _move_locally(gvcf_by_sample, dataset_name, picard_file_by_sname_by_key)

    rows = []
    hdr = ['sample', 'population', 'gvcf'] + list(PICARD_SUFFIX_D.keys())
    for sample, pop in zip(
            sample_df['Individual.ID'],
            sample_df['Population'],
    ):
        if sample in gvcf_by_sample:
            row = dict(
                sample=sample,
                gvcf=gvcf_by_sample[sample],
                population=pop if (not samples_with_pop_labels or
                                   sample in samples_with_pop_labels) else ''
            )
            for picard_key, picard_suffix in PICARD_SUFFIX_D.items():
                picard_path = picard_file_by_sname_by_key.get(picard_key, {}).get(sample)
                row[picard_key] = ''
                if picard_path:
                    row[picard_key] = picard_path
            rows.append(row)

    assert rows[0]['gvcf'] != ''

    samplemap_prefix = join(datasets_dir, dataset_name, 'sample-maps', dataset_name)
    safe_mkdir(dirname(samplemap_prefix))
    with open(samplemap_prefix + '-all.csv', 'w', newline='') as out_all:
        csvwriter_all = csv.writer(out_all)
        csvwriter_all.writerow(hdr)
        for row in rows:
            csvwriter_all.writerow(row[h] for h in hdr)
    if split_rounds:
        with open(samplemap_prefix + '-round1.csv', 'w', newline='') as out_r1, \
                open(samplemap_prefix + '-round2.csv', 'w', newline='') as out_r2:
            csvwriter_r1 = csv.writer(out_r1)
            csvwriter_r2 = csv.writer(out_r2)
            csvwriter_r1.writerow(hdr)
            csvwriter_r2.writerow(hdr)
            round1_row_num = int(len(rows) // 1.5)
            for row in rows[:round1_row_num]:
                csvwriter_r1.writerow(row[h] for h in hdr)
            for row in rows[round1_row_num:]:
                csvwriter_r2.writerow(row[h] for h in hdr)


def safe_mkdir(dirpath: str, descriptive_name: str = '') -> str:
    """
    Multiprocessing-safely and recursively creates a directory
    """
    if not dirpath:
        sys.stderr.write(
            f'Path is empty: {descriptive_name if descriptive_name else ""}\n'
        )

    if isdir(dirpath):
        return dirpath

    if isfile(dirpath):
        sys.stderr.write(descriptive_name + ' ' + dirpath + ' is a file.\n')

    num_tries = 0
    max_tries = 10

    while not exists(dirpath):
        # we could get an error here if multiple processes are creating
        # the directory at the same time. Grr, concurrency.
        try:
            os.makedirs(dirpath)
        except OSError:
            if num_tries > max_tries:
                raise
            num_tries += 1
            time.sleep(2)
    return dirpath


def file_exists(path: str) -> bool:
    """
    Check if the object exists, where the object can be:
        * local file
        * local directory
        * Google Storage object
        * Google Storage URL representing a *.mt or *.ht Hail data,
          in which case it will check for the existince of a
          *.mt/_SUCCESS or *.ht/_SUCCESS file.
    :param path: path to the file/directory/object/mt/ht
    :return: True if the object exists
    """
    if path.startswith('gs://'):
        bucket = path.replace('gs://', '').split('/')[0]
        path = path.replace('gs://', '').split('/', maxsplit=1)[1]
        path = path.rstrip('/')  # ".mt/" -> ".mt"
        if any(path.endswith(f'.{suf}') for suf in ['mt', 'ht']):
            path = os.path.join(path, '_SUCCESS')
        gs = storage.Client()
        return gs.get_bucket(bucket).get_blob(path)
    return os.path.exists(path)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
