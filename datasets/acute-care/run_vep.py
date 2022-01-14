#!/usr/bin/env python3

"""
create a dataproc cluster, and try to use VEP within that
- not using Hail
"""


import os
import hailtop.batch as hb

from analysis_runner import dataproc

import click


@click.command()
@click.option(
    '--file',
    'file',
    help='file to annotate'
)
def main(file: str):
    """
    takes a given file argument,

    :param file: str, the GCP path for a given input file
    """
    dirname, filename = os.path.split(file)
    new_vcf_path = os.path.join(dirname, f'anno_{filename}')

    vep_cmd = f'/vep --format vcf -i {file} ' \
              '--everything --allele_number --no_stats ' \
              '--cache --offline --minimal ' \
              f'--assembly GRCh38 --vcf -o {new_vcf_path}'

    service_backend = hb.ServiceBackend(
        billing_project=os.getenv('HAIL_BILLING_PROJECT'),
        bucket=os.getenv('HAIL_BUCKET'),
    )

    # create a hail batch
    batch = hb.Batch(
        name='run_vep_in_dataproc_cluster',
        backend=service_backend
    )

    _job = dataproc.hail_dataproc_job(
        batch=batch,
        script=vep_cmd,
        max_age='4h',
        job_name='annotate_vcf',
        num_secondary_workers=4,
        cluster_name='annotate_vcf with max-age=4h',
        vep='GRCh38'
    )  # noqa: F841

    batch.run(wait=False)
