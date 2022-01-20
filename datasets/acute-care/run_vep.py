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
@click.option(
    '--script',
    'script',
    help='path to vep bash script'
)
def main(file: str, script: str):
    """
    takes a given file argument,

    :param file: str, the GCP path for a given input file
    :param script: str, the path to the VEP script
    """
    dirname, filename = os.path.split(file)
    new_vcf_path = os.path.join(dirname, f'anno_{filename}')

    service_backend = hb.ServiceBackend(
        billing_project=os.getenv('HAIL_BILLING_PROJECT'),
        bucket=os.getenv('HAIL_BUCKET'),
    )

    # create a hail batch
    batch = hb.Batch(
        name='run_vep_in_dataproc_cluster',
        backend=service_backend
    )

    vep_cmd = f'{script} --infile {filename} --outfile {new_vcf_path}'

    job = dataproc.hail_dataproc_job(
        batch=batch,
        script=vep_cmd,
        max_age='4h',
        job_name='annotate_vcf',
        num_secondary_workers=4,
        cluster_name='annotate_vcf with max-age=4h',
        vep='GRCh38'
    )  # noqa: F841
    job.cpu(2)
    job.memory('standard')  # ~ 4G/core ~ 7.5G
    job.storage('20G')

    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
