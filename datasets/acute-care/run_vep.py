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

    # read into the batch
    input_vcf = batch.read_input(file)

    # try and trick batch
    job_1 = batch.new_job(name='fake_job')

    # create as a real file in the batch
    job_1.command(f'touch {job_1.ofile}')
    vep_cmd = f'{script} --infile {input_vcf} --outfile {job_1.ofile}'

    job_2 = dataproc.hail_dataproc_job(
        batch=batch,
        script=vep_cmd,
        max_age='4h',
        job_name='annotate_vcf',
        num_secondary_workers=4,
        cluster_name='annotate_vcf with max-age=4h',
        vep='GRCh38'
    )  # noqa: F841
    job_2.cpu(2)
    job_2.memory('standard')  # ~ 4G/core ~ 7.5G
    job_2.storage('20G')
    job_1.write_output(job_1.ofile, new_vcf_path)

    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
