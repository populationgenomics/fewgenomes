#!/usr/bin/env python3


"""
wrapper to create a batch using the samtools image
runs tabix on one file
"""


import os
import hailtop.batch as hb

import click

from analysis_runner import dataproc


AR_REPO = 'australia-southeast1-docker.pkg.dev/cpg-common/images'
BCFTOOLS_IMAGE = f'{AR_REPO}/bcftools:1.10.2--h4f4756c_2'


@click.command()
@click.option(
    '--file',
    'file',
    help='file to run the command on'
)
def main(file: str):
    """
    Create a Hail Batch, and run a tabix task within a job
    """

    service_backend = hb.ServiceBackend(
        billing_project=os.getenv('HAIL_BILLING_PROJECT'),
        bucket=os.getenv('HAIL_BUCKET'),
    )

    # create a hail batch
    batch = hb.Batch(
        name='run_tabix',
        backend=service_backend
    )

    my_job = dataproc.hail_dataproc_job(
        batch=batch,
        script=f'tabix {file}',
        max_age='1h',
        job_name='run_tabix',
        num_secondary_workers=1,
        cluster_name='run_tabix with max-age=1h',
    )  # noqa: F841

    my_job.image(BCFTOOLS_IMAGE)

    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
