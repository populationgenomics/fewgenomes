#!/usr/bin/env python3


"""
wrapper to create a batch using the samtools image
runs tabix on one file
"""


import os
import hailtop.batch as hb

import click


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

    job = batch.new_job(name='run tabix')
    job.declare_resource_group(
        vcf={'vcf': '{root}.vcf.gz', 'index': '{root}.vcf.gz.tbi'}
    )
    in_temp = batch.read_input(file)
    job.command(f'cat {in_temp} > {job.vcf.vcf}')
    job.command(f'tabix {job.vcf.vcf}')
    batch.write_output(job.vcf, os.path.join(os.getenv('HAIL_BUCKET', ''), 'tabix'))
    job.image(BCFTOOLS_IMAGE)

    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
