#!/usr/bin/env python3

"""
wraps a functional batch submission, submitting using a container runtime which
includes the cpg_pipes package, used to set up the runtime configurations
"""

import os
import hailtop.batch as hb

import click


AR_REPO = 'australia-southeast1-docker.pkg.dev/cpg-common/images'
CPG_PIPES_TAG = 'cpg-pipes:0.2.8'
CPG_PIPES_IMG = f'{AR_REPO}/{CPG_PIPES_TAG}'


@click.command()
@click.option(
    '--script',
    'script',
    help='script to submit, with arguments added in other params'
)
@click.option(
    '--vcf',
    'vcf',
    help='file to run the command on'
)
@click.option(
    '--project',
    'project',
    help='project to set up the batch within'
)
def main(script: str, vcf: str, project: str):
    """
    Create a Batch using the cpg_pipes image, and run a task within it
    """

    service_backend = hb.ServiceBackend(
        billing_project=os.getenv('HAIL_BILLING_PROJECT'),
        bucket=os.getenv('HAIL_BUCKET'),
    )

    # create a hail batch
    batch = hb.Batch(
        name='run_slivar_wrapper',
        backend=service_backend
    )

    job = batch.new_job(name='run slivar_wrapper')

    # really minimal configuration, as this will create a new, appropriately resourced batch
    job.cpu(1)
    job.memory('standard')  # ~ 4G/core ~ 7.5G
    job.storage('20G')
    job.command(f'python3 {script} --vcf {vcf} --project {project}')
    job.image(CPG_PIPES_IMG)
    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
