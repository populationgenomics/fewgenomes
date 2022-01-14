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
    '--script',
    'script',
    help='path to vep python script'
)
def main(script: str):
    """
    runs a small script inside dataproc to see if VEP exists/is usable
    :param script: str, the path to the VEP script
    """

    service_backend = hb.ServiceBackend(
        billing_project=os.getenv('HAIL_BILLING_PROJECT'),
        bucket=os.getenv('HAIL_BUCKET'),
    )

    # create a hail batch
    batch = hb.Batch(
        name='run_vep_in_dataproc_cluster',
        backend=service_backend
    )

    job = dataproc.hail_dataproc_job(
        batch=batch,
        script=script,
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
