#!/usr/bin/env python3


"""
I've tried to use every possible way of using VEP
without using hail, but it's not simple.
Hell I'm not sure if it's possible.

Instead I'm just going to use... Hail
"""


import os
import hailtop.batch as hb
from analysis_runner import dataproc
import click


@click.command()
@click.option(
    '--matrix',
    'matrix',
    help='mt to interrogate'
)
@click.option(
    '--conf',
    'conf',
    help='location of a settings json'
)
@click.option(
    '--script',
    'script',
    help='path to script to run inside dataproc instance'
)
def main(matrix: str, conf: str, script: str):
    """
    submit reanalysis script using dataproc
    :param matrix: str, path to a MatrixTable
    :param conf: str, path to a json file configuring the analysis
    :param script: str, path to a script to run inside the dataproc
    """

    service_backend = hb.ServiceBackend(
        billing_project=os.getenv('HAIL_BILLING_PROJECT'),
        bucket=os.getenv('HAIL_BUCKET'),
    )

    # create a hail batch
    batch = hb.Batch(
        name='run_analysis',
        backend=service_backend
    )

    _job = dataproc.hail_dataproc_job(
        batch=batch,
        script=f'{script} --conf {conf} --matrix {matrix}',
        max_age='4h',
        job_name='annotate_vcf',
        num_secondary_workers=4,
        cluster_name='annotate_vcf with max-age=4h',
        vep='GRCh38'
    )  # noqa: F841

    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
