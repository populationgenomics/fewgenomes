#!/usr/bin/env python3


"""
pass variables to dataproc, reduce the MT footprint
"""


import os
import hailtop.batch as hb
from analysis_runner import dataproc
import click


@click.command()
@click.option(
    '--matrix_in',
    'matrix_in',
    help='mt to interrogate'
)
@click.option(
    '--matrix_out',
    'matrix_out',
    help='mt to write'
)
@click.option(
    '--script',
    'script',
    help='path to script to run inside dataproc instance'
)
def main(matrix_in: str, matrix_out: str, script: str):
    """
    submit reanalysis script using dataproc
    :param matrix_in: str, path to a MatrixTable
    :param matrix_out: str, path to a json file configuring the analysis
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
        script=f'{script} --matrix_in {matrix_in} --matrix_out {matrix_out}',
        max_age='4h',
        job_name='reduce_phat_mt',
        num_secondary_workers=4,
        cluster_name='reduce_phat_mt with max-age=4h',
        vep='GRCh38'
    )  # noqa: F841

    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
