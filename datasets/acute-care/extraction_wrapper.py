#!/usr/bin/env python3


"""
Wrapper script to initiate a hail batch, and submit using an
analysis-runner managed dataproc cluster

A consequence of this wrapping is that the json-str argument needs shlex
escaping to be passed successfully as a CLI argument to the pyspark process

e.g.

--json-str '{"FAM1":["P1"]}'
becomes
--json-str '\'{"FAM1":["P1"]}\''
"""


import os
import sys
import hailtop.batch as hb

from analysis_runner import dataproc


def main():
    """
    Create a Hail Batch
    analysis-runner helper creates a DataProc cluster, add the job
    Set off the batch
    """

    service_backend = hb.ServiceBackend(
        billing_project=os.getenv('HAIL_BILLING_PROJECT'),
        bucket=os.getenv('HAIL_BUCKET'),
    )

    # create a hail batch
    batch = hb.Batch(
        name='cohort_mt_extraction',
        backend=service_backend
    )

    _my_job = dataproc.hail_dataproc_job(
        batch=batch,
        script=' '.join(sys.argv[1:]),
        max_age='4h',
        job_name='extract_from_cohort_mt',
        num_secondary_workers=4,
        cluster_name='cohort_mt_extraction with max-age=4h',
    )  # noqa: F841

    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
