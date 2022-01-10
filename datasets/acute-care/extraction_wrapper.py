#!/usr/bin/env python3

"""
Wrapper script to initiate a hail batch, and submit using an analysis-runner managed dataproc cluster
"""
import hailtop.batch as hb
import os
import sys

from analysis_runner import dataproc


def main():
    """
    Create a Hail Batch
    Use the analysis-runner helper to generate a DataProc cluster, and add the job
    Set off the batch
    """

    service_backend = hb.ServiceBackend(
        billing_project=os.getenv("HAIL_BILLING_PROJECT"),
        bucket=os.getenv("HAIL_BUCKET"),
    )

    # create a hail batch
    batch = hb.Batch(name="cohort_mt_extraction", backend=service_backend)

    my_job = dataproc.hail_dataproc_job(
        batch=batch,
        script=" ".join(sys.argv[1:]),
        max_age="4h",
        job_name="extract_from_cohort_mt",
        num_secondary_workers=4,
        cluster_name="cohort_mt_extraction with max-age=4h",
    )

    batch.run(wait=False)


if __name__ == "__main__":
    main()
