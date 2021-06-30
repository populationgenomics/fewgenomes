#!/usr/bin/env python

"""
Hail Batch workflow to perform joint calling, sample QC, and variant QC with VQSR and 
random forest methods on a WGS germline callset.

The workflow is parametrised by the access level, the dataset name, 
batch names and the output version.

It must be only run with the CPG analysis-runner:
https://github.com/populationgenomics/analysis-runner (see helper script `driver_for_analysis_runner.sh` for analysis-runner submissions)
"""

import os
from os.path import join, dirname, abspath
import logging
import hailtop.batch as hb
from analysis_runner import dataproc

logger = logging.getLogger('joint-calling')
logger.setLevel('INFO')

DEFAULT_REF = 'GRCh38'

DATAPROC_PACKAGES = [
    'joint-calling',
    'click',
    'cpg-gnomad',
    'google',
    'slackclient',
    'fsspec',
    'sklearn',
    'gcloud',
]


def main():  # pylint: disable=too-many-arguments,too-many-locals,too-many-statements
    """
    Drive a Hail Batch workflow that creates and submits jobs. A job usually runs
    either a Hail Query script from the scripts folder in this repo using a Dataproc
    cluster; or a GATK command using the GATK or Gnarly image.
    """
    billing_project = os.getenv('HAIL_BILLING_PROJECT')
    hail_bucket = os.environ.get('HAIL_BUCKET')
    logger.info(
        f'Starting hail Batch with the project {billing_project}, '
        f'bucket {hail_bucket}'
    )
    backend = hb.ServiceBackend(
        billing_project=billing_project,
        bucket=hail_bucket.replace('gs://', ''),
    )
    b = hb.Batch(
        f'Test mt2vcf',
        backend=backend,
    )

    mt_path = 'gs://gcp-public-data--gnomad/release/3.1/mt/genomes/gnomad.genomes.v3.1.hgdp_1kg_subset_dense.mt'
    combined_vcf_path = (
        'gs://cpg-fewgenomes-test-tmp/joint-calling/v2/variant_qc/vqsr/input-test.vcf.gz'
    )
    dataproc.hail_dataproc_job(
        b,
        f'mt_to_vcf.py --overwrite '
        f'--mt {mt_path} '
        f'-o {combined_vcf_path} ',
        max_age='8h',
        packages=DATAPROC_PACKAGES,
        num_secondary_workers=50,
        secondary_worker_boot_disk_size=15,
        job_name='MT to VCF',
    )

    b.run()


if __name__ == '__main__':
    main()  # pylint: disable=E1120
