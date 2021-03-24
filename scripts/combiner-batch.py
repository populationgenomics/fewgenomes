"""Demonstrates the use of the dataproc module."""

import os
import hail as hl
import hailtop.batch as hb
from analysis_runner import dataproc

OUTPUT = os.getenv('OUTPUT')
assert OUTPUT

hl.init(default_reference='GRCh38')

service_backend = hb.ServiceBackend(
    billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=os.getenv('HAIL_BUCKET')
)

batch = hb.Batch(name='dataproc example', backend=service_backend)

dataproc.hail_dataproc_job(
    batch,
    'combiner-on-dataproc.sh'
    max_age='8h',
    packages=['joint-calling', 'click', 'cpg-gnomad', 'google', 'slackclient', 'fsspec', 'sklearn', 'gcloud'],
    init=['gs://cpg-reference/hail_dataproc/install_phantomjs.sh'],
    num_secondary_workers=10,
)

batch.run()
