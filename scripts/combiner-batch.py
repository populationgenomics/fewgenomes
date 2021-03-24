"""Demonstrates the use of the dataproc module."""

import os
import hail as hl
import hailtop.batch as hb
import subprocess
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
    f'run-python-script.py combine_gvcfs.py \
--sample-map gs://cpg-fewgenomes-temporary/tmp/sample-map-2.csv \
--out-mt gs://cpg-fewgenomes-temporary/test-dataproc-package/2genomes.mt \
--bucket gs://cpg-fewgenomes-temporary/work/vcf-combiner/test-dataproc-package/ \
--local-tmp-dir tmp \
--hail-billing fewgenomes',
    max_age='8h',
    packages=['joint-calling', 'click', 'cpg-gnomad', 'google', 'slackclient', 'fsspec', 'sklearn', 'gcloud'],
    init=['gs://cpg-reference/hail_dataproc/install_phantomjs.sh'],
    num_secondary_workers=10,
)

batch.run()
