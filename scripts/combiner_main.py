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
    (f'combine_gvcfs.py --sample-map gs://cpg-fewgenomes-temporary/joint-calling/50genomes-gcs-au-round1.csv '
 	 f'--out-mt gs://cpg-fewgenomes-temporary/test-dataproc-package/50genomes.mt '
 	 f'--bucket gs://cpg-fewgenomes-temporary/work/vcf-combiner/test-dataproc-package/ '
 	 f'--local-tmp-dir tmp'),
    max_age='1h',
    packages=['click', 'selenium'],
    init=[
        'gs://cpg-reference/hail_dataproc/install_phantomjs.sh',
        'gs://cpg-reference/hail_dataproc/install_joint_calling.sh',
    ],
)

batch.run()

