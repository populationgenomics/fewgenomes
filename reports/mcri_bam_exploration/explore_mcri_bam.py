#!/usr/bin/env python

import os
import hailtop.batch as hb


output_bucket = os.getenv('OUTPUT')
assert output_bucket and output_bucket.startswith('gs://cpg-fewgenomes-temporary/')
SAMTOOLS = 'quay.io/biocontainers/samtools:1.12--h9aed4be_1'
BAM = 'gs://cpg-fewgenomes-test/mcri_pcr_free_validation/NA12878-HIGH.bam'

service_backend = hb.ServiceBackend(
    billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=os.getenv('HAIL_BUCKET')
)
b = hb.Batch(backend=service_backend, name='explore-mcri-bam')
j = b.new_job(name=f'samtools idxstats')
(j.image(SAMTOOLS)
  .command(f'samtools idxstats {BAM} > {j.ofile}'))
b.write_output(j.ofile, f'{output_bucket}/explore-mcri-bam/samtools_idxstats.txt')

b.run()
