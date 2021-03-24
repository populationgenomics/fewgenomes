"""Copies PCR-free validation data from the upload bucket to the test bucket."""

import os
import subprocess

output = os.getenv('OUTPUT')
assert output and output.startswith('gs://cpg-fewgenomes-test/')

subprocess.run(
    ['gsutil', 'mv', 'gs://cpg-fewgenomes-upload/cas-simons/*', output]
)

subprocess.run(['cat', f'{output}/NA12878-HIGH.bam.md5sum'])

subprocess.run(
    f'gsutil cat {output}/NA12878-HIGH.bam | md5sum', shell=True
)
