"""Copies PCR-free validation data from the upload bucket to the test bucket."""

import os
import subprocess

output = os.getenv('OUTPUT')
assert output and output.startswith('gs://cpg-fewgenomes-test/')

subprocess.check_output(
    ['gsutil', 'mv', 'gs://cpg-fewgenomes-upload/cas-simons', output]
)
