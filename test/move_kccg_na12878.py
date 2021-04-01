"""Moves KCCG NA12878 data from the upload bucket to the test bucket."""

import os
import subprocess

output = os.getenv('OUTPUT')
assert output and output.startswith('gs://cpg-fewgenomes-test/')

subprocess.run(
    ['gsutil', 'mv', 'gs://cpg-fewgenomes-upload/kccg/*', output]
)
