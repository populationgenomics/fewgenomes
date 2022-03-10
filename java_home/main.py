#!/usr/bin/env python3

import hail as hl
import os
import subprocess

subprocess.run(['hailctl', 'config', 'set', 'batch/billing_project', os.getenv('HAIL_BILLING_PROJECT')], check=True)
hail_bucket = os.getenv('HAIL_BUCKET')
subprocess.run(['hailctl', 'config', 'set', 'batch/remote_tmpdir', f'gs://{hail_bucket}/batch-tmp'], check=True)

hl.init(default_reference='GRCh38')
hl.import_table('test.csv')

