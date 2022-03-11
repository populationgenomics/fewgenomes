#!/usr/bin/env python3

import hail as hl
import os
import subprocess

subprocess.run(['hailctl', 'config', 'set', 'batch/billing_project', os.getenv('HAIL_BILLING_PROJECT')], check=True)
hail_bucket = os.getenv('HAIL_BUCKET')
subprocess.run(['hailctl', 'config', 'set', 'batch/remote_tmpdir', f'gs://{hail_bucket}/batch-tmp'], check=True)

hl.init(default_reference='GRCh38')
t = hl.import_table('gs://cpg-fewgenomes-test/benchmark/outputs/NA12340/duplicate-metrics/NA12340-duplicate-metrics.csv')
t.describe()

