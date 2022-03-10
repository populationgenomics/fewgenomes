#!/usr/bin/env python3

import hail as hl
import os
import subprocess

subprocess.run(['hailctl', 'config', 'set', 'batch/billing_project', os.getenv('HAIL_BILLING_PROJECT')], check=True)
subprocess.run(['hailctl', 'config', 'set', 'batch/remote_tmpdir', os.getenv('HAIL_BUCKET')], check=True)

hl.init(default_reference='GRCh38')
hl.import_table('test.csv')

