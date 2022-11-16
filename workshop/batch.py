#!/usr/bin/env python3


"""
blah
"""


import hailtop.batch as hb
from cpg_utils.hail_batch import get_config, remote_tmpdir

config = get_config()

sb = hb.ServiceBackend(
    billing_project=config['hail']['billing_project'],
    remote_tmpdir=remote_tmpdir(),
)

# HERE
NAME = 'ME'
b = hb.Batch(name=f'{NAME}: Hello Batch', backend=sb)
job = b.new_bash_job('Print my name')
job.command(f'echo "Hello, {NAME}!"')

b.run(wait=False)
