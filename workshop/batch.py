#!/usr/bin/env python3
import hailtop.batch as hb
from cpg_utils.hail_batch import get_config, remote_tmpdir

config = get_config()

sb = hb.ServiceBackend(
    billing_project=config['hail']['billing_project'],
    remote_tmpdir=remote_tmpdir(),
)

name = 'Michael_I'
b = hb.Batch(name=f'{name}: Hello Batch', backend=sb)

# your code here
job = b.new_bash_job('Print my name')
job.command(f'Hello "{name} ;)"')

b.run(wait=False)
