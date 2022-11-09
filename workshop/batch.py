#!/usr/bin/env python3
import hailtop.batch as hb
from cpg_utils.hail_batch import get_config, remote_tmpdir

config = get_config()

sb = hb.ServiceBackend(
    billing_project=config['hail']['billing_project'],
    remote_tmpdir=remote_tmpdir(),
)

name = 'YOURNAME'
b = hb.Batch(name=f'{name}: Hello Batch', backend=sb)

job = b.new_bash_job('Print my name')
job_output_1 = job.my_tdout
job.command(f'echo "Hello, {name}!" > {job_output_1}')

job2 = b.new_bash_job('Print contents of job1')
job2.command(f'cat {job_output_1}')

b.run(wait=False)
