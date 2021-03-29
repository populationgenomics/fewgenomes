"""
Copies HGDP CRAM files from 'gs://ibd-external-datasets'
to the fewgenomes main bucket.
"""

# import os
import csv
import hailtop.batch as hb

# output = os.getenv('OUTPUT')
input_filelist = './data/filtered65.csv'

def copy_to_bucket(b, sample_name, ftype, fname):
    j = b.new_job(name=f'{sample_name}-{ftype}')
    j.command(f'echo "copy {ftype} file {fname} for {sample_name}!" > {j.ofile}')
    b.write_output(j.ofile, f'gs://cpg-peter-dev/batch_runs/test/{sample_name}_{ftype}.txt')

backend = hb.ServiceBackend('peterdiakumis-trial', 'cpg-peter-dev')
b = hb.Batch(backend=backend, name='test-scatter')

with open(input_filelist, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        copy_to_bucket(b, row["sample_name"], row["ftype"], row["fname"])

b.run()