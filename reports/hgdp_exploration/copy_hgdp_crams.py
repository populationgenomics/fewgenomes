"""
Copies HGDP CRAM files from 'gs://ibd-external-datasets'
to the fewgenomes main bucket.
"""

import os
import csv
import subprocess
import hailtop.batch as hb

# OUTPUT gets propagated from the analysis-runner cli to the server
output_bucket = os.getenv('OUTPUT')
assert output_bucket and output_bucket.startswith('gs://cpg-fewgenomes-temporary/')

input_filelist = './data/filtered65.csv'

def copy_to_bucket(bucket: str, batch: hb.batch.Batch, sample_name: str, ftype: str, fname: str) -> None:
    """
    :param bucket: GCS bucket to copy to
    :param batch: hailtop Batch object
    :param sample_name: name of sample
    :param ftype: file type (cram or index)
    :param fname: file name to copy (full GCS path)
    """
    j = batch.new_job(name=f'{sample_name}-{ftype}')
    # j.command(f'echo "copy {ftype} file {fname} for {sample_name}!" > {j.ofile}')
    # batch.write_output(j.ofile, f'gs://cpg-peter-dev/batch_runs/test/{sample_name}_{ftype}.txt')
    print('which gcloud?')
    j.command(f'which gcloud')
    print('which gsutil?')
    j.command(f'which gsutil')
    #j.command(f'gsutil cp {fname} {bucket}')

service_backend = hb.ServiceBackend(
    billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=os.getenv('HAIL_BUCKET')
)
#backend = hb.ServiceBackend('peterdiakumis-trial', 'cpg-peter-dev')
b = hb.Batch(backend=service_backend, name='test-cram-copying')

with open(input_filelist, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    line_count = 0
    for row in reader:
        if line_count < 6:
            copy_to_bucket(output_bucket, b, row['sample_name'], row['ftype'], row['fname'])
            line_count += 1

b.run()
