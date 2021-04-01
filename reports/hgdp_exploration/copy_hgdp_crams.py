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
analysis_runner_image = 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:45c3f8125e300cd70bb790e32d96816f003a7af2-hail-0.2.64.devcb1c44c7b529'

def copy_to_bucket(bucket: str, batch: hb.batch.Batch, sample_name: str, ftype: str, fname: str) -> None:
    """
    :param bucket: GCS bucket to copy to
    :param batch: hailtop Batch object
    :param sample_name: name of sample
    :param ftype: file type (cram or index)
    :param fname: file name to copy (full GCS path)
    """
    j = batch.new_job(name=f'copy-{sample_name}-{ftype}')
    j.command(f'gsutil cp {fname} {bucket}')

service_backend = hb.ServiceBackend(
    billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=os.getenv('HAIL_BUCKET')
)
b = hb.Batch(backend=service_backend, name='test-cram-copying')
j_auth = b.new_job(name='authorise service account access')
(j_auth.image(analysis_runner_image)
       .command(f'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json'))

# Copy CRAMs and indexes listed in CSV to bucket
with open(input_filelist, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    line_count = 0
    for row in reader:
        if line_count < 4:
            copy_to_bucket(output_bucket, b, row['sample_name'], row['ftype'], row['fname'])
            line_count += 1

b.run()
