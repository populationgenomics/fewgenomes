"""
Copies HGDP CRAM and index files listed in a CSV file
to the gs://cpg-fewgenomes-main bucket.
"""

import os
import csv
import hailtop.batch as hb

# OUTPUT gets propagated from the analysis-runner cli to the server
output_bucket = os.getenv('OUTPUT')
assert output_bucket and output_bucket.startswith('gs://cpg-fewgenomes-main/')

# input CSV contains 3 columns with sample name, file type, full GCS file path
INPUT_FILELIST = './data/filtered65.csv'
ANALYSIS_RUNNER_IMAGE = 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:45c3f8125e300cd70bb790e32d96816f003a7af2-hail-0.2.64.devcb1c44c7b529'

service_backend = hb.ServiceBackend(
    billing_project=os.getenv('HAIL_BILLING_PROJECT'), bucket=os.getenv('HAIL_BUCKET')
)
b = hb.Batch(backend=service_backend, name='copy-crams')

# Create Hail Batch jobs to copy CRAMs and indexes listed in CSV file to output bucket
with open(INPUT_FILELIST, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        j_copy = b.new_job(name=f'copy-{row["sample_name"]}-{row["ftype"]}')
        (j_copy.image(ANALYSIS_RUNNER_IMAGE)
               .command(f'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json')
               .command(f'gsutil cp {row["fname"]} {output_bucket}'))

b.run()
