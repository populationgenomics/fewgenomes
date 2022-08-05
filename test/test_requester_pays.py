#!/usr/bin/env python3
import os
from shlex import quote

from cpg_utils.hail_batch import copy_common_env, get_config, remote_tmpdir
import hailtop.batch as hb

config = get_config()

HAIL_PROJECT = 'hail-295901'
DRIVER_IMAGE = config.get('workflow', {}).get(
    'driver_image',
    'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:646dcc2ab7e484314943b979d2132b0133dbbe94-hail-b28ed8c6a7f8dadc9275a87064b5ccfe562238c3',
)
GCLOUD_INIT = 'gcloud -q auth activate-service-account --key-file=/gsa-key/key.json'

requester_pays_base = 'gs://cpg-mfranklin-requester-pays'
requester_pays_file = os.path.join(requester_pays_base, 'hello.txt')

JOBS_COPY_IN = 'copy-in'
JOBS_COPY_OUT = 'copy-out'
JOBS_STREAM_IN = 'stream-in'
JOBS_STREAM_OUT = 'stream-out'
JOBS_HAIL_QUERY = 'hail-query'

jobs = [
    # JOBS_COPY_IN,
    # JOBS_COPY_OUT,
    # JOBS_STREAM_IN,
    # JOBS_STREAM_OUT,
    JOBS_HAIL_QUERY,
]


# sb = hb.ServiceBackend(
#     billing_project='michaelfranklin-trial', remote_tmpdir='gs://cpg-michael-hail-dev/tmp/'
# )
sb = hb.ServiceBackend(
    billing_project=config["hail"]["billing_project"],
    remote_tmpdir=remote_tmpdir(),
)
batch = hb.Batch(
    'requester-pays-tests', backend=sb, requester_pays_project=HAIL_PROJECT
)

if JOBS_COPY_IN in jobs:
    j_copy_in = batch.new_job(JOBS_COPY_IN)
    inp = batch.read_input(requester_pays_file)
    j_copy_in.command(f'cat {inp}')

if JOBS_COPY_OUT in jobs:
    j_copy_out = batch.new_job('copy-out')
    copy_out_file = j_copy_out.outfile
    j_copy_out.command(f'echo "Copy out" > {copy_out_file}')
    batch.write_output(
        copy_out_file, os.path.join(requester_pays_base, 'copy-output.txt')
    )

if JOBS_STREAM_IN in jobs:
    j_stream_in = batch.new_job(JOBS_STREAM_IN)
    j_stream_in.image(DRIVER_IMAGE)
    j_stream_in.command(GCLOUD_INIT)
    j_stream_in.command(f'gsutil cat {quote(requester_pays_file)}')

if JOBS_STREAM_OUT in jobs:
    j_stream_out = batch.new_job('stream-out')
    j_stream_out.image(DRIVER_IMAGE)
    j_stream_out.command(GCLOUD_INIT)
    stream_out_path = os.path.join(requester_pays_base, "stream-out")
    j_stream_out.command(f'echo "Stream out" | gsutil cp - {quote(stream_out_path)}')


if JOBS_HAIL_QUERY in jobs:
    j_hq = batch.new_job(JOBS_HAIL_QUERY)
    j_hq.image(DRIVER_IMAGE)
    j_hq.command(GCLOUD_INIT)
    copy_common_env(j_hq)
    j_hq.command(
        f"""
cat > script.py <<EOF 
import hail as hl

hl.init_batch(
    default_reference="GRCh38",
    billing_project=get_config()["hail"]["billing_project"],
    remote_tmpdir=remote_tmpdir(),
)

mt = hl.read_matrix_table('gs://cpg-fewgenomes-test/mt')

mt.show()
EOF

python script.py
"""
    )


if len(batch._jobs) == 0:
    raise ValueError('No jobs to run')

batch.run(wait=False)
