#!/usr/bin/env python3

import asyncio
import hail as hl
import os

def init_hail_query_service():
    billing_project = os.getenv('HAIL_BILLING_PROJECT')
    hail_bucket = os.getenv('HAIL_BUCKET')
    asyncio.get_event_loop().run_until_complete(hl.init_service(default_reference='GRCh38', billing_project=billing_project, remote_tmpdir=f'gs://{hail_bucket}/batch-tmp'))

init_hail_query_service()
t = hl.import_table('gs://cpg-fewgenomes-test/gatk_sv/output/GatherSampleEvidence/NA12878-2/GatherSampleEvidence/1ca67ed3-4220-4e28-bc3d-9e544a6d15c8/call-GatherSampleEvidenceMetrics/GatherSampleEvidenceMetrics/f9124086-2ea0-413e-9bc8-a5404bb6f708/call-CountsMetrics/NA12878-2.raw-counts.tsv')
t.describe()
t.show()
pd = t.to_pandas()
t2 = pd.from_pandas()
t2.describe()
t2.show()

