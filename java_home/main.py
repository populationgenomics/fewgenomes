#!/usr/bin/env python3

import hail as hl
import os

print(f'HAIL_QUERY_BACKEND={os.getenv("HAIL_QUERY_BACKEND")}')

hl.init(default_reference='GRCh38')
hl.import_table('test.csv')

