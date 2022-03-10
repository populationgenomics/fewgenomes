#!/usr/bin/env python3

import hail as hl

hl.init(default_reference='GRCh38')
hl.import_table('test.csv')

