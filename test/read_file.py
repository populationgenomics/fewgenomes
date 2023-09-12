import hail as hl

hl.init(default_reference='GRCh38')

COVERAGE_FILE = "gs://gcp-public-data--gnomad/release/3.0.1/coverage/genomes/gnomad.genomes.r3.0.1.coverage.ht"
ht = hl.read_table(COVERAGE_FILE)

ht.head()
