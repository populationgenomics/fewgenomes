import random
import hail as hl

hl.init(default_reference="GRCh38")

N = 50
src_path = "gs://gcp-public-data--gnomad/release/3.1/mt/genomes/gnomad.genomes.v3.1.hgdp_1kg_subset_dense.mt/"
trg_path = "gs://playground-us-central1/mt-gnomad-subset/cpg-fewgenomes.mt"

mt = hl.read_matrix_table(src_path)
mt = mt.drop(*[k for k in mt.globals.dtype.keys()])
mt = mt.drop(*[k for k in mt.col.dtype.keys() if k != 's'])
mt = mt.drop(*[k for k in mt.row.dtype.keys() if k not in ['s', 'locus', 'alleles']])

indices = list(range(mt.count_cols()))
random.shuffle(indices)
mt_subset = mt.choose_cols(list(range(N)))
mt_subset.write(trg_path, overwrite=True)
