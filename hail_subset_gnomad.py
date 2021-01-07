import random
import hail as hl
import pandas as pd

hl.init(default_reference="GRCh38")

N = 50
src_path = "gs://gcp-public-data--gnomad/release/3.1/mt/genomes/gnomad.genomes.v3.1.hgdp_1kg_subset_dense.mt/"
trg_path = "gs://playground-us-central1/cpg-fewgenomes/subset_from_gnomad.mt"
# gsutil cp datasets/50genomes/samples.ped gs://playground-us-central1/cpg-fewgenomes/samples.ped
samples_csv_fpath = "gs://playground-us-central1/cpg-fewgenomes/samples.ped"
df = pd.read_csv(samples_csv_fpath, sep='\t')
samples = df['Individual.ID']

mt = hl.read_matrix_table(src_path)
mt = mt.filter_cols(hl.literal(list(samples)).contains(mt['s']))
mt = mt.drop(*[k for k in mt.globals.dtype.keys()])
mt = mt.drop(*[k for k in mt.col.dtype.keys() if k != 's'])
mt = mt.drop(*[k for k in mt.row.dtype.keys() if k not in ['s', 'locus', 'alleles']])

mt.write(trg_path, overwrite=True)
