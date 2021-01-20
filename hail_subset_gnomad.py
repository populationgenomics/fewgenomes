import os
import hail as hl
import pandas as pd
import click

mt_src_path = \
    'gs://gcp-public-data--gnomad/release/3.1/mt/genomes/' \
    'gnomad.genomes.v3.1.hgdp_1kg_subset_dense.mt/'

@click.command()
@click.argument('ped_path')
def main(ped_path):
    df = pd.read_csv(ped_path, sep='\t')
    sample_names = list(df['Individual.ID'])

    mt_subset_path = os.path.join(os.path.dirname(ped_path), 'gnomad.subset.mt')
    hl.init(default_reference='GRCh38')
    mt = hl.read_matrix_table(mt_src_path)
    mt = mt.filter_cols(hl.literal(sample_names).contains(mt['s']))
    mt = mt.drop(*[k for k in mt.globals.dtype.keys()])
    mt = mt.drop(*[k for k in mt.col.dtype.keys() if k != 's'])
    mt = mt.drop(*[k for k in mt.row.dtype.keys() if k not in ['s', 'locus', 'alleles']])

    mt.write(mt_subset_path, overwrite=True)
