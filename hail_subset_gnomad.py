import os
import hail as hl
import pandas as pd
import click
import random

mt_src_path = \
    'gs://gcp-public-data--gnomad/release/3.1/mt/genomes/' \
    'gnomad.genomes.v3.1.hgdp_1kg_subset_dense.mt/'

@click.command()
@click.option('--trg-path', 'trg_path', required=True)
@click.option('--ped-path', 'ped_path')
@click.option('-n', 'n', type=click.INT, default=50)
@click.option('--clean', 'clean', type=click.BOOL, default=False)
def main(trg_path, ped_path, n, clean):
    assert ped_path or n

    hl.init(default_reference='GRCh38')
    mt = hl.read_matrix_table(mt_src_path)

    if ped_path:
        df = pd.read_csv(ped_path, sep='\t')
        sample_names = list(df['Individual.ID'])
        mt = mt.filter_cols(hl.literal(sample_names).contains(mt['s']))
    else:
        indices = list(range(mt.count_cols()))
        random.shuffle(indices)
        mt = mt.choose_cols(list(range(n)))

    if clean:
        mt = mt.drop(*[k for k in mt.globals.dtype.keys()])
        mt = mt.drop(*[k for k in mt.col.dtype.keys() if k != 's'])
        mt = mt.drop(*[k for k in mt.row.dtype.keys() if k not in ['s', 'locus', 'alleles']])

    mt.write(trg_path, overwrite=True)

main()