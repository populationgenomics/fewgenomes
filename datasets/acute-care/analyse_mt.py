#!/usr/bin/env python3


"""
Loads in the MatrixTable, and prints out a description of the cols

Sample Genotype is annotated at the Entry level 'GT'
Only interested in genes with vep.transcript_consequences.gene_id
perhaps a filter for 'not present' or ''
A threshold

Filter syntax example
mt = mt.filter_rows(dataset.variant_qc.AF[1] < 0.01, keep=True)
"""


import logging
from typing import Optional
import hail as hl
import click


def go_and_get_mt(mt_path: str) -> hl.MatrixTable:
    """
    Reads in the stored MatrixTable from disk
    :param mt_path: str, path to a MT directory
    """

    annotated_mt = hl.read_matrix_table(mt_path)
    return annotated_mt


def remove_non_genic_variants(
        matrix: hl.MatrixTable,
        keep_genes: Optional[set] = None
) -> hl.MatrixTable:
    """
    either remove all variants without gene annotations
    :param matrix: hl.MatrixTable
    :param keep_genes: Optional[set]
    """

    logging.info('Number of variants prior to filtering: %d', matrix.count_rows())

    if keep_genes is None:
        keep_genes = set()

    # if we specified some gene IDs, only keep those variants
    if len(keep_genes) > 0:
        keep_genes = hl.literal(keep_genes)
        filtered_matrix = matrix.filter_rows(
            hl.len(matrix.geneIds & keep_genes) > 0
        )
    # otherwise only keep rows relating to any gene ID
    else:
        filtered_matrix = matrix.filter_rows(
            hl.len(matrix.geneIds) > 0
        )

    logging.info('Number of variants prior to filtering: %d', filtered_matrix.count_rows())
    return filtered_matrix


@click.command()
@click.option(
    '--matrix',
    'matrix',
    help='mt to interrogate'
)
@click.option(
    '--conf',
    'conf',
    help='location of a settings json'
)
@click.option(
    '--ref',
    'reference',
    help='genomic reference for hail load',
    default='GRCh38'
)
def main(matrix: str, conf: str, reference: str):
    """
    :param matrix: str, path to a MatrixTable
    :param conf: str, path to a json file configuring the analysis
    :param reference: str, path to a script to run inside the dataproc
    """

    # boot up hail with the relevant reference
    hl.init(default_reference=reference)

    logging.info('Config path: %s', conf)
    annotated_mt = go_and_get_mt(mt_path=matrix)

    remove_non_genic_variants(matrix=annotated_mt)

    print(annotated_mt.describe())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()  # pylint: disable=E1120
