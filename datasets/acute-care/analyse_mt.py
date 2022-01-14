#!/usr/bin/env python3


"""
Loads in the MatrixTable, and prints out a description of the cols
"""


import logging
import hail as hl
import click


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
    hl.init(default_reference=reference)

    logging.info('Config path: %s', conf)
    annotated_mt = hl.read_matrix_table(matrix)
    print(annotated_mt.col.describe())
    print(annotated_mt.describe())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()  # pylint: disable=E1120
