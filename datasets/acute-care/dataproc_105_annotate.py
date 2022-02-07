#!/usr/bin/env python3


"""
Run VEP on the hail mt
"""


import click
import hail as hl


@click.command()
@click.option('--matrix', required=True, help='Hail matrix table to run VEP on')
@click.option('--output', required=True, help='Hail matrix table to write out')
def main(matrix: str, output: str):
    """
    Run vep using main.py wrapper
    :param matrix: input path
    :param output: output path
    """

    hl.init(default_reference='GRCh38')
    matrix_data = hl.read_matrix_table(matrix)

    # hard filter for quality and abundance in the joint call
    matrix_data = matrix_data.filter_rows(matrix_data.info.AC <= 20)
    matrix_data = matrix_data.filter_rows(matrix_data.filters.length() == 0)

    # filter to biallelic loci only
    matrix_data = matrix_data.filter_rows(hl.len(matrix_data.alleles) == 2)
    matrix_data = matrix_data.filter_rows(matrix_data.alleles[1] != '*')
    vep = hl.vep(matrix_data, config='file:///vep_data/vep-gcloud.json')
    vep.write(output)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
