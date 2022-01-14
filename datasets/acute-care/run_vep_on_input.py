#!/usr/bin/env python3


"""
execute a subprocess call in python to run VEP annotation
"""


import subprocess
import click


@click.option(
    '--infile',
    'infile',
    help='file to annotate'
)
@click.option(
    '--outfile',
    'outfile',
    help='file to produce'
)
def main(infile: str, outfile: str):
    """
    takes an input VCF, runs VEP on it, creates an output
    :param infile: str, the GCP path for a given input VCF
    :param outfile: str, the path to the VEP annotated output VCF
    """

    complete_cmd_string = f'/vep --format vcf -i {infile} ' \
                          f'--everything --allele_number --no_stats ' \
                          f'--cache --offline --minimal --assembly ' \
                          f'GRCh38 --vcf -o {outfile}'

    subprocess.check_output(complete_cmd_string.split())


if __name__ == '__main__':
    main()  # pylint: disable=E1120
