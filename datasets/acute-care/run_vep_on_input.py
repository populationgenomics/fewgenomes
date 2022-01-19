#!/usr/bin/env python3


"""
execute a subprocess call in python to run VEP annotation
write the output to a location defined using output_path
"""


import subprocess
import click

from analysis_runner import output_path


@click.command()
@click.option(
    '--infile',
    'infile',
    help='file to annotate'
)
def main(infile: str):
    """
    takes an input VCF, runs VEP on it, creates an output
    THIS WILL NOT WORK, as VEP is installed but can't resolve the VCF path
    :param infile: str, the GCP path for a given input VCF
    """

    complete_cmd_string = f'/vep --format vcf -i {infile} ' \
                          f'--everything --allele_number --no_stats ' \
                          f'--cache --offline --minimal --assembly ' \
                          f'GRCh38 --vcf -o {output_path(path_suffix="annotated_with_vep.vcf.bgz")}'

    subprocess.check_output(complete_cmd_string.split())


if __name__ == '__main__':
    main()  # pylint: disable=E1120
