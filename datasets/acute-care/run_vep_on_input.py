#!/usr/bin/env python3


"""
execute a subprocess call in python to run VEP annotation
"""


import subprocess
import click


@click.command()
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
    THIS WILL NOT WORK, as VEP is installed but can't resolve the VCF path
    :param infile: str, the GCP path for a given input VCF
    :param outfile: str, the path to the VEP annotated output VCF
    """

    # copy the gs file to local
    subprocess.check_output(['gsutil', 'cp', infile, 'target.vcf.bgz'])
    subprocess.check_output(['gsutil', 'cp', f'{infile}.tbi', 'target.vcf.bgz.tbi'])

    complete_cmd_string = '/vep --format vcf -i target.vcf.bgz ' \
                          '--everything --allele_number --no_stats ' \
                          '--cache --offline --minimal --assembly ' \
                          'GRCh38 --vcf -o local_output'

    subprocess.check_output(complete_cmd_string.split())

    # now push the file back to GCP bucket
    subprocess.check_output(['gsutil', 'cp', 'local_output', outfile])


if __name__ == '__main__':
    main()  # pylint: disable=E1120
