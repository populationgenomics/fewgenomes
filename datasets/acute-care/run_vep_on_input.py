#!/usr/bin/env python3


"""
execute a subprocess call in python to run VEP annotation
"""


import os
import hailtop.batch as hb
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
    :param infile: str, the GCP path for a given input VCF
    :param outfile: str, the path to the VEP annotated output VCF
    """

    # start a hail batch
    service_backend = hb.ServiceBackend(
        billing_project=os.getenv('HAIL_BILLING_PROJECT'),
        bucket=os.getenv('HAIL_BUCKET'),
    )

    # create a hail batch
    batch = hb.Batch(
        name='run_vep',
        backend=service_backend
    )

    job = batch.new_job(name='run vep')
    job.cpu(2)
    job.memory('standard')  # ~ 4G/core ~ 7.5G
    job.storage('20G')

    # read in from GC file
    in_vcf = batch.read_input(infile)

    job.command(
        f'/vep --format vcf -i {in_vcf} --everything '
        f'--allele_number --no_stats --cache --offline '
        f'--minimal --assembly GRCh38 --vcf -o {job.ofile}'
    )
    batch.write_output(job.ofile, outfile)
    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
