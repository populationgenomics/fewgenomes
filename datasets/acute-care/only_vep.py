#!/usr/bin/env python3


"""
python script to try and run vep
"""


import subprocess
import click


@click.command()
def main():
    """
    just runs vep
    """

    subprocess.check_output(['/vep'])


if __name__ == '__main__':
    main()  # pylint: disable=E1120
