#!/usr/bin/env python3


"""
Loads in the MatrixTable, runs some reduction filters, writes out
Coarse filters applied, so that this dataset will be more
reactive on smaller (notebook) VMs when plotting
"""

import logging
from typing import Any, Dict, Optional
import requests
import hail as hl
import click


def go_and_get_mt(mt_path: str) -> hl.MatrixTable:
    """
    Reads in the stored MatrixTable from disk
    :param mt_path: str, path to a MT directory
    """

    annotated_mt = hl.read_matrix_table(mt_path)
    return annotated_mt


def get_panel_green(
        reference_genome: str,
        panel_app_url: Optional[str] = 'https://panelapp.agha.umccr.org/api/v1/panels/',
        panel_number: Optional[int] = 137
) -> Dict[Any, Dict[str, Optional[Any]]]:
    """
    Takes a panel number, and pulls all details from PanelApp
    :param reference_genome: GRch37 or GRch38
    :param panel_app_url: the root URL for the panelapp server
    :param panel_number: defaults to the PanelAppAU Mendeliome
    :return:
    """
    gene_dict = {}

    panel_app_genes_url = f'{panel_app_url}{panel_number}/'

    panel_response = requests.get(panel_app_genes_url)
    panel_response.raise_for_status()
    panel_json = panel_response.json()
    for gene in panel_json['genes']:
        # or we could use 'Expert Review Green' in 'evidence'
        if (
                gene['confidence_level'] != '3' or
                gene['entity_type'] != 'gene'
        ):
            continue

        symbol = gene.get('entity_name')
        moi = gene.get('mode_of_inheritance')
        ensg = None

        # for some reason the build is capitalised oddly in panelapp
        for build, content in gene['gene_data']['ensembl_genes'].items():
            if build.lower() == reference_genome.lower():
                # the ensembl version may alter over time, but will be singular
                ensg = content[list(content.keys())[0]]['ensembl_id']

        gene_dict[symbol] = {
            'ensembl': ensg,
            'moi': moi
        }
    return gene_dict


def obtain_unique_genes(gene_dict: Dict[Any, Dict[str, Optional[Any]]]) -> set:
    """
    pull out all the unique ensembl ids from a panelapp dictionary
    :param gene_dict:
    :return: set of all unique gene ENSGs
    """

    panel_app_green = set(
        map(lambda x: x['ensembl'], gene_dict.values())
    )
    panel_app_green.discard(None)
    return panel_app_green


@click.command()
@click.option(
    '--matrix_in',
    'matrix_in',
    help='mt to interrogate'
)
@click.option(
    '--matrix_out',
    'matrix_out',
    help='mt to write out'
)
@click.option(
    '--ref',
    'reference',
    help='genomic reference for hail load',
    default='GRCh38'
)
def main(matrix_in: str, matrix_out: str, reference: str):
    """
    :param matrix_in: str, path to a MatrixTable
    :param matrix_out: str, path to a new MT location
    :param reference: str, path to a script to run inside the dataproc
    """

    # boot up hail with the relevant reference
    hl.init(default_reference=reference)

    annotated_mt = go_and_get_mt(mt_path=matrix_in)

    logging.info('# variants before filters: %d', annotated_mt.count_rows())

    # now for some pretty coarse filtering
    # filter only to PASS (i.e. no filter) variants
    annotated_mt = annotated_mt.filter_rows(annotated_mt.filters.length() == 0)

    # do builtin variant QC
    annotated_mt = hl.variant_qc(annotated_mt)

    panel_app_dict = get_panel_green(
        reference_genome='GRch38',
        panel_number=137
    )
    green_genes = obtain_unique_genes(panel_app_dict)

    # remove high frequency potential artefacts
    annotated_mt = annotated_mt.filter_rows(annotated_mt.info.AC <= 0.1 * annotated_mt.info.AN)

    # filter for panel-app genic variants
    annotated_mt = annotated_mt.filter_rows(
        hl.len(annotated_mt.geneIds.intersection(
            hl.literal(green_genes)
        )) > 0
    )

    # syntax for filtering based on AF... but this doesn't address the allele/gene
    # e.g. per-sample our filtering would take into account MOI
    # it also doesn't take into account any sub-populations
    # so... the threshold is very high. 1% for a biallelic would be more realistic
    annotated_mt = annotated_mt.filter_rows(
        (annotated_mt.gnomad_genomes.AF <= 0.06) |
        (annotated_mt.gnomad_genomes.AF == hl.missing('float')) |
        (annotated_mt.exac.AF <= 0.06) |
        (annotated_mt.exac.AF == hl.missing('float'))
    )

    # a lot of filtering here, so let's repartition
    annotated_mt = annotated_mt.repartition(120, shuffle=False)

    # now write out
    annotated_mt.write(matrix_out, overwrite=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()  # pylint: disable=E1120
