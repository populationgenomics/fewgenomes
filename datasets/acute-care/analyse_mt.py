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
from typing import Dict, Optional, Union
from gnomad.utils.vep import vep_struct_to_csq

import hail as hl
import click


# first attempt to use a config failed, not packaged in repo?
config_dict = {
  'missense': {
    'cadd': 28.0,
    'revel': '0.4'
  },
  'af': {
    'exac': 0.001,
    'gnomad_genomes': 0.001
  }
}


def make_info_expr(hail_object: Union[hl.MatrixTable, hl.Table]) -> Dict[str, hl.expr.Expression]:
    """
    Make Hail expression for variant annotations to be included in VCF INFO field.
    :param hail_object: Table/MatrixTable w/annotations to be reformatted for VCF export.
    :return: Dictionary containing Hail expressions for relevant INFO annotations.
    :rtype: Dict[str, hl.expr.Expression]
    """
    vcf_info_dict = {
        'cadd_phred': hail_object['cadd']['PHRED'],
        'splice_ai_max_ds': hail_object['splice_ai']['delta_score'],
        'splice_ai_consequence': hail_object['splice_ai']['splice_consequence']
    }

    # Add in silico annotations to info dict; dbnsfp all taken
    for field, expression in hail_object['dbnsfp'].items():
        vcf_info_dict[f'{field.split()[0]}_score'] = expression

    # get AF sections
    for resource in ['gnomad_genomes', 'gnomad_exomes', 'exac']:
        for field, value in hail_object[resource].items():
            vcf_info_dict[f'{resource}_{field}'] = value

    return vcf_info_dict


def go_and_get_mt(mt_path: str) -> hl.MatrixTable:
    """
    Reads in the stored MatrixTable from disk
    :param mt_path: str, path to a MT directory
    """

    annotated_mt = hl.read_matrix_table(mt_path)
    return annotated_mt


def export_annotated_vcf(matrix: hl.MatrixTable, path: str):
    """
    takes the MT provided, bundles the annotations into info, writes to file
    :param matrix: the MT representing all variant data
    :param path: the path to write out the VCF
    """
    logging.info('Reformatting VEP annotation...')
    # this flattens the possible multiple-consequence-per-entry into separate sections
    vep_expr = vep_struct_to_csq(matrix.vep)

    logging.info('Updating INFO field')
    info_expression = make_info_expr(matrix.rows())
    matrix = matrix.annotate_rows(
        info=matrix.annotate(
            **info_expression,
            vep=vep_expr
        )
    )

    # export directly to a test bucket path
    hl.export_vcf(matrix, path, tabix=True)


def remove_non_genic_variants(
        matrix: hl.MatrixTable,
        keep_genes: Optional[set] = None
) -> hl.MatrixTable:
    """
    either remove all variants without gene annotations
    experimenting with set expressions here
    https://hail.is/docs/0.2/hail.expr.SetExpression.html

    example use case - query PanelApp for genes on a panel
    :param matrix: hl.MatrixTable
    :param keep_genes: Optional[set]
    """

    # the input genes are optional, but can't be a mutable default
    if keep_genes is None:
        keep_genes = set()

    # if we specified some gene IDs, only keep those variants
    if len(keep_genes) > 0:
        keep_genes = hl.literal(keep_genes)
        filtered_matrix = matrix.filter_rows(
            hl.len(matrix.geneIds.intersection(keep_genes)) > 0
        )
    # otherwise only keep rows relating to any gene ID
    else:
        filtered_matrix = matrix.filter_rows(
            hl.len(matrix.geneIds) > 0
        )

    logging.info('Variants retained after filtering: %d', filtered_matrix.count_rows())
    return filtered_matrix


def find_high_impact_missense(matrix: hl.MatrixTable, config: dict) -> hl.MatrixTable:
    """
    CADD = row.cadd.PHRED (float)
    revel = row.dbnsfp.REVEL_score (str) - this will work, but should be cast really
    :param matrix: pass a MT to be row-filtered
    :param config: dict of filtering parameters

    e.g. filtered_matrix = mt.filter_rows((mt.info.AN > 5000) | (mt.info.AN < 4500))
    """

    high_in_silico = matrix.filter_rows(
        (matrix.cadd.PHRED >= config['missense']['cadd']) |
        (matrix.dbnsfp.REVEL_score >= config['missense']['revel'])
    )

    return high_in_silico


def find_rare_variants(matrix: hl.MatrixTable, config: dict) -> hl.MatrixTable:
    """
    finds rare variants in the supplied populations

    gnomad = row.gnomad_genomes.AF (float)
    exac = row.exac.AF (float)
    :param matrix: pass a MT to be row-filtered
    :param config: dict of filtering parameters

    e.g. filtered_matrix = mt.filter_rows((mt.info.AN > 5000) | (mt.info.AN < 4500))
    """

    rare = matrix.filter_rows(
        (matrix.gnomad_genomes.AF >= config['af']['gnomad_genomes']) |
        (matrix.exac.AF >= config['af']['exac'])
    )

    return rare


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
    # using hard coded conf dict for now
    # with open(conf, 'rt', encoding='utf-8') as read_handle:
    #     config = json.load(read_handle)

    annotated_mt = go_and_get_mt(mt_path=matrix)

    logging.info('# variants before filters: %d', annotated_mt.count_rows())

    # very basic pre-filtering, this process will ignore intergenic regions
    # note, this is not the same as coding-only
    # any variant with any relation to a genic annotation retained
    # remove_non_genic_variants(
    #   matrix=annotated_mt,
    #   keep_genes={'ENSG00000012048'}
    # )  # example with one gene ID (working)
    annotated_mt = remove_non_genic_variants(matrix=annotated_mt)

    logging.info('# variants after gene filter: %d', annotated_mt.count_rows())

    # print(annotated_mt.describe())

    # find all rare variation
    annotated_mt = find_rare_variants(matrix=annotated_mt, config=config_dict)

    logging.info('# variants after rare filter: %d', annotated_mt.count_rows())

    # find high impact missense
    annotated_mt = find_high_impact_missense(matrix=annotated_mt, config=config_dict)

    logging.info('# variants after missense filter: %d', annotated_mt.count_rows())

    # annotated_mt.describe()
    # try and dump it out
    export_annotated_vcf(annotated_mt, path='gs://cpg-acute-care-test/annotated_subset.vcf.bgz')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()  # pylint: disable=E1120
