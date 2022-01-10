#!/usr/bin/env python3


import click
import hail as hl
import json
import logging
import os
import sys

from itertools import chain
from typing import Optional


"""
Procedural script that is provided with command line inputs
- the JSON dictionary representing "family_name_1: [list, of, internal, sample, IDs], "
- the project/dataset name, used to identify the buckets assc. with the specific project

"""


class NotAllSamplesPresent(Exception):
    pass


def check_samples_in_mt(
    sample_names: set, family_structures: dict, mat: hl.MatrixTable
):
    """
    checks if all samples are present
    - if not, checks for any samples being present
    - if some but not all flag families, if all are missing flag the values as possibly in wrong format

    could restructure this so that we only check for absence, with the 'good' case being no errors
    """
    samples_in_mt = set(mat.s.collect())

    # all good?
    if len(sample_names - samples_in_mt) == 0:
        logging.info(
            f"All {len(samples_in_mt)} samples represented across {len(family_structures)} families"
        )
        return

    # partially good? some requested samples are present, but not all
    elif samples_in_mt.intersection(sample_names):
        for family, samples in family_structures.items():
            family_missing = set(samples) - samples_in_mt
            if family_missing:
                logging.info(
                    f"Family {family} is not fully represented in the data. "
                    f"Samples missing: {sorted(family_missing)}"
                )

    # problem, none present
    else:
        logging.error(
            f"No requested samples were present in the MT, please check format matches '{samples_in_mt.pop()}'"
        )

    raise NotAllSamplesPresent("please check logging messages for details")


def get_all_unique_members(family_dict: dict) -> set:
    """
    pulls all unique individual members from a dict and returns as a set
    """
    return set(chain.from_iterable(family_dict.values()))


def read_mt(mt_location: str, reference: str = "GRCh38") -> hl.MatrixTable:

    # initiate Hail expecting GRCh38
    hl.init(default_reference=reference)

    # open full MT (note, not all read in, this is done lazily with spark)
    return hl.read_matrix_table(mt_location)


def obtain_mt_subset(matrix: hl.MatrixTable, samples: list) -> hl.MatrixTable:
    """
    implements the actual subsetting of the MT
    """

    # filter the full dataset to only the samples we're interested in
    return matrix.filter_cols(hl.literal(samples).contains(matrix["s"]))


@click.command()
@click.option(
    "--json-str",
    help="this is the formatted json string of family_sample_lookups",
    type=click.STRING,
)
@click.option(
    "--dataset", help="name of the dataset to use (gcp bucket names)", type=click.STRING
)
@click.option(
    "--ref",
    "reference",
    help="name of GRChXX reference to use",
    default="GRCh38",
    type=click.STRING,
)
@click.option(
    "--multi_fam",
    "multi_fam",
    is_flag=True,
    default=False,
    help="use this flag if we also want a multi-family MT written to test",
)
def main(json_str: str, dataset: str, reference: Optional[str], multi_fam: bool):
    """
    This takes the family structures encoded in the JSON str and creates a number of single-family objects in Test

    The option to include a multi-family structure is useful for experimenting with operations mapping MOI patterns
    per-family within a larger dataset. This will be useful in analysing runtime/cost of analysing a single family with
    and without extracting from a larger dataset first
    """

    # parse the families dict from the input string, e.g. '{"fam1":["sam1","sam2"]}'
    families_dict = json.loads(json_str)

    gcp_test_bucket = f"gs://cpg-{dataset}-test"

    # path built in this way to pass separate components to the GCP file check
    gcp_main_bucket = f"gs://cpg-{dataset}-main"
    mt_in_bucket = os.path.join("mt", f"{dataset}.mt")
    gcp_mt_full = os.path.join(gcp_main_bucket, mt_in_bucket)

    # collect all unique sample IDs for a single filter on the MT
    all_samples = get_all_unique_members(families_dict)

    mt = read_mt(gcp_mt_full, reference=reference)

    if multi_fam:
        # pull all samples from all requested families
        multi_fam_mt = obtain_mt_subset(mt, list(all_samples))
        # force-write this family MT to a test location
        multi_fam_mt.write(
            os.path.join(gcp_test_bucket, "multiple_families.mt"), overwrite=True
        )

    # check that all the samples are present - alter this so the method either completes or raises Exception?
    check_samples_in_mt(all_samples, families_dict, mt)

    # for each family, dump both a small MT and a VCF containing the same samples/variants
    for family, samples in families_dict.items():

        # pull out only this family's samples from the MT
        family_mt = obtain_mt_subset(mt, samples)

        # write this family MT to a test location
        family_mt.write(os.path.join(gcp_test_bucket, f"{family}.mt"))

        # revert to a VCF file format, and write to a test location
        hl.export_vcf(os.path.join(gcp_test_bucket, f"{family}.vcf.bgz"))


if __name__ == "__main__":
    # verbose logging, but this will cause issues matching exact strings in tests
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)s] %(levelname)s %(module)s:%(lineno)d - %(message)s",
        datefmt="%Y-%M-%d %H:%M:%S",
        stream=sys.stderr,
    )
    main()
