import click
import hail as hl
import json
import logging
import os
import sys

from itertools import chain


"""
simple procedural script that is provided with command line inputs
- the JSON dictionary representing "family_name_1: [list, of, internal, sample, IDs], "
- the project/dataset name, used to identify the buckets assc. with the specific project

arguments passing mediated with click, so minimal hard-coding required 

Additional - check that the requested samples are present, and throw exceptions if not
"""


def check_for_samples(sample_names: set, family_structures: dict, mat: hl.MatrixTable) -> bool:
    """
    checks if all samples are present
    - if not, checks for any samples being present
    - if some but not all flag families, if all are missing flag the values as possibly in wrong format
    """
    samples_in_mt = set(mat.s.collect())

    print(sample_names)
    print(samples_in_mt)

    # all good?
    if all([sam in samples_in_mt for sam in sample_names]):
        logging.info(f"All {len(samples_in_mt)} samples represented across {len(family_structures)} families")
        return True

    # partially good? some requested samples are present, but not all
    elif any([sam in samples_in_mt for sam in sample_names]):
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

    return False


def get_all_unique_members(family_dict: dict) -> set:
    """
    pulls all individual members from a dict and returns as a set
    """
    return set(chain.from_iterable(family_dict.values()))


@click.command()
@click.option(
    "--json-str",
    help="this is the formatted json string of family_sample_lookups",
    type=click.STRING,
)
@click.option(
    "--dataset", help="name of the dataset to use (gcp bucket names)", type=click.STRING
)
def main(json_str: str, dataset: str):

    # parse the families dict from the input string, e.g. '{"fam1":["sam1","sam2"]}'
    families_dict = json.loads(json_str)

    gcp_test = f"gs://cpg-{dataset}-test"
    gcp_main = f"gs://cpg-{dataset}-main"
    gcp_mt_full = os.path.join(gcp_main, "mt", f"{dataset}.mt")

    # collect all unique sample IDs for a single filter on the MT
    all_samples = get_all_unique_members(families_dict)

    # initiate Hail expecting GRCh38
    hl.init(default_reference='GRCh38')

    # open full MT (note, not all read in, this is done lazily with spark)
    mt = hl.read_matrix_table(gcp_mt_full)

    # check that all the samples are present
    valid_names = check_for_samples(all_samples, families_dict, mt)
    if not valid_names:
        exit()

    # filter the full dataset to only the samples we're interested in
    mt = mt.filter_cols(hl.literal(all_samples).contains(mt["s"]))

    # if we want to implement a region filter, e.g. MANE plus clinical, this would be the ideal time
    # current thinking is that it's not necessary at this time, as the case-specific work will determine
    # the regions of interest, etc.

    # for each family, dump both a small MT and a VCF containing the same samples/variants
    for family, samples in families_dict.items():

        # take the list of samples and translate to a hail expression
        samples_to_retain = hl.literal(samples)

        # based on defaults for hl.import_vcf, location for the sample ID would be the attribute 's'
        family_mt = mt.filter_cols(samples_to_retain.contains(mt["s"]))

        # write this family MT to a test location
        family_mt.write(os.path.join(gcp_test, f"{family}.mt"))

        # revert to a VCF file format, and write to a test location
        hl.export_vcf(os.path.join(gcp_test, f"{family}.vcf.bgz"))


if __name__ == "__main__":

    # verbose logging, but this will cause issues matching exact strings in tests
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)s] %(levelname)s %(module)s:%(lineno)d - %(message)s",
        datefmt="%Y-%M-%d %H:%M:%S",
        stream=sys.stderr,
    )
    main()
