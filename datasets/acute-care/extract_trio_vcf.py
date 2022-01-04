import click
import hail as hl
import json
import os

from itertools import chain


"""
simple procedural script that is provided with command line inputs
- the JSON dictionary representing "family_name_1: [list, of, internal, sample, IDs], "
- the project/dataset name, used to identify the buckets assc. with the specific project

arguments passing mediated with click, so minimal hard-coding required 
"""


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
    all_samples = set(chain.from_iterable(families_dict.values()))

    # open full MT (note, not all read in, this is done lazily with spark)
    mt = hl.read_matrix_table(gcp_mt_full)

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
    main()
