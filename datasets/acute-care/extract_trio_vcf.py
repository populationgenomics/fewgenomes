import hail as hl
import os

from itertools import chain


"""
simple procedural script that is manually provided with inputs
1. the family/families we're interested in retaining for the joint call (family ID and samples)
2. the path to the MT from the full project joint call
3. the path to the test output location, for storing the single-family VCF and MT
"""


GCP_MT = "gs://cpg-acute-care-main/mt/acute-care.mt"
GCP_ACUTE_TEST_BASE = "gs://cpg-acute-care-test"

# --- Manually entered inputs, generated from families_to_samples.py --- #
# **check which form of ID is present in the MT (looks like these IDs will be CPG internal IDs
# **also check whether these IDs can/should be committed to a public repo
family_sample_lookups = {
    "FAM000340": [
        "CPG54601",
        "CPG54593",
        "CPG54619"
    ],
    "FAM000320": [
        "CPG54130",
        "CPG54114",
        "CPG54122"
    ],
    "FAM000337": [
        "CPG54692",
        "CPG54700",
        "CPG54684"
    ],
    "FAM000327": [
        "CPG54676",
        "CPG54650",
        "CPG54668"
    ],
    "FAM000334": [
        "CPG54627",
        "CPG54635",
        "CPG54643"
    ]
}

# collect all unique sample IDs for a single filter on the MT
all_samples = set(chain.from_iterable(family_sample_lookups.values()))

# ok, so we need to pull in the massive MT
mt = hl.read_matrix_table(GCP_MT)

# filter the massive dataset to only the samples we're interested in
mt = mt.filter_cols(hl.literal(all_samples).contains(mt["s"]))

# if we want to implement a region filter, e.g. MANE plus clinical, this would be the ideal time
# current thinking is that it's not necessary at this time, as the case-specific work will determine
# the regions of interest, etc.

# for each family, dump both a small MT and a VCF containing the same samples/variants
for family, samples in family_sample_lookups.items():
    samples_to_retain = hl.literal(samples)
    # based on the implementation of hl.import_vcf, the default location for the sample ID would be the attribute 's'
    family_mt = mt.filter_cols(samples_to_retain.contains(mt["s"]))

    # write this family MT to a test location
    family_mt.write(os.path.join(GCP_ACUTE_TEST_BASE, f"{family}.mt"))

    # revert to a VCF file format, and write to a test location
    hl.export_vcf(os.path.join(GCP_ACUTE_TEST_BASE, f"{family}.vcf.bgz"))
