#!/bin/bash

# Creates mini FASTQs from a few public 1KG samples.
# 
# Run as follows:
# 
# analysis-runner \
#     --dataset fewgenomes \
#     --access-level test \
#     --description "create small FASTQs for testing" \
#     --output small_fastqs \
#     --image australia-southeast1-docker.pkg.dev/cpg-common/images/samtools:1.16.1 \
#     small_fastqs.sh

set -ex

SAMPLES=(NA12340 NA12489 NA12878 NA12891 NA12892)
CRAM_PREFIX="gs://cpg-fewgenomes-test/cram"
OUTPUT_PREFIX="gs://cpg-fewgenomes-test/small_fastqs"
# FLRT2 gene, but there's no significance to that.
BED_INTERVAL="chr14 85530143 85654428"
REFERENCE="gs://cpg-common-main/references/hg38/v0/Homo_sapiens_assembly38.fasta"

# Localize the reference once.
gsutil -m cp "$REFERENCE" "$REFERENCE.fai" .

for SAMPLE in "${SAMPLES[@]}"; do
    # Required by samtools to be able to read from GCS. Refresh this regularly.
    GCS_OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
    export GCS_OAUTH_TOKEN
    # Extract a genomic interval from the CRAM, then convert to FASTQs.
    # https://www.metagenomics.wiki/tools/samtools/converting-bam-to-fastq
    samtools view -L <(echo "$BED_INTERVAL") -b -T "$(basename $REFERENCE)" "$CRAM_PREFIX/$SAMPLE.cram" | \
    samtools sort -n | \
    samtools fastq -1 "$SAMPLE.R1.fastq.gz" -2 "$SAMPLE.R2.fastq.gz" -0 /dev/null -s /dev/null -n
    # Writing to GCS directly seems problematic, so copy temporary local files instead.
    gsutil -m mv "$SAMPLE.R?.fastq.gz" "$OUTPUT_PREFIX/"
done
