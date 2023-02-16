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

SAMPLES=NA12340 NA12489 NA12878 NA12891 NA12892
CRAM_PREFIX="gs://cpg-fewgenomes-test/cram"
OUTPUT_PREFIX="gs://cpg-fewgenomes-test/small_fastqs"
# FLRT2 gene, but there's no significance to that.
BED_INTERVAL="chr14 85530143 85654428"
TMP_BAM="tmp.bam"
TMP_FASTQ1="R1.fastq.gz"
TMP_FASTQ2="R2.fastq.gz"


for SAMPLE in $SAMPLES; do
    echo "*** $SAMPLE ***"
    # Extract a genomic interval from the CRAM.
    samtools view -L <(echo "$BED_INTERVAL") -b "$TMP_BAM" "$CRAM_PREFIX/$SAMPLE.cram"
    # Convert to FASTQs.
    samtools bam2fq -1 "$TMP_FASTQ1" -2 "$TMP_FASTQ2" "$TMP_BAM"
    # Copy to output folder.
    gcloud storage cp "$TMP_FASTQ1" "$OUTPUT_PREFIX/$SAMPLE.R1.fastq.gz"
    gcloud storage cp "$TMP_FASTQ2" "$OUTPUT_PREFIX/$SAMPLE.R2.fastq.gz"
done
