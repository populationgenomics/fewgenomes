#!/usr/bin/env bash

set -ex

input_file=$1
output_file=$2

/vep --format vcf -i "${input_file}" --everything \
  --allele_number --no_stats --cache --offline \
  --minimal --assembly GRCh38 --vcf -o "${output_file}"
