#!/usr/bin/env python3

"""Creates mini FASTQs from a few public 1KG samples.

Run as follows:

analysis-runner \
    --dataset fewgenomes \
    --access-level test \
    --description "create small FASTQs for testing" \
    --output small_fastqs \
    --image australia-southeast1-docker.pkg.dev/cpg-common/images/samtools:1.16.1 \
    small_fastqs.py
"""

import subprocess
from cpg_utils.hail_batch import dataset_path, output_path

CRAMS = [
    'NA12340.cram',
    'NA12489.cram',
    'NA12878.cram',
    'NA12891.cram',
    'NA12892.cram',
]

# FLRT2 gene, but there's no significance to that.
BED_INTERVAL = 'chr14 85530143 85654428'
TMP_BAM = 'tmp.bam'
TMP_FASTQ1 = 'R1.fastq.gz'
TMP_FASTQ2 = 'R2.fastq.gz'


def main():
    for cram in CRAMS:
        path = dataset_path(f'cram/{cram}')
        print(path)

        # First extract a window.
        subprocess.run(
            [
                'samtools',
                'view',
                '-L',
                f'<$(echo "{BED_INTERVAL}")',
                '-b',
                TMP_BAM,
                path,
            ],
            check=True,
        )

        # Then convert to FASTQs.
        subprocess.run(
            [
                'samtools',
                'bam2fq',
                '-1',
                TMP_FASTQ1,
                '-2',
                TMP_FASTQ2,
                TMP_BAM,
            ],
            check=True,
        )

        # Copy to output folder.
        subprocess.run(
            [
                'gcloud',
                'storage',
                'cp',
                TMP_FASTQ1,
                output_path(cram.replace('.cram', '.R1.fastq.gz')),
            ]
        )

        subprocess.run(
            [
                'gcloud',
                'storage',
                'cp',
                TMP_FASTQ2,
                output_path(cram.replace('.cram', '.R2.fastq.gz')),
            ]
        )


if __name__ == '__main__':
    main()
