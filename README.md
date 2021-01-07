# Fewgenomes

Preparing a test dataset for continuous evaluation of single-sample and join variant calling analysis

## Sample selection

As a baseline, we picked 1 trio from the 1000genomes project (NA19238, NA19239, NA19240) of the YRI ancestry, as well NA12878 of CEU ancestry as it's a genome with a validated truth set.

One top of that, we randomly selected samples from different families and ancestries from the 1000genomes project, as long as there is data available at the Google public genomics bucket: (`gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/phase3/data/`). 

A [toy dataset](datasets/toy/samples.ped) of 6 samples ended up containing exomes for individuals from 4 families of 3 ancestries, with 50% females and 50% males. To generate it:

```
snakemake -j1 -p --config n=6 input_type=exome_bam dataset_name=toy
```

A larger [50-sample dataset](datasets/50genomes/samples.ped) ended up containing genomes from 48 families of 21 ancesteies with a roughly equal male/female distribution. To generate it:

```
snakemake -j1 -p --config n=50 input_type=wgs_bam dataset_name=50genomes
```

The workflow `snakefile` pulls the 1000genomes project metadata, overlaps it with the data available at `gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/phase3/data/` according to the requested `input_type` (options: `wgs_fastq`, `wgs_bam`, `wgs_bam_highcov`, `exome_bam`), selects the required number of samples, and generates inputs for the germline variant calling [WDL pipeline](https://github.com/populationgenomics/warp/blob/start_from_mapped_bam/pipelines/broad/dna_seq/germline/single_sample/) which is built on top of Broad WARP workflows.

To set up the environment, run:

```
conda env create -n fewgenomes -f environment.yml
```

The WDL inputs are written into `datasets/<dataset_name>/<input_type>/`, and can be used to execute a pipeline to generate GVCFs:

```
conda install cromwell==54
git clone https://github.com/populationgenomics/warp warp
SAMPLE=NA12878
cromwell -Dconfig.file=cromwell/cromwell.conf run \
    warp/pipelines/broad/dna_seq/germline/single_sample/wgs/ExomeFromBam.wdl \
    --inputs datasets/toy/exome_bam/${SAMPLE}.json \
    --options cromwell/options.json
```

## gnomAD Matrix Table subset

Script `hail_subset_gnomad.py` subsets the gnomAD matrix table (`gs://gcp-public-data--gnomad/release/3.1/mt/genomes/gnomad.genomes.v3.1.hgdp_1kg_subset_dense.mt/`) to the samples in the test dataset. To run it, upload your dataset PED file as `gs://playground-us-central1/cpg-fewgenomes/samples.ped` and submit the script into Hail Batch:

```
gsutil cp datasets/50genomes/samples.ped gs://playground-us-central1/cpg-fewgenomes/samples.ped

hailctl dataproc start cpg-fewgenomes --region us-central1 --zone us-central1-a --max-age 12h
hailctl dataproc submit cpg-fewgenomes hail_subset_gnomad.py --region us-central1 --zone us-central1-a
hailctl dataproc stop cpg-fewgenomes
```



