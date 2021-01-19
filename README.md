# Fewgenomes

Preparing a test dataset for continuous evaluation of single-sample and join variant calling analysis

## Sample selection

As a baseline, we picked 1 trio from the 1000genomes project (NA19238, NA19239, NA19240) of the YRI ancestry, as well NA12878 of CEU ancestry as it's a genome with a validated truth set.

One top of that, we randomly selected samples from different families and ancestries from the 1000genomes project, as long as there is data available at the Google public genomics bucket: (`gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/phase3/data/`). 

A [toy dataset](datasets/toy/samples.ped) of 6 samples ended up containing exomes for individuals from 4 families of 3 ancestries, with 50% females and 50% males. To generate it:

```
snakemake -j1 -p --config n=6 input_type=exome_bam dataset_name=toy
```

A larger [50-sample dataset](datasets/50genomes/samples.ped) ended up containing genomes from 48 families of 18 ancestries with a roughly equal male/female distribution. To generate it:

```
snakemake -j1 -p --config n=50 input_type=wgs_bam dataset_name=50genomes
```

These scripts run the workflow [Snakefile](Snakefile), which:
1. pulls the 1000genomes project metadata from [the 1000genomes FTP](ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/),
2. overlaps it with the data available at `gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/phase3/data/` according to the requested `input_type` (options: `wgs_fastq`, `wgs_bam`, `wgs_bam_highcov`, `exome_bam`),
3. selects a subset of the requested number of samples,
4. generates inputs for the germline variant calling [WDL workflow](https://github.com/populationgenomics/warp/blob/start_from_mapped_bam/pipelines/broad/dna_seq/germline/single_sample/), which is built on top of [Broad WARP](https://github.com/broadinstitute/warp/),
5. generates a PED file for the subset.

To run the worklofw, first set up the environment with:

```
conda env create -n fewgenomes -f environment.yml
```

The WDL inputs are written into `datasets/<dataset_name>/<input_type>/`, and can be used along with Cromwell configs, to execute a pipeline on Google Cloud to generate GVCFs:

```
conda install cromwell==55
git clone https://github.com/populationgenomics/warp
git clone https://github.com/populationgenomics/cromwell-configs
# edit tempaltes in `cromwell-configs` to replace <project> and <bucket>, and save as `cromwell.conf` and `options.json`
SAMPLE=NA12878
cromwell -Dconfig.file=cromwell.conf run \
    warp/pipelines/broad/dna_seq/germline/single_sample/exome/ExomeFromBam.wdl \
    --inputs datasets/toy/exome_bam/${SAMPLE}.json \
    --options options.json
```

## gnomAD Matrix Table subset

Script `hail_subset_gnomad.py` subsets the gnomAD matrix table (`gs://gcp-public-data--gnomad/release/3.1/mt/genomes/gnomad.genomes.v3.1.hgdp_1kg_subset_dense.mt/`) to the samples in the test dataset. To run it, upload your dataset PED file as `gs://playground-us-central1/cpg-fewgenomes/samples.ped` and submit the script into Hail Batch:

```
gsutil cp datasets/50genomes/samples.ped gs://playground-us-central1/cpg-fewgenomes/samples.ped

hailctl dataproc start cpg-fewgenomes --region us-central1 --zone us-central1-a --max-age 12h
hailctl dataproc submit cpg-fewgenomes hail_subset_gnomad.py --region us-central1 --zone us-central1-a
hailctl dataproc stop cpg-fewgenomes
```


