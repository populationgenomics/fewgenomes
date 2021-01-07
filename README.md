# A test dataset for continuous evaluation of single-sample and join variant calling analysis

## Sample selection

We picked 3 samples from the CEPH 1463 family (NA12878, NA12891, NA12892), and added 47 samples randomly selected from 47 different families from the 1000genomes project, for which there is raw read data available at Google public genomics buckets (`gs://genomics-public-data/ftp-trace.ncbi.nih.gov/1000genomes/ftp/phase3/data/<sample>/sequence_read/*.fastq.gz`). Selected samples ended up from 21 ancestries, and with a roughly equal male/female distribution. 

Script `prep_dataset.py` pulls a the 1000genomes project metadata spreadsheet, selects samples following the criteria above, and generates inputs for the single-sample WGS germline variant calling [WDL pipeline](https://github.com/populationgenomics/warp/blob/start_from_mapped_bam/pipelines/broad/dna_seq/germline/single_sample/wgs/WGSFromFastq.wdl), based on Broad's WARP.

```
pip install click pandas google-cloud-storage==1.25.0 ngs-utils==2.8.7
python prep_dataset.py
```

The WDL inputs are written into the `workflow/inputs` folder, and can be used to execute a pipeline to generate GVCFs.

```
conda install cromwell==54
git clone https://github.com/populationgenomics/warp workflow/warp
SAMPLE=HG00103
cromwell -Dconfig.file=workflow/cromwell.conf run \
    workflow/warp/pipelines/broad/dna_seq/germline/single_sample/wgs/WGSFromFastq.wdl \
    --inputs workflow/inputs/${SAMPLE}.json \
    --options workflow/options.json
```

## gnomAD Matrix Table subset

Script `hail_subset_gnomad.py` subsets the gnomAD matrix table (`gs://gcp-public-data--gnomad/release/3.1/mt/genomes/gnomad.genomes.v3.1.hgdp_1kg_subset_dense.mt/`) to the samples in the test dataset. To run it, upload your dataset PED file as `gs://playground-us-central1/cpg-fewgenomes/samples.ped` and submit the script into Hail Batch:

```
gsutil cp datasets/50genomes/samples.ped gs://playground-us-central1/cpg-fewgenomes/samples.ped

hailctl dataproc start cpg-fewgenomes --region us-central1 --zone us-central1-a --max-age 12h
hailctl dataproc submit cpg-fewgenomes hail_subset_gnomad.py --region us-central1 --zone us-central1-a
hailctl dataproc stop cpg-fewgenomes
```



