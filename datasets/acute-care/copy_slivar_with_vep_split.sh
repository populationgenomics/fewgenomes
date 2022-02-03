#!/usr/bin/env bash

set -ex

gcloud auth configure-docker australia-southeast1-docker.pkg.dev
skopeo copy docker://docker.io/mwellandcpg/slivar:vep_split docker://australia-southeast1-docker.pkg.dev/cpg-common/images/slivar:vep_split
