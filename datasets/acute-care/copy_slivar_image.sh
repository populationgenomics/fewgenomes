#!/usr/bin/env bash

set -ex

gcloud auth configure-docker australia-southeast1-docker.pkg.dev
skopeo copy docker://docker.io/brentp/slivar:v0.2.7 docker://australia-southeast1-docker.pkg.dev/cpg-common/images/slivar:v0.2.7
