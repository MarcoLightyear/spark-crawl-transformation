#!/bin/bash
export INPUT=path-to--input-bucket
gcloud dataproc batches submit \
    pyspark colonies.py \
    --container-image=path-to-docker-registry \
    --batch=twentyeight \
    --region=us-east1 \
    --deps-bucket=path-to-jobs-bucket \
    --version=1.1 \
    --history-server-cluster=path-to-phs-cluster \
    -- ${INPUT}