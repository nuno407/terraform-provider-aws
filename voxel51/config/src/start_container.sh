#!/usr/bin/env bash
mkdir /mnt/s3 
s3fs dev-rcd-anonymized-video-files /mnt/s3 
python /fiftyone/FiftyOne-EKS-datasetLync.py
