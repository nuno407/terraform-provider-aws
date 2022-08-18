#!/bin/bash
set -x
BUCKET_LIST="local-rcd-config-files local-rcd-raw-video-files local-rcd-anonymized-video-files"
IFS=' '
for bucket in $BUCKET_LIST
do
    awslocal s3 mb s3://${bucket}
done

QUEUE_LIST="local-terraform-queue-anonymize local-terraform-queue-metadata local-terraform-queue-chc"
IFS=' '
for queue in $QUEUE_LIST
do
    awslocal sqs create-queue --queue-name ${queue}  
done

awslocal s3 cp /docker-entrypoint-initaws.d/localtest-containers-config.json s3://local-rcd-config-files/containers/config_file_containers.json
set +x
