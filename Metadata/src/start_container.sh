#!/usr/bin/env bash
echo $RCD_EKS_CLUSTER_ID
echo $region
echo $METADATA_IAM_ROLE_NAME
python ./main.py $CONFIG_S3 & python api.py $CONFIG_S3 #& python tests/integration/api_test.py