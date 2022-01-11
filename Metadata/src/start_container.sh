#!/usr/bin/env bash
echo $CONFIG_S3
python ./main.py $CONFIG_S3 & python api.py $CONFIG_S3 #& python tests/integration/api_test.py