#!/usr/bin/env bash
python ./main.py $CONFIG_S3 & python api.py $CONFIG_S3 #& python tests/integration/api_test.py