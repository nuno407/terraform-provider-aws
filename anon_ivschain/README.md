# Anonymizer Handler for Inference Service

This service is responsable for receiving messages from the SDM through an input SQS queue and downloading the media from the input S3 bucket and posting a processing request to the IVS Feature Chain. After this, receives the output and apply a post-processing step using ffmpeg to convert encoding from `.avi` to `.mp4` finally, uploads the result to the anonymized bucket.

## Setup

1. Normal setup to run the microservices package
```bash
pip install -e requirements.txt
```

2. Local/Test mode
```bash
pip install -r requirements_dev.txt
```

## Environment variables

- `CONTAINER_NAME` - container name for ContainerServices
- `CONFIG_S3` - configuration bucket for ContainerServices
- `LOGLEVEL` - Logger verbosity: INFO, DEBUG, ERROR, WARNING
- `MOCK_POST_PROCESSOR` - Mocks ffmpeg post-processing step to be used with ivsfc `-m "True"` positional args
- `START_DELAY_SECONDS` - time in seconds to wait during start-up
- `IVS_FC_HOSTNAME` - Hostname of the ivs feature chain API for requesting processing
- `API_PORT` - Port to run flask callback HTTP API (That receives output from IVSFC)
- `AWS_ACCESS_KEY_ID` - boto3 AWS credentials
- `AWS_SECRET_ACCESS_KEY` - boto3 AWS credentials
- `AWS_REGION` - boto3 AWS region
- `AWS_ENDPOINT` - boto3 SDK AWS endpoints (changed for localstack)
