#!/bin/bash
#
# for quick local testing with localstack
#
if ! command -v awslocal > /dev/null; then
    echo "awslocal required, pip install awscli-local"
    exit 1
fi

if ! command -v jq > /dev/null; then
    echo "jq required"
    exit 1
fi

if ! command -v tee > /dev/null; then
    echo "tee required"
    exit 1
fi

TEST_VIDEO=${TEST_VIDEO:-'test_input_video.mp4'}
INPUT_QUEUE_NAME=${INPUT_QUEUE_NAME:-'local-terraform-queue-anonymize'}
RAW_VIDEO_BUCKET=${RAW_VIDEO_BUCKET:-'local-rcd-raw-video-files'}
TMP_MESSAGE_PATH='./.localdev/example-message.json'

if ! test -f .localdev/${TEST_VIDEO}; then
    echo "must download a test video first and place it under .localdev/${TEST_VIDEO}"
    exit 1
fi

function cleanup() {
    rm -f "${TMP_MESSAGE_PATH}"
}

trap cleanup EXIT

set -x
awslocal s3 cp .localdev/${TEST_VIDEO} s3://${RAW_VIDEO_BUCKET}/Debug_Lync/${TEST_VIDEO}

ANON_INPUT_QUEUE_URL=$(awslocal sqs get-queue-url --queue-name ${INPUT_QUEUE_NAME} | jq '.QueueUrl' -r)

tee "${TMP_MESSAGE_PATH}" <<EOF
{
    "processing_steps": ["Anonymize"],
    "s3_path": "Debug_Lync/${TEST_VIDEO}",
    "data_status": "received"
}
EOF

awslocal sqs send-message --queue-url ${ANON_INPUT_QUEUE_URL} --message-body "$(cat ${TMP_MESSAGE_PATH} | jq -c)"
set +x
