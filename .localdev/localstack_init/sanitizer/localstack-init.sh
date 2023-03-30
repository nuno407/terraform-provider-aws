#!/bin/bash
awslocal sqs create-queue --queue-name "local-sanitize-input"
awslocal sns create-topic --name "local-sanitize-output"
