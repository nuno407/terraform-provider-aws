#!/usr/bin/env bash
rabbitmq-server -detached
python api.py && celery -A proj worker --pool=solo -l INFO 