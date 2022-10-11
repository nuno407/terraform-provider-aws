#!/bin/bash
MODE=${1:?'must provide a mode as argument, available options: [api, consumer]'}

# using exec forfits the PID 1 from bash to the python process
# this works better with k8s liveness command probe
if [[ "${MODE}" == "api" ]]; then
    exec python -m metadata.api
elif [[ "${MODE}" == "consumer" ]]; then
    exec python -m metadata.consumer
else
    echo "invalid mode. Should be one of: [api, consumer]"
    exit 1
fi
