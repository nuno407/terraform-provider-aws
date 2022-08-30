#!/bin/bash
MODE=${1:?'must provide a mode as argument, available options: [api, consumer]'}

# using exec forfits the PID 1 from bash to the python process
# this works better with k8s liveness command probe
if [[ "${MODE}" == "api" ]]; then
    exec python api.py
elif [[ "${MODE}" == "consumer" ]]; then
    exec python main.py
else
    echo "invalid mode"
    exit 1
fi
