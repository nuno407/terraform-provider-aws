SHELL := /bin/bash

# Define the name of your Skaffold profile (if you have one)
SKAFFOLD_PROFILE := vcluster
KUBE_CONTEXT := vcluster-dev
# Define the name of your Skaffold configuration file
SKAFFOLD_CONFIG := skaffold.yaml

# Define the path to your Bash script
SCRIPT_PATH := ./plugin-watcher.sh

NT_USER=${NTUSER}
CONNECT_PIPE := connect_pipe

# The default target runs Skaffold and then your script
.PHONY: connect skaffold create stack
create:

	echo -e "sync:\n  serviceaccounts:\n    enabled: true" > values.yaml
	vcluster create $(NT_USER) --namespace developer-env-$(NT_USER) --context arn:aws:eks:eu-central-1:081962623310:cluster/dev_rcd-eks-cluster --kube-config-context-name vcluster-dev --upgrade -f values.yaml

stack:
	skaffold run -f skaffold.stack.yaml --port-forward --profile vcluster --kube-context vcluster-dev

connect:
	@( \
		vcluster connect $(NT_USER) --context arn:aws:eks:eu-central-1:081962623310:cluster/dev_rcd-eks-cluster --namespace developer-env-$(NT_USER) --kube-config-context-name vcluster-dev | tee connect.log \
	) &
skaffold:
	@( \
		while ! grep -q "Forwarding" connect.log; do \
			sleep 1; \
		done; \
		rm connect.log; \
		skaffold dev --profile $(SKAFFOLD_PROFILE) --port-forward --trigger polling --cleanup=false --kube-context $(KUBE_CONTEXT) -f $(SKAFFOLD_CONFIG) & $(SCRIPT_PATH);   \
	)
start: connect skaffold
