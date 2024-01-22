# Container Scripts

## Create Local K8s cluster

```
kind create cluster --config kind-config.yaml
```

Note: Please ensure that, for the following commands you are selecting the correct context, for example:

```
kubectl config use-context kind-kind
```

Please ensure that your repos follow this folder structure:
```
ANY FOLDER/
├── container_scripts
├── tf_infrastructure
├── kubernetes_config
└── voxel51_plugins
```

## Using local machine

### Deploy LocalStack

```
skaffold run -f skaffold.stack.yaml --port-forward --profile local
```

Or If you want to deploy and attach to logs:

```
skaffold run -f skaffold.stack.yaml --port-forward --tail --profile local
```

Please note that if you want to fully remove/reset the localstack you should do (localstack has cross-deployment persistence):

```
skaffold delete -f skaffold.stack.yaml --profile local
```

### Deploy all apps/services

Make sure to export the Voxel token on the terminal you run Skaffold.
It is needed to fetch the Voxel libs during Docker build.
```
export VOXEL_REGISTRY_TOKEN=xxx
```

```
skaffold dev --profile local
```

If the changes in code are not detected please do

```
skaffold dev --trigger polling --profile local
```


## Using remote cluster

### Create a vcluster

Create the cluster

```
vcluster create NTUSER --namespace developer-env-NTUSER --context arn:aws:eks:eu-central-1:081962623310:cluster/dev_rcd-eks-cluster --kube-config-context-name vcluster-dev --upgrade -f vcluster-config.yaml
```
NOTE: Replace NTUSER by you user in lowercase

### Deploy LocalStack

Conect to your remote vcluster

```
vcluster connect NTUSER --context arn:aws:eks:eu-central-1:081962623310:cluster/dev_rcd-eks-cluster --namespace developer-env-NTUSER --kube-config-context-name vcluster-dev
```
NOTE: Replace NTUSER by you user in lowercase


Deploy local stack

```
skaffold run -f skaffold.stack.yaml --port-forward --profile vcluster --kube-context vcluster-dev
```

Or If you want to deploy and attach to logs:

```
skaffold run -f skaffold.stack.yaml --port-forward --tail --port-forward --profile vcluster --kube-context vcluster-dev
```

Please note that if you want to fully remove/reset the localstack you should do (localstack has cross-deployment persistence):

```
skaffold delete -f skaffold.stack.yaml --profile vcluster --kube-context vcluster-dev
```

### Deploy all apps/services

Make sure to export the Voxel token on the terminal you run Skaffold.
It is needed to fetch the Voxel libs during Docker build.
```
export VOXEL_REGISTRY_TOKEN=xxx
export NTUSER=YOUR_NT_USER_LOWERCASE
```

```
skaffold dev --profile vcluster --kube-context vcluster-dev
```

If the changes in code are not detected please do

```
skaffold dev --trigger polling --profile vcluster --kube-context vcluster-dev
```

### Using the Makefile

Make sure that you export both your NTUSER and Voxel Token, as the previous section.

To create your VCluster:

```
make create
```

To deploy the localstack:
```
make stack
```
To connect to the VCluster and deploy all apps/services:
```
make start
```
