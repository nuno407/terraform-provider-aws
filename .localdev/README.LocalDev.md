# Container Scripts

## Create Local K8s cluster

```
kind create cluster
```

## Deploy LocalStack

```
skaffold run -f skaffold.stack.yaml --port-forward
```

Or If you want to deploy and attach to logs:

```
skaffold run -f skaffold.stack.yaml --port-forward --tail
```

Please note that if you want to fully remove/reset the localstack you should do (localstack has cross-deployment persistence):

```
skaffold delete -f skaffold.stack.yaml
```

## Deploy all apps/services

Make sure to export the Voxel token on the terminal you run Skaffold.
It is needed to fetch the Voxel libs during Docker build.
```
export TOKEN=xxx
```

```
skaffold dev
```

If the changes in code are not detected please do

```
skaffold dev --trigger polling
```
