# Container Scripts

## Create Local K8s cluster

```
kind create cluster
```

## Deploy LocalStack

```
skaffold dev -f skaffold.stack.yaml
```

Or If you want to deploy and attach to logs:

```
skaffold run -f skaffold.stack.yaml --tail
```

Please note that if you want to fully remove/reset the localstack you should do (localstack has cross-deployment persistence):

```
skaffold delete -f skaffold.stack.yaml
```

## Deploy all apps/services

```
skaffold dev
```

If the changes in code are not detected please do

```
skaffold dev --trigger polling
```
