# Container Scripts

This repository contains all Datanauts owned applications.

## Github Workflows
* Every push to `master` will trigger an image build of all **changed** applications. When `baseaws` is changed all dependent services will be built (determined by checking for a reference to `baseaws` in Dockerfile).
* A manual trigger of the Github workflow `Build Docker image and update k8s config for changed services` will build **all** applications. This also works for feature branches and will create image tags with the branch name suffix.
* For master only: After successfully building all images the new tags will automatically be set in the `kubernetes_config` repository for the `dev` environment and automatically rolled out to the cluster via Flux.
