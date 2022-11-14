# Container Scripts

This repository contains all Datanauts owned applications.

- [Container Scripts](#container-scripts)
  - [Github Workflows](#github-workflows)
  - [Contribute for this repo](#contribute-for-this-repo)
    - [Bash-ish machine Setup](#bash-ish-machine-setup)
      - [Pre-Commit](#pre-commit)

## Github Workflows
* Every push to `master` will trigger an image build of all **changed** applications. When `base` is changed all dependent services will be built (determined by checking for a reference to `base` in Dockerfile).
* A manual trigger of the Github workflow `Build Docker image and update k8s config for changed services` will build **all** applications. This also works for feature branches and will create image tags with the branch name suffix.
* For master only: After successfully building all images the new tags will automatically be set in the `kubernetes_config` repository for the `dev` environment and automatically rolled out to the cluster via Flux.


## Contribute for this repo

In order to contribute for this repo we advice to use Linux/WSL/"Bash-ish" machines.

### Bash-ish machine Setup

Requirements:

You should have installed:
* Python
* Pre-commit installed via pip. Check the pre-commit section.
* Pyenv and Pyenv virtualenv. If using brew (linux/macos) please check [here](https://github.com/pyenv/pyenv-virtualenv#installing-with-homebrew-for-macos-users)
* VSCode (everything is configured for it :) ). Just open the workspace in: `.vscode/container_scripts.code-workspace`

Checkout/Clone the repo then just do:

```bash
export VOXEL_REGISTRY_TOKEN=XXXXXXXX
make setup-python-virtual-envs
```
This will create a virtualenv for each solution and install de solution dependencies.

Then open the `container_scripts.code-workspace` VS Code workspace.


#### Pre-Commit

Installation steps:

```bash
python -m pip install pre-commit
pre-commit install
```

Ad-hoc run:
```bash
pre-commit run --all-files
```
