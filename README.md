# Container Scripts

This repository contains all Datanauts owned applications.

- [Container Scripts](#container-scripts)
  - [Github Workflows](#github-workflows)
  - [Pre-Commit](#pre-commit)
  - [Recommended VSCode Editor extensions](#recommended-vscode-editor-extensions)

## Github Workflows
* Every push to `master` will trigger an image build of all **changed** applications. When `base` is changed all dependent services will be built (determined by checking for a reference to `base` in Dockerfile).
* A manual trigger of the Github workflow `Build Docker image and update k8s config for changed services` will build **all** applications. This also works for feature branches and will create image tags with the branch name suffix.
* For master only: After successfully building all images the new tags will automatically be set in the `kubernetes_config` repository for the `dev` environment and automatically rolled out to the cluster via Flux.

## Pre-Commit

Installation steps:

```bash
python -m pip install pre-commit
pre-commit install
```

Ad-hoc run:
```bash
pre-commit run --all-files
```

## Recommended VSCode Editor extensions

- [EditorConfig](https://editorconfig.org/) (for [VSCode](https://marketplace.visualstudio.com/items?itemName=EditorConfig.EditorConfig))
- [Python for VS Code](https://code.visualstudio.com/docs/languages/python)
- [Pylance for VS Code](https://marketplace.visualstudio.com/items?itemName=ms-python.vscode-pylance)
- [Docker for VS Code](https://code.visualstudio.com/docs/containers/overview)
- [Trailing Whitespace for VS Code](https://marketplace.visualstudio.com/items?itemName=shardulm94.trailing-spaces)
