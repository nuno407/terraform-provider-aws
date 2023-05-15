ARG VARIANT=3.9
ARG TOKEN
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

# Install Python dependencies into seperate virtual environments
WORKDIR /workspace
RUN mkdir /venv
RUN --mount=type=bind,source=.,target=/ctx,readwrite \
    find /ctx -maxdepth 2 -type f -name "requirements*.txt" -exec bash -c 'COMPDIR=$(dirname "$1") &&\
    ENVNAME=$(basename "$COMPDIR") &&\
    python -m venv /venv/$ENVNAME &&\
    source /venv/${ENVNAME}/bin/activate &&\
    cd $COMPDIR &&\
    pip install -r "$1"' \
    bash {} \;

RUN chown -R vscode:vscode /venv
