#### VOXEL 51
FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.16-bullseye

RUN apt -y update && apt -y upgrade

# Run as non-root user and group
RUN addgroup --system --gid 3000 docker-group && adduser --disabled-login --system --gid 3000 --uid 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy required directories to docker container
COPY --chown=1000:3000 ./data_importer/requirements.txt /app/data_importer/requirements.txt
WORKDIR /app/data_importer

#Code to install always the latest
RUN --mount=type=secret,id=voxel51token,uid=1000 TOKEN=$(cat /run/secrets/voxel51token) pip --no-cache-dir install -r requirements.txt

# Copy module
COPY --chown=1000:3000 ./data_importer/data_importer/ /app/data_importer/data_importer
COPY --chown=1000:3000 ./data_importer/setup.py /app/data_importer/setup.py
WORKDIR /app/data_importer
RUN pip install -e .

# Necessary in order to have localstack working
COPY --chown=1000:3000 ./data_importer/localstack.patch /app/data_importer/localstack.patch
ARG LOCALSTACK_PATCH
RUN if [ -z "$LOCALSTACK_PATCH" ] ; then echo Localstack patch NOT APPLIED ; else patch ~/.local/lib/python3.9/site-packages/eta/core/storage.py < /app/data_importer/localstack.patch ; fi

# Run DataImporter python script
ENTRYPOINT [ "python", "-m", "data_importer.main"]
