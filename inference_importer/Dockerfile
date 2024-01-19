FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.10-bullseye

# Install latest patches
RUN apt -y update && apt -y upgrade

# Run as non-root user and group
RUN addgroup --system --gid 3000 docker-group && adduser --disabled-login --system --gid 3000 --uid 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy required directories to docker container
COPY --chown=1000:3000 ./inference_importer/requirements.txt /app/inference_importer/requirements.txt
WORKDIR /app/inference_importer

#Code to install always the latest
RUN --mount=type=secret,id=voxel51token,uid=1000 TOKEN=$(cat /run/secrets/voxel51token) pip --no-cache-dir install -r requirements.txt

# Copy module
COPY --chown=1000:3000 ./inference_importer/inference_importer/ /app/inference_importer/inference_importer
COPY --chown=1000:3000 ./inference_importer/setup.py /app/inference_importer/setup.py
WORKDIR /app/inference_importer
RUN pip install -e .

# Run SDM python script
ENTRYPOINT [ "python", "-m", "inference_importer.main"]
