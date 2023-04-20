#### VOXEL 51
FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.16-bullseye

RUN apt -y update && apt -y upgrade && apt -y --no-install-recommends install ffmpeg

# Run as non-root user and group
RUN addgroup --system --gid 3000 docker-group && adduser --disabled-login --system --gid 3000 --uid 1000 docker
USER 1000

# Configure Voxel parameters
ENV FIFTYONE_DATABASE_ADMIN=true

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy requirements.txt
COPY --chown=1000:3000 ./Metadata/requirements.txt /app/Metadata/requirements.txt
WORKDIR /app/Metadata

#Code to install always the latest
RUN --mount=type=secret,id=voxel51token,uid=1000 TOKEN=$(cat /run/secrets/voxel51token) pip --no-cache-dir install -r requirements.txt

# Copy application code
COPY --chown=1000:3000 ./Metadata/metadata/ /app/Metadata/metadata
COPY --chown=1000:3000 ./Metadata/setup.py /app/Metadata/setup.py
WORKDIR /app/Metadata
RUN pip install -e .

COPY ./Metadata/entrypoint.sh /app/
ENTRYPOINT ["/app/entrypoint.sh"]
