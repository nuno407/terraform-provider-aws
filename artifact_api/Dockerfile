FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.10-bullseye

RUN apt -y update && apt -y upgrade && apt -y --no-install-recommends install ffmpeg

# Run as non-root user and group
RUN addgroup --system --gid 3000 docker-group && adduser --disabled-login --system --gid 3000 --uid 1000 docker
USER 1000

# Configure Voxel parameters
ENV FIFTYONE_DATABASE_ADMIN=true
ENV PATH=/home/docker/.local/bin:${PATH}

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy requirements.txt
COPY --chown=1000:3000 ./artifact_api/requirements.txt /app/artifact_api/requirements.txt
WORKDIR /app/artifact_api

#Code to install always the latest
RUN pip install -U pip
RUN --mount=type=secret,id=voxel51token,uid=1000 TOKEN=$(cat /run/secrets/voxel51token) pip --no-cache-dir install -r requirements.txt
# Necessary in order to have localstack working
COPY --chown=1000:3000 ./artifact_api/localstack.patch /app/artifact_api/localstack.patch
ARG LOCALSTACK_PATCH
RUN if [ -z "$LOCALSTACK_PATCH" ] ; then echo Localstack patch NOT APPLIED ; else patch ~/.local/lib/python3.10/site-packages/eta/core/storage.py < /app/artifact_api/localstack.patch ; fi

# Copy application code
COPY --chown=1000:3000 ./artifact_api/artifact_api/ /app/artifact_api/artifact_api
COPY --chown=1000:3000 ./artifact_api/setup.py /app/artifact_api/setup.py
WORKDIR /app/artifact_api
RUN pip install -e .

# Run uvicorn to serve the API
CMD [ "uvicorn", "artifact_api.router:app", "--host", "0.0.0.0", "--port", "5000"]
