FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.12-bullseye

RUN apt -y update && apt -y upgrade

# Run as non-root user and group
RUN addgroup --system --gid 3000 docker-group && adduser --disabled-login --system --gid 3000 --uid 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy required directories to docker container
COPY --chown=1000:3000 ./labeling_bridge/requirements.txt /app/labeling_bridge/requirements.txt
WORKDIR /app/labeling_bridge

RUN --mount=type=secret,id=voxel51token,uid=1000 TOKEN=$(cat /run/secrets/voxel51token) pip --no-cache-dir install -r requirements.txt

# Copy module
COPY --chown=1000:3000 ./labeling_bridge/labeling_bridge/ /app/labeling_bridge/labeling_bridge
COPY --chown=1000:3000 ./labeling_bridge/setup.py /app/labeling_bridge/setup.py
WORKDIR /app/labeling_bridge
RUN pip install -e .

# Necessary in order to have localstack working
COPY --chown=1000:3000 ./labeling_bridge/localstack.patch /app/labeling_bridge/localstack.patch
ARG LOCALSTACK_PATCH
RUN if [ -z "$LOCALSTACK_PATCH" ] ; then echo Localstack patch NOT APPLIED ; else patch ~/.local/lib/python3.9/site-packages/eta/core/storage.py < /app/labeling_bridge/localstack.patch ; fi

# Run DataImporter python script
ENTRYPOINT [ "python", "-m", "labeling_bridge.main"]
