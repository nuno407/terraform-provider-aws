#### VOXEL 51
FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.10-bullseye

RUN apt -y update && apt -y upgrade

# Run as non-root user and group
RUN addgroup --system --gid 3000 docker-group && adduser --disabled-login --system --gid 3000 --uid 1000 docker
USER 1000

# Configure Voxel parameters
ENV FIFTYONE_DATABASE_ADMIN=true

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Add basehandler library
COPY --chown=1000:3000 ./healthcheck /app/basehandler

# Copy requirements.txt
COPY --chown=1000:3000 ./healthcheck/requirements.txt /app/healthcheck/requirements.txt
WORKDIR /app/healthcheck

#Code to install always the latest
RUN --mount=type=secret,id=voxel51token,uid=1000 TOKEN=$(cat /run/secrets/voxel51token) pip --no-cache-dir install -r requirements.txt

# Copy application code
COPY --chown=1000:3000 ./healthcheck/healthcheck/ /app/healthcheck/healthcheck
COPY --chown=1000:3000 ./healthcheck/setup.py /app/healthcheck/setup.py

WORKDIR /app/healthcheck
RUN pip install -e .

# Run healthcheck main by its package name
ENTRYPOINT [ "python", "-m", "healthcheck.main" ]
