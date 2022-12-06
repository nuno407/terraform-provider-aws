FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.12-alpine3.15

# Install latest patches
RUN apk update && apk upgrade

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && \
    adduser -S -D -G docker-group -u 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Add basehandler library
COPY --chown=1000:3000 ./healthcheck /app/basehandler

# Copy requirements.txt
COPY --chown=1000:3000 ./healthcheck/requirements.txt /app/healthcheck/requirements.txt
WORKDIR /app/healthcheck
RUN pip install -r requirements.txt

# Copy application code
COPY --chown=1000:3000 ./healthcheck/healthcheck/ /app/healthcheck/healthcheck
COPY --chown=1000:3000 ./healthcheck/setup.py /app/healthcheck/setup.py

WORKDIR /app/healthcheck
RUN pip install -e .

# Run healthcheck main by its package name
ENTRYPOINT [ "python", "-m", "healthcheck.main" ]
