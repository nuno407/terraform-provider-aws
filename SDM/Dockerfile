FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.12-alpine3.15

WORKDIR /app

# Install latest patches
RUN apk update && apk upgrade

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && adduser -S -D -G docker-group -u 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy required directories to docker container
COPY --chown=1000:3000 ./SDM/requirements.txt /app/sdm/requirements.txt
WORKDIR /app/sdm
RUN pip install -r requirements.txt

# Copy module
COPY --chown=1000:3000 ./SDM/sdm/ /app/sdm/sdm
COPY --chown=1000:3000 ./SDM/setup.py /app/sdm/setup.py
WORKDIR /app/sdm
RUN pip install -e .

# Run sdm python script
ENTRYPOINT [ "python", "-m", "sdm.main"]
