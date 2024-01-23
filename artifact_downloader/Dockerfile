FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.10.13-alpine

WORKDIR /app

RUN apk update && apk upgrade && apk add ffmpeg

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && adduser -S -D -G docker-group -u 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy requirements.txt
COPY --chown=1000:3000 ./artifact_downloader/requirements.txt /app/artifact_downloader/requirements.txt
WORKDIR /app/artifact_downloader
RUN pip install -r requirements.txt

# Copy application code
COPY --chown=1000:3000 ./artifact_downloader/artifact_downloader/ /app/artifact_downloader/artifact_downloader
COPY --chown=1000:3000 ./artifact_downloader/setup.py /app/artifact_downloader/setup.py

WORKDIR /app/artifact_downloader
RUN pip install -e .

# Run anon_ivschain main by its package name
ENTRYPOINT [ "python", "-m", "artifact_downloader.main" ]
