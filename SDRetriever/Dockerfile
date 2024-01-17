FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.12-alpine

WORKDIR /app

# Install ffmpeg program (to convert .avi to .mp4)
RUN apk update && apk upgrade && apk add ffmpeg

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && adduser -S -D -G docker-group -u 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy requirements.txt
COPY --chown=1000:3000 ./SDRetriever/requirements.txt /app/SDRetriever/requirements.txt
WORKDIR /app/SDRetriever
RUN pip install -r requirements.txt

# Copy application code
COPY --chown=1000:3000 ./SDRetriever/sdretriever/ /app/SDRetriever/sdretriever
COPY --chown=1000:3000 ./SDRetriever/setup.py /app/SDRetriever/setup.py

WORKDIR /app/SDRetriever
RUN pip install -e .

# Run anon_ivschain main by its package name
ENTRYPOINT [ "python", "-m", "sdretriever.main" ]
