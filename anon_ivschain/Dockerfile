FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.10.13-alpine
RUN apk update && apk upgrade && apk add ffmpeg

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && \
    adduser -S -D -G docker-group -u 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Add basehandler library
COPY --chown=1000:3000 ./basehandler /app/basehandler

# Copy requirements.txt
COPY --chown=1000:3000 ./anon_ivschain/requirements.txt /app/anon_ivschain/requirements.txt
WORKDIR /app/anon_ivschain
RUN pip install -r requirements.txt

# Copy application code
COPY --chown=1000:3000 ./anon_ivschain/anon_ivschain/ /app/anon_ivschain/anon_ivschain
COPY --chown=1000:3000 ./anon_ivschain/setup.py /app/anon_ivschain/setup.py

WORKDIR /app/anon_ivschain
RUN pip install -e .

# Run anon_ivschain main by its package name
ENTRYPOINT [ "python", "-m", "anon_ivschain.main" ]
