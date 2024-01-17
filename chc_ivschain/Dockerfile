FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.12-alpine

# Install latest patches
RUN apk update && apk upgrade

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && \
    adduser -S -D -G docker-group -u 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Add basehandler library
COPY --chown=1000:3000 ./basehandler /app/basehandler

# Copy requirements.txt
COPY --chown=1000:3000 ./chc_ivschain/requirements.txt /app/chc_ivschain/requirements.txt
WORKDIR /app/chc_ivschain
RUN pip install -r requirements.txt

# Copy application code
COPY --chown=1000:3000 ./chc_ivschain/chc_ivschain/ /app/chc_ivschain/chc_ivschain
COPY --chown=1000:3000 ./chc_ivschain/setup.py /app/chc_ivschain/setup.py

WORKDIR /app/chc_ivschain
RUN pip install -e .

# Run chc_ivschain main by its package name
ENTRYPOINT [ "python", "-m", "chc_ivschain.main" ]
