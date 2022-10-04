FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.12-alpine3.15

WORKDIR /app

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && adduser -S -D -G docker-group -u 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy requirements.txt 
COPY --chown=1000:3000 ./Selector/requirements.txt /app/Selector/requirements.txt
WORKDIR /app/Selector
RUN pip install -r requirements.txt

# Copy application code
COPY --chown=1000:3000 ./Selector/selector/ /app/Selector/selector
COPY --chown=1000:3000 ./Selector/setup.py /app/Selector/setup.py

WORKDIR /app/Selector
RUN pip install -e .

# Run anon_ivschain main by its package name
ENTRYPOINT [ "python", "-m", "selector.main" ]
