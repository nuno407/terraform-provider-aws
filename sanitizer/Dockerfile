FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.10-alpine

WORKDIR /app

RUN apk update && apk upgrade

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && adduser -S -D -G docker-group -u 1000 docker
USER 1000

# Add base library
COPY --chown=1000:3000 ./base /app/base

# Copy requirements.txt
COPY --chown=1000:3000 ./sanitizer/requirements.txt /app/sanitizer/requirements.txt
WORKDIR /app/sanitizer
RUN pip install -r requirements.txt

# Copy application code
COPY --chown=1000:3000 ./sanitizer/sanitizer/ /app/sanitizer/sanitizer
COPY --chown=1000:3000 ./sanitizer/setup.py /app/sanitizer/setup.py

WORKDIR /app/sanitizer
RUN pip install -e .

# Run anon_ivschain main by its package name
ENTRYPOINT [ "python", "-m", "sanitizer.main" ]
