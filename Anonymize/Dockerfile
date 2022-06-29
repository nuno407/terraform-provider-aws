FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.12-alpine3.15

WORKDIR /app
RUN apk update && apk upgrade && apk add ffmpeg

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && adduser -S -D -G docker-group -u 1000 docker
USER 1000

# Install dependencies
COPY --chown=1000:3000 ./Anonymize/src/requirements.txt /app/src/requirements.txt
WORKDIR /app/src
RUN pip install -r requirements.txt

# Install BaseAws package (custom)
COPY --chown=1000:3000 ./baseaws/ /app/package/
WORKDIR /app/package
RUN pip install .

# Copy module
COPY --chown=1000:3000 ./Anonymize/src/ /app/src/
WORKDIR /app/src

# Run anonymize python script
ENTRYPOINT [ "python", "main.py"]
