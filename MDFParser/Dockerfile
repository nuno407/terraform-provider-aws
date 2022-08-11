FROM 081962623310.dkr.ecr.eu-central-1.amazonaws.com/base-images:python3.9.12-alpine3.15

WORKDIR /app

# Run as non-root user and group
RUN addgroup -S -g 3000 docker-group && adduser -S -D -G docker-group -u 1000 docker
USER 1000

# Install requirements
COPY --chown=1000:3000 ./MDFParser/src/requirements.txt /app/src/requirements.txt
WORKDIR /app/src
RUN pip install -r requirements.txt

# Copy and install baseaws
COPY --chown=1000:3000 ./baseaws/ /app/package/
WORKDIR /app/package
RUN pip install .

# Install required modules
COPY --chown=1000:3000 ./MDFParser/src/ /app/src/
WORKDIR /app/src
# Run MDFParser python script
ENTRYPOINT [ "python", "main.py" ]
